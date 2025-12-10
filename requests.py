from sqlalchemy.exc import IntegrityError
from models import Session, User, Referral, SupportTicket, PointsHistory
import secrets
import string
import logging
from datetime import datetime
import sqlite3

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from config import REFERRAL_POINTS, STARTPOINTS, NEW_USER_POINTS


def generate_referral_code(length=8):
    """Генерация уникального реферального кода"""
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def get_or_create_user(tg_id: int, first_name: str = None, last_name: str = None, 
                       username: str = None, phone: str = None, referrer_code: str = None):
    """Получить или создать пользователя"""
    session = Session()
    try:
        # Пытаемся найти пользователя по tg_id
        user = session.query(User).filter(User.tg_id == tg_id).first()
        
        if user:
            # Обновляем данные если они переданы
            if first_name and not user.first_name:
                user.first_name = first_name
            if last_name and not user.last_name:
                user.last_name = last_name
            if username and not user.username:
                user.username = username
            if phone and not user.phone:
                user.phone = phone
                session.commit()
            return user.id
        
        # Создаем нового пользователя
        referral_code = generate_referral_code()
        while session.query(User).filter(User.referral_code == referral_code).first():
            referral_code = generate_referral_code()
        
        user = User(
            tg_id=tg_id,
            first_name=first_name,
            last_name=last_name,
            username=username,
            phone=phone,
            referral_code=referral_code,
            invited_by=referrer_code
        )
        
        session.add(user)
        session.commit()
        
        # Если есть реферер, начисляем баллы через add_points_with_history
        if referrer_code:
            award_referral_points(referrer_code, tg_id)
        
        return user.id
        
    except IntegrityError as e:
        session.rollback()
        logger.error(f"IntegrityError in get_or_create_user: {e}")
        
        # Если ошибка из-за дублирования телефона
        if phone:
            try:
                # Пытаемся найти пользователя по телефону
                user_by_phone = session.query(User).filter(User.phone == phone).first()
                if user_by_phone:
                    # Если нашли по телефону, привязываем к этому Telegram ID
                    user_by_phone.tg_id = tg_id
                    session.commit()
                    return user_by_phone.id
            except Exception as inner_e:
                session.rollback()
                logger.error(f"Error handling duplicate phone: {inner_e}")
        
        return None
    except Exception as e:
        session.rollback()
        logger.error(f"Error in get_or_create_user: {e}")
        return None
    finally:
        session.close()


def get_user_by_phone(phone: str):
    """Найти пользователя по номеру телефона"""
    session = Session()
    try:
        return session.query(User).filter(User.phone == phone).first()
    finally:
        session.close()

def get_user_by_tg_id(tg_id: int):
    """Найти пользователя по Telegram ID"""
    session = Session()
    try:
        return session.query(User).filter(User.tg_id == tg_id).first()
    finally:
        session.close()

def award_referral_points(referrer_code: str, referred_tg_id: int):
    """Начислить баллы за реферала с историей"""
    session = Session()
    try:
        referrer = session.query(User).filter(User.referral_code == referrer_code).first()
        referred = session.query(User).filter(User.tg_id == referred_tg_id).first()
        
        if referrer and referred:
            # Начисляем рефереру
            add_points_with_history(
                referrer.id,
                'referral',
                REFERRAL_POINTS,
                f'Реферал: {referred.phone or referred.first_name}'
            )
            
            # Начисляем новому пользователю
            add_points_with_history(
                referred.id,
                'referral',
                NEW_USER_POINTS,
                f'Приветственные за регистрацию по реф. ссылке'
            )
            
            # Создаем запись о реферале
            referral = Referral(
                referrer_code=referrer_code,
                referred_phone=referred.phone,
                points_awarded=100
            )
            session.add(referral)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error in award_referral_points: {e}")
        return False
    finally:
        session.close()

def add_welcome_bonus(user_id: int):
    """Добавить приветственные 250 баллов с историей"""
    return add_points_with_history(
        user_id,
        'welcome',
        250,
        'Приветственные баллы за привязку телефона'
    )

def add_manual_points(phone: str, points: int):
    """Добавить баллы вручную"""
    session = Session()
    try:
        user = session.query(User).filter(User.phone == phone).first()
        if user:
            # Используем add_points_with_history вместо прямого изменения
            return add_points_with_history(
                user.id,
                'manual',
                points,
                'Ручное начисление администратором'
            )
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error in add_manual_points: {e}")
        return False
    finally:
        session.close()

def update_phone_with_welcome_bonus(tg_id: int, phone: str) -> tuple[bool, str, int]:
    """Обновляет телефон с начислением приветственных баллов и историей"""
    session = Session()
    try:
        # Нормализация
        if phone.startswith('+'):
            phone = '8' + phone[2:]
        elif phone.startswith('7'):
            phone = '8' + phone[1:]
        
        if len(phone) != 11:
            return False, "❌ Неверный формат номера", 0
        
        # Находим пользователя
        user = session.query(User).filter(User.tg_id == tg_id).first()
        if not user:
            return False, "❌ Пользователь не найден", 0
        
        # Проверяем существование телефона
        existing = session.query(User).filter(User.phone == phone).first()
        
        welcome_bonus = 0
        
        if existing and existing.id != user.id:
            # Переносим все от существующего пользователя
            # Переносим баллы с историей через add_points_with_history
            if existing.points_manual > 0:
                add_points_with_history(
                    user.id,
                    'manual',
                    existing.points_manual,
                    f'Перенос баллов от старого аккаунта {existing.phone}'
                )
            
            if existing.points_referral > 0:
                add_points_with_history(
                    user.id,
                    'referral',
                    existing.points_referral,
                    f'Перенос реферальных баллов от старого аккаунта'
                )
            
            # Удаляем старого
            session.delete(existing)
            
            msg = f"✅ Аккаунт объединен! Перенесено {existing.get_total_points()} баллов"
        
        elif user.phone is None:
            # Первый телефон - начисляем приветственные через add_welcome_bonus
            add_welcome_bonus(user.id)
            welcome_bonus = 250
            msg = "✅ Номер привязан! 🎉 +250 приветственных баллов!"
        
        else:
            # Просто меняем телефон
            msg = "✅ Номер телефона изменен"
        
        # Сохраняем телефон
        user.phone = phone
        session.commit()
        
        return True, msg, welcome_bonus
        
    except Exception as e:
        session.rollback()
        return False, f"❌ Ошибка: {str(e)[:50]}", 0
    finally:
        session.close()

def user_has_phone(tg_id: int) -> bool:
    """Проверяет, есть ли у пользователя телефон"""
    session = Session()
    try:
        user = session.query(User).filter(User.tg_id == tg_id).first()
        return user is not None and user.phone is not None
    finally:
        session.close()

def update_user_phone_simple(tg_id: int, phone: str) -> tuple[bool, str, int]:
    """Обновление телефона с обработкой объединения аккаунтов"""
    session = Session()
    try:
        # Нормализация номера
        if phone.startswith('+'):
            phone = '8' + phone[2:]
        elif phone.startswith('7'):
            phone = '8' + phone[1:]
        
        if len(phone) != 11 or not phone.isdigit():
            return False, "❌ Неверный формат номера", 0
        
        # Находим текущего пользователя
        current_user = session.query(User).filter(User.tg_id == tg_id).first()
        if not current_user:
            return False, "❌ Пользователь не найден", 0
        
        # Проверяем, был ли у пользователя уже телефон
        is_first_phone = current_user.phone is None
        
        # Находим существующего владельца этого телефона (если есть)
        existing_user = session.query(User).filter(User.phone == phone).first()
        
        transferred_points = 0
        welcome_bonus = 0
        
        if existing_user:
            # СИТУАЦИЯ 1: Телефон уже есть в БД у другого пользователя
            if existing_user.id != current_user.id:
                # 1. Переносим баллы через add_points_with_history
                if existing_user.points_manual > 0:
                    add_points_with_history(
                        current_user.id,
                        'manual',
                        existing_user.points_manual,
                        f'Перенос ручных баллов от аккаунта {existing_user.phone}'
                    )
                    transferred_points += existing_user.points_manual
                
                if existing_user.points_referral > 0:
                    add_points_with_history(
                        current_user.id,
                        'referral',
                        existing_user.points_referral,
                        f'Перенос реферальных баллов от аккаунта {existing_user.phone}'
                    )
                    transferred_points += existing_user.points_referral
                
                # 2. Переносим имя/фамилию (если у текущего нет)
                if not current_user.first_name and existing_user.first_name:
                    current_user.first_name = existing_user.first_name
                if not current_user.last_name and existing_user.last_name:
                    current_user.last_name = existing_user.last_name
                
                # 3. Переносим реферальный код (если у текущего нет)
                if not current_user.referral_code and existing_user.referral_code:
                    current_user.referral_code = existing_user.referral_code
                
                # 4. Переносим кто пригласил (если у текущего нет)
                if not current_user.invited_by and existing_user.invited_by:
                    current_user.invited_by = existing_user.invited_by
                
                # 5. Удаляем старого пользователя из БД
                session.delete(existing_user)
                
                message = f"✅ Найден старый аккаунт! Перенесено {transferred_points} баллов"
            
            else:
                # СИТУАЦИЯ 2: Это тот же самый пользователь (уже имеет этот телефон)
                message = "✅ Этот номер уже привязан к вашему аккаунту"
        
        else:
            # СИТУАЦИЯ 3: Телефон НОВЫЙ (нет в БД)
            if is_first_phone:
                # Начисляем приветственные 250 баллов за первый телефон через add_welcome_bonus
                add_welcome_bonus(current_user.id)
                welcome_bonus = STARTPOINTS
                message = f"✅ Номер привязан! 🎉 +{welcome_bonus} приветственных баллов!"
            else:
                # Просто меняем телефон (без бонуса)
                message = "✅ Номер телефона изменен"
        
        # Привязываем/изменяем телефон
        current_user.phone = phone
        session.commit()
        
        total_bonus = transferred_points + welcome_bonus
        
        return True, message, total_bonus
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error in update_user_phone_simple: {e}")
        return False, f"❌ Ошибка: {str(e)[:50]}", 0
    finally:
        session.close()

def update_phone_universal(tg_id: int, phone: str) -> tuple[bool, str]:
    """Универсальная функция обновления телефона с правильным удалением дубликатов"""
    session = Session()
    try:
        # Нормализация
        if phone.startswith('+'):
            phone = '8' + phone[2:]  # +7999... -> 8999...
        elif phone.startswith('7'):
            phone = '8' + phone[1:]  # 7999... -> 8999...
        
        if len(phone) != 11 or not phone.isdigit():
            return False, "❌ Неверный формат номера"
        
        # 1. Находим текущего пользователя (кто вводит номер)
        current_user = session.query(User).filter(User.tg_id == tg_id).first()
        if not current_user:
            return False, "❌ Пользователь не найден"
        
        # 2. Проверяем, не пытается ли пользователь привязать уже свой же телефон
        if current_user.phone == phone:
            return True, "✅ Этот номер уже привязан к вашему аккаунту"
        
        # 3. Ищем существующего пользователя с этим телефоном
        existing_user = session.query(User).filter(User.phone == phone).first()
        
        if existing_user:
            # 4. Если нашли другого пользователя с таким телефоном
            if existing_user.id != current_user.id:
                # ВАЖНО: Сначала удаляем существующего пользователя из сессии
                # чтобы избежать конфликта уникальности
                session.expunge(existing_user)  # Открепляем от сессии
                
                # Переносим баллы через add_points_with_history
                transferred_points = 0
                
                if existing_user.points_manual > 0:
                    add_points_with_history(
                        current_user.id,
                        'manual',
                        existing_user.points_manual,
                        f'Перенос ручных баллов от аккаунта {existing_user.phone}'
                    )
                    transferred_points += existing_user.points_manual
                
                if existing_user.points_referral > 0:
                    add_points_with_history(
                        current_user.id,
                        'referral',
                        existing_user.points_referral,
                        f'Перенос реферальных баллов от аккаунта {existing_user.phone}'
                    )
                    transferred_points += existing_user.points_referral
                
                # Переносим другие данные
                if not current_user.first_name and existing_user.first_name:
                    current_user.first_name = existing_user.first_name
                if not current_user.last_name and existing_user.last_name:
                    current_user.last_name = existing_user.last_name
                if not current_user.referral_code and existing_user.referral_code:
                    current_user.referral_code = existing_user.referral_code
                
                # Теперь удаляем существующего пользователя из БД
                session.delete(existing_user)
                session.flush()  # Применяем удаление
                
                msg = f"✅ Найден старый аккаунт! Перенесено {transferred_points} баллов"
            else:
                return True, "✅ Этот номер уже привязан"
        else:
            # 5. Если телефон новый (нет в БД)
            if current_user.phone is None:
                # Первый телефон - начисляем бонус через add_welcome_bonus
                add_welcome_bonus(current_user.id)
                msg = "✅ Номер привязан! 🎉 +250 приветственных баллов!"
            else:
                # Просто меняем телефон (без бонуса)
                msg = "✅ Номер телефона изменен"
        
        # 6. Привязываем телефон к текущему пользователю
        current_user.phone = phone
        session.commit()
        
        return True, msg
        
    except IntegrityError as e:
        session.rollback()
        logger.error(f"IntegrityError: {e}")
        # Если все равно ошибка, используем радикальный метод
        return force_update_phone(tg_id, phone)
    
    except Exception as e:
        session.rollback()
        logger.error(f"Error in update_phone_universal: {e}")
        return False, f"❌ Ошибка: {str(e)[:50]}"
    
    finally:
        session.close()

def force_update_phone(tg_id: int, phone: str) -> tuple[bool, str]:
    """Принудительное обновление телефона через прямой SQL"""
    try:
        # Нормализация
        if phone.startswith('+'):
            phone = '8' + phone[2:]
        elif phone.startswith('7'):
            phone = '8' + phone[1:]
        
        if len(phone) != 11:
            return False, "❌ Неверный формат номера"
        
        # Прямое подключение к SQLite
        conn = sqlite3.connect('bot.app.db')
        cursor = conn.cursor()
        
        # 1. Находим ID текущего пользователя
        cursor.execute("SELECT id FROM users WHERE tg_id = ?", (tg_id,))
        user_row = cursor.fetchone()
        
        if not user_row:
            conn.close()
            return False, "❌ Пользователь не найден"
        
        user_id = user_row[0]
        
        # 2. Находим старого владельца телефона
        cursor.execute("SELECT id, points_manual, points_referral, first_name, last_name FROM users WHERE phone = ?", (phone,))
        old_owner = cursor.fetchone()
        
        transferred_points = 0
        
        if old_owner:
            old_id, old_manual, old_referral, old_first_name, old_last_name = old_owner
            
            if old_id != user_id:
                # 3. Переносим баллы
                transferred_points = old_manual + old_referral
                cursor.execute(
                    "UPDATE users SET points_manual = points_manual + ?, points_referral = points_referral + ? WHERE id = ?",
                    (old_manual, old_referral, user_id)
                )
                
                # 4. Переносим имя (если у нового нет)
                cursor.execute("SELECT first_name FROM users WHERE id = ?", (user_id,))
                current_first_name = cursor.fetchone()[0]
                
                if not current_first_name and old_first_name:
                    cursor.execute(
                        "UPDATE users SET first_name = ? WHERE id = ?",
                        (old_first_name, user_id)
                    )
                
                # 5. Удаляем старого пользователя
                cursor.execute("DELETE FROM users WHERE id = ?", (old_id,))
        
        # 6. Проверяем, есть ли у пользователя уже телефон
        cursor.execute("SELECT phone FROM users WHERE id = ?", (user_id,))
        current_phone = cursor.fetchone()[0]
        
        welcome_bonus = 0
        
        if not current_phone:
            # 7. Начисляем приветственные 250 баллов через отдельное обновление
            cursor.execute(
                "UPDATE users SET points_manual = points_manual + 250, last_manual_points_update = datetime('now') WHERE id = ?",
                (user_id,)
            )
            welcome_bonus = STARTPOINTS
        
        # 8. Привязываем телефон
        cursor.execute(
            "UPDATE users SET phone = ? WHERE id = ?",
            (phone, user_id)
        )
        
        conn.commit()
        conn.close()
        
        # Формируем сообщение
        total_bonus = transferred_points + welcome_bonus
        
        if transferred_points > 0 and welcome_bonus > 0:
            return True, f"✅ Объединены аккаунты! Перенесено {transferred_points} баллов + {welcome_bonus} приветственных!"
        elif transferred_points > 0:
            return True, f"✅ Найден старый аккаунт! Перенесено {transferred_points} баллов"
        elif welcome_bonus > 0:
            return True, f"✅ Номер привязан! 🎉 +{welcome_bonus} приветственных баллов!"
        else:
            return True, "✅ Номер телефона изменен"
        
    except Exception as e:
        return False, f"❌ Ошибка SQL: {str(e)[:50]}"

def update_user_phone_in_db(tg_id: int, new_phone: str) -> bool:
    """Обновляет номер телефона пользователя"""
    session = Session()
    try:
        user = session.query(User).filter(User.tg_id == tg_id).first()
        if user:
            # Нормализуем номер
            if new_phone.startswith('+'):
                new_phone = '8' + new_phone[2:]  # +7999... -> 8999...
            elif new_phone.startswith('7'):
                new_phone = '8' + new_phone[1:]  # 7999... -> 8999...
            
            user.phone = new_phone
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating phone: {e}")
        return False
    finally:
        session.close()

def get_user_data(tg_id: int):
    """Получить все данные пользователя в виде словаря"""
    session = Session()
    try:
        user = session.query(User).filter(User.tg_id == tg_id).first()
        if user:
            return {
                'id': user.id,
                'tg_id': user.tg_id,
                'first_name': user.first_name,
                'last_name': user.last_name,
                'username': user.username,
                'phone': user.phone,
                'referral_code': user.referral_code,
                'points_referral': user.points_referral,
                'points_manual': user.points_manual,
                'last_manual_points_update': user.last_manual_points_update,
                'last_referral_points_update': user.last_referral_points_update,
                'invited_by': user.invited_by
            }
        return None
    finally:
        session.close()

def get_user_points(tg_id: int):
    """Получить баллы пользователя"""
    session = Session()
    try:
        user = session.query(User).filter(User.tg_id == tg_id).first()
        if user:
            return {
                'referral': user.points_referral,
                'manual': user.points_manual,
                'total': user.points_referral + user.points_manual
            }
        return None
    finally:
        session.close()

def create_support_ticket(user_id: int, question: str, group_message_id: int):
    """Создать тикет поддержки"""
    session = Session()
    try:
        ticket = SupportTicket(
            user_id=user_id,
            user_question=question,
            group_message_id=group_message_id,
            is_answered=False,
            created_at=datetime.now()
        )
        session.add(ticket)
        session.commit()
        return ticket.id
    except Exception as e:
        session.rollback()
        logger.error(f"Error creating ticket: {e}")
        return None
    finally:
        session.close()

def get_ticket_by_group_message(group_message_id: int):
    """Найти тикет по ID сообщения в группе"""
    session = Session()
    try:
        return session.query(SupportTicket).filter(
            SupportTicket.group_message_id == group_message_id
        ).first()
    finally:
        session.close()

def update_ticket_with_answer(ticket_id: int, answer_text: str):
    """Обновить тикет с ответом"""
    session = Session()
    try:
        ticket = session.query(SupportTicket).filter(SupportTicket.id == ticket_id).first()
        if ticket:
            ticket.is_answered = True
            ticket.answer_text = answer_text
            ticket.answered_at = datetime.now()
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating ticket: {e}")
        return False
    finally:
        session.close()

def get_user_tickets(user_id: int):
    """Получить все тикеты пользователя"""
    session = Session()
    try:
        return session.query(SupportTicket).filter(
            SupportTicket.user_id == user_id
        ).order_by(SupportTicket.created_at.desc()).all()
    finally:
        session.close()

def close_ticket(ticket_id: int):
    """Закрывает тикет (меняет статус)"""
    session = Session()
    try:
        ticket = session.query(SupportTicket).filter(SupportTicket.id == ticket_id).first()
        if ticket:
            ticket.is_answered = True
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error closing ticket: {e}")
        return False
    finally:
        session.close()

def delete_users_without_phone():
    """Удаляет всех пользователей без номера телефона"""
    session = Session()
    try:
        # Находим пользователей без телефона
        users_to_delete = session.query(User).filter(
            (User.phone.is_(None)) | (User.phone == '')
        ).all()
        
        count = len(users_to_delete)
        
        if count == 0:
            return 0, "Нет пользователей без телефона"
        
        # Удаляем каждого
        for user in users_to_delete:
            session.delete(user)
        
        session.commit()
        
        return count, f"✅ Удалено {count} пользователей без телефона"
        
    except Exception as e:
        session.rollback()
        return 0, f"❌ Ошибка: {e}"
    finally:
        session.close()

def safe_clean_database():
    """Безопасная очистка с созданием резервной копии"""
    try:
        # 1. Создаем резервную копию
        backup_file = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.db"
        
        source = sqlite3.connect('bot.app.db')
        backup = sqlite3.connect(backup_file)
        
        source.backup(backup)
        source.close()
        backup.close()
        
        # 2. Выполняем очистку
        count, msg = delete_users_without_phone()
        
        return f"✅ Резервная копия: {backup_file}\n{msg}"
        
    except Exception as e:
        return f"❌ Ошибка при создании backup: {e}"
    
def get_admin_stats():
    """Статистика для админ-панели"""
    session = Session()
    try:
        from sqlalchemy import func
        
        total_users = session.query(func.count(User.id)).scalar()
        users_with_phone = session.query(func.count(User.id)).filter(User.phone.isnot(None)).scalar()
        
        # Сумма всех баллов
        total_points = session.query(
            func.sum(User.points_referral + User.points_manual)
        ).scalar() or 0
        
        # Среднее количество баллов
        avg_points = session.query(
            func.avg(User.points_referral + User.points_manual)
        ).scalar() or 0
        
        return {
            'total_users': total_users,
            'users_with_phone': users_with_phone,
            'total_points': total_points,
            'avg_points': round(avg_points, 2)
        }
    finally:
        session.close()    

def get_all_users(limit: int = 50):
    """Получить всех пользователей"""
    session = Session()
    try:
        return session.query(User).order_by(User.id.desc()).limit(limit).all()
    finally:
        session.close()

def get_user_by_id(user_id: int):
    """Получить пользователя по ID в базе"""
    session = Session()
    try:
        return session.query(User).filter(User.id == user_id).first()
    finally:
        session.close()

def update_user_points(user_id: int, points_type: str, points: int):
    """Обновить баллы пользователя"""
    session = Session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if user:
            if points_type == 'manual':
                # Используем add_points_with_history вместо прямого изменения
                return add_points_with_history(
                    user_id,
                    'manual',
                    points,
                    'Ручное изменение баллов'
                )
            elif points_type == 'referral':
                # Используем add_points_with_history вместо прямого изменения
                return add_points_with_history(
                    user_id,
                    'referral',
                    points,
                    'Изменение реферальных баллов'
                )
            elif points_type == 'add_manual':
                # Используем add_points_with_history вместо прямого изменения
                return add_points_with_history(
                    user_id,
                    'manual',
                    points,
                    'Добавление ручных баллов'
                )
            elif points_type == 'add_referral':
                # Используем add_points_with_history вместо прямого изменения
                return add_points_with_history(
                    user_id,
                    'referral',
                    points,
                    'Добавление реферальных баллов'
                )
            
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error updating user points: {e}")
        return False
    finally:
        session.close()

def get_statistics():
    """Получить статистику"""
    session = Session()
    try:
        from sqlalchemy import func
        
        total_users = session.query(func.count(User.id)).scalar()
        users_with_phone = session.query(func.count(User.id)).filter(User.phone.isnot(None)).scalar()
        total_points = session.query(func.sum(User.points_manual + User.points_referral)).scalar() or 0
        
        return {
            'total_users': total_users,
            'users_with_phone': users_with_phone,
            'total_points': total_points
        }
    finally:
        session.close()

def search_users_by_phone(phone_part: str):
    """Поиск пользователей по номеру телефона"""
    session = Session()
    try:
        return session.query(User).filter(User.phone.like(f"%{phone_part}%")).all()
    finally:
        session.close()

def search_users_by_name(name_part: str):
    """Поиск пользователей по имени"""
    session = Session()
    try:
        return session.query(User).filter(
            (User.first_name.like(f"%{name_part}%")) | 
            (User.last_name.like(f"%{name_part}%"))
        ).all()
    finally:
        session.close()

def delete_empty_users():
    """Удаляет пользователей без телефона и без Telegram ID"""
    session = Session()
    try:
        users_to_delete = session.query(User).filter(
            (User.phone.is_(None) | (User.phone == '')),
            (User.tg_id.is_(None) | (User.tg_id == 0))
        ).all()
        
        count = len(users_to_delete)
        
        for user in users_to_delete:
            session.delete(user)
        
        session.commit()
        
        return count, f"✅ Удалено {count} пустых пользователей"
        
    except Exception as e:
        session.rollback()
        return 0, f"❌ Ошибка: {e}"
    finally:
        session.close()

def clean_duplicate_phones():
    """Удаляет дубликаты телефонов, оставляя последнюю запись"""
    session = Session()
    try:
        from sqlalchemy import func
        
        duplicates = session.query(
            User.phone,
            func.count(User.id).label('count'),
            func.max(User.id).label('max_id')
        ).filter(
            User.phone.isnot(None)
        ).group_by(
            User.phone
        ).having(
            func.count(User.id) > 1
        ).all()
        
        total_deleted = 0
        
        for phone, count, max_id in duplicates:
            # Удаляем все записи с этим телефоном, кроме последней
            deleted = session.query(User).filter(
                User.phone == phone,
                User.id != max_id
            ).delete(synchronize_session=False)
            
            total_deleted += deleted
        
        session.commit()
        
        return total_deleted, f"✅ Удалено {total_deleted} дубликатов телефонов"
        
    except Exception as e:
        session.rollback()
        return 0, f"❌ Ошибка: {e}"
    finally:
        session.close()

def delete_user(user_id: int):
    """Удалить пользователя"""
    session = Session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if user:
            session.delete(user)
            session.commit()
            return True
        return False
    except Exception as e:
        session.rollback()
        logger.error(f"Error deleting user: {e}")
        return False
    finally:
        session.close()

def add_user_with_details(phone: str, points: int = 0, first_name: str = None, 
                          last_name: str = None) -> tuple[bool, str, dict]:
    """
    Добавить пользователя с деталями
    """
    session = Session()
    try:
        # Нормализация
        if phone.startswith('+'):
            phone = '8' + phone[2:]
        elif phone.startswith('7'):
            phone = '8' + phone[1:]
        
        if len(phone) != 11:
            return False, "❌ Неверный формат номера", {}
        
        # Проверяем существование
        existing = session.query(User).filter(User.phone == phone).first()
        
        if existing:
            # Обновляем существующего через add_points_with_history
            if points > 0:
                add_points_with_history(
                    existing.id,
                    'manual',
                    points,
                    'Добавление баллов через админ-панель'
                )
            
            if first_name and not existing.first_name:
                existing.first_name = first_name
            if last_name and not existing.last_name:
                existing.last_name = last_name
            
            session.commit()
            
            return True, f"✅ Обновлен существующий пользователь", {
                'id': existing.id,
                'phone': phone,
                'points': existing.get_total_points(),
                'is_new': False
            }
        
        # Создаем нового
        referral_code = generate_referral_code()
        while session.query(User).filter(User.referral_code == referral_code).first():
            referral_code = generate_referral_code()
        
        new_user = User(
            phone=phone,
            first_name=first_name,
            last_name=last_name,
            referral_code=referral_code,
            points_manual=points
        )
        
        session.add(new_user)
        session.commit()
        
        # Добавляем баллы в историю если они есть
        if points > 0:
            add_points_with_history(
                new_user.id,
                'manual',
                points,
                'Создание пользователя с баллами'
            )
        
        return True, f"✅ Создан новый пользователь", {
            'id': new_user.id,
            'phone': phone,
            'points': points,
            'is_new': True,
            'referral_code': referral_code
        }
        
    except Exception as e:
        session.rollback()
        return False, f"❌ Ошибка: {str(e)[:50]}", {}
    finally:
        session.close()

def quick_add_user(phone: str, points: int = 0) -> str:
    """
    Быстро добавить пользователя или обновить баллы
    """
    session = Session()
    try:
        # Нормализация
        if phone.startswith('+'):
            phone = '8' + phone[2:]
        elif phone.startswith('7'):
            phone = '8' + phone[1:]
        
        # Ищем существующего
        user = session.query(User).filter(User.phone == phone).first()
        
        if user:
            # Обновляем баллы через add_points_with_history
            if points > 0:
                add_points_with_history(
                    user.id,
                    'manual',
                    points,
                    'Быстрое добавление баллов'
                )
            return f"✅ Обновлен пользователь {phone}. Теперь баллов: {user.get_total_points()}"
        else:
            # Создаем нового
            referral_code = generate_referral_code()
            while session.query(User).filter(User.referral_code == referral_code).first():
                referral_code = generate_referral_code()
            
            new_user = User(
                phone=phone,
                referral_code=referral_code,
                points_manual=points
            )
            
            session.add(new_user)
            session.commit()
            
            # Добавляем баллы в историю если они есть
            if points > 0:
                add_points_with_history(
                    new_user.id,
                    'manual',
                    points,
                    'Создание пользователя с баллами'
                )
            
            return f"✅ Создан новый пользователь {phone}. ID: {new_user.id}, Баллы: {points}"
            
    except Exception as e:
        session.rollback()
        return f"❌ Ошибка: {str(e)[:50]}"
    finally:
        session.close()

def add_manual_user(phone: str, first_name: str = None, last_name: str = None, 
                    manual_points: int = 0, referral_points: int = 0) -> tuple[bool, str, int]:
    """
    Добавить пользователя вручную
    Возвращает: (успех, сообщение, user_id)
    """
    session = Session()
    try:
        # Нормализация номера
        if phone.startswith('+'):
            phone = '8' + phone[2:]
        elif phone.startswith('7'):
            phone = '8' + phone[1:]
        
        # Проверка формата
        if len(phone) != 11 or not phone.isdigit():
            return False, "❌ Неверный формат номера", 0
        
        # Проверяем, не существует ли уже пользователь с таким телефоном
        existing_user = session.query(User).filter(User.phone == phone).first()
        if existing_user:
            # Если пользователь существует, обновляем баллы через add_points_with_history
            if manual_points > 0:
                add_points_with_history(
                    existing_user.id,
                    'manual',
                    manual_points,
                    'Ручное добавление баллов'
                )
            if referral_points > 0:
                add_points_with_history(
                    existing_user.id,
                    'referral',
                    referral_points,
                    'Ручное добавление реферальных баллов'
                )
            
            if first_name and not existing_user.first_name:
                existing_user.first_name = first_name
            if last_name and not existing_user.last_name:
                existing_user.last_name = last_name
            
            session.commit()
            return True, f"✅ Пользователь существует. Обновлены баллы. Всего: {existing_user.get_total_points()}", existing_user.id
        
        # Генерируем реферальный код
        referral_code = generate_referral_code()
        while session.query(User).filter(User.referral_code == referral_code).first():
            referral_code = generate_referral_code()
        
        # Создаем нового пользователя
        new_user = User(
            phone=phone,
            first_name=first_name,
            last_name=last_name,
            referral_code=referral_code,
            points_manual=manual_points,
            points_referral=referral_points,
            tg_id=None  # Без привязки к Telegram
        )
        
        session.add(new_user)
        session.commit()
        
        # Добавляем баллы в историю
        if manual_points > 0:
            add_points_with_history(
                new_user.id,
                'manual',
                manual_points,
                'Создание с ручными баллами'
            )
        
        if referral_points > 0:
            add_points_with_history(
                new_user.id,
                'referral',
                referral_points,
                'Создание с реферальными баллами'
            )
        
        return True, f"✅ Пользователь создан! ID: {new_user.id}, Баллы: {new_user.get_total_points()}", new_user.id
        
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding manual user: {e}")
        return False, f"❌ Ошибка: {str(e)[:50]}", 0
    finally:
        session.close()

def update_user_points_with_history(user_id: int, points_type: str, points: int, description: str = None):
    """Обновить баллы пользователя с записью в историю"""
    return add_points_with_history(user_id, points_type, points, description)

def add_points_with_history(user_id: int, points_type: str, amount: int, description: str = None):
    """
    Добавляет баллы с записью в историю
    """
    session = Session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            logger.error(f"Пользователь с ID {user_id} не найден")
            return False
        
        logger.info(f"Начинаем добавление баллов: user_id={user_id}, type={points_type}, amount={amount}")
        
        # Добавляем баллы в зависимости от типа
        now = datetime.now()
        
        if points_type == 'manual':
            user.points_manual += amount
            user.last_manual_points_update = now
            logger.info(f"Добавлено {amount} ручных баллов. last_manual_points_update = {now}")
            
        elif points_type == 'referral':
            user.points_referral += amount
            user.last_referral_points_update = now
            logger.info(f"Добавлено {amount} реферальных баллов. last_referral_points_update = {now}")
            
        elif points_type == 'welcome':
            user.points_manual += amount
            user.last_manual_points_update = now
            logger.info(f"Добавлено {amount} приветственных баллов. last_manual_points_update = {now}")
            
        elif points_type == 'admin':
            user.points_manual += amount
            user.last_manual_points_update = now
            logger.info(f"Добавлено {amount} админских баллов. last_manual_points_update = {now}")
            
        else:
            # По умолчанию считаем manual
            user.points_manual += amount
            user.last_manual_points_update = now
            logger.warning(f"Неизвестный тип баллов '{points_type}', использован manual. last_manual_points_update = {now}")
        
        # Записываем в историю
        history_record = PointsHistory(
            user_id=user_id,
            points_type=points_type,
            points_amount=amount,
            description=description,
            created_at=now
        )
        session.add(history_record)
        logger.info(f"Создана запись в истории: {description}")
        
        session.commit()
        logger.info(f"Успешно добавлены баллы пользователю {user_id}")
        
        # Проверяем, сохранились ли даты
        session.refresh(user)
        logger.info(f"Проверка после коммита: last_manual={user.last_manual_points_update}, last_referral={user.last_referral_points_update}")
        
        return True
        
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка добавления баллов с историей: {e}", exc_info=True)
        return False
    finally:
        session.close()

def get_points_history(user_id: int, limit: int = 10):
    """Получить историю начисления баллов пользователя"""
    session = Session()
    try:
        return session.query(PointsHistory)\
            .filter(PointsHistory.user_id == user_id)\
            .order_by(PointsHistory.created_at.desc())\
            .limit(limit)\
            .all()
    finally:
        session.close()

def get_user_points_summary(user_id: int):
    """Получить сводку по баллам пользователя"""
    session = Session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if not user:
            return None
        
        # Суммируем по типам из истории
        from sqlalchemy import func
        
        summary = session.query(
            PointsHistory.points_type,
            func.sum(PointsHistory.points_amount).label('total')
        ).filter(
            PointsHistory.user_id == user_id
        ).group_by(
            PointsHistory.points_type
        ).all()
        
        result = {
            'user': user,
            'manual_points': user.points_manual,
            'referral_points': user.points_referral,
            'total_points': user.get_total_points(),
            'last_manual_update': user.last_manual_points_update,
            'last_referral_update': user.last_referral_points_update,
            'history_summary': dict(summary)
        }
        
        return result
        
    finally:
        session.close()


def get_points_statistics(start_date: datetime = None, end_date: datetime = None):
    """Статистика начисления баллов по периодам"""
    session = Session()
    try:
        from sqlalchemy import func
        
        query = session.query(
            func.date(PointsHistory.created_at).label('date'),
            PointsHistory.points_type,
            func.sum(PointsHistory.points_amount).label('total')
        ).group_by(
            func.date(PointsHistory.created_at),
            PointsHistory.points_type
        ).order_by(
            func.date(PointsHistory.created_at).desc()
        )
        
        if start_date:
            query = query.filter(PointsHistory.created_at >= start_date)
        if end_date:
            query = query.filter(PointsHistory.created_at <= end_date)
        
        return query.limit(30).all()
        
    finally:
        session.close()