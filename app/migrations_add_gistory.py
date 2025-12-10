# migration_add_history.py - запусти один раз
from models import Session, User, PointsHistory, Base
from sqlalchemy import create_engine
from datetime import datetime

def migrate_to_history():
    """Переносит существующие баллы в историю"""
    session = Session()
    try:
        # Получаем всех пользователей с баллами
        users_with_points = session.query(User).filter(
            (User.points_manual > 0) | (User.points_referral > 0)
        ).all()
        
        migrated_count = 0
        
        for user in users_with_points:
            # Ручные баллы
            if user.points_manual > 0:
                history = PointsHistory(
                    user_id=user.id,
                    points_type='manual',
                    points_amount=user.points_manual,
                    description='Перенос существующих баллов (миграция)',
                    created_at=datetime.now()
                )
                session.add(history)
                user.last_manual_points_update = datetime.now()
            
            # Реферальные баллы
            if user.points_referral > 0:
                history = PointsHistory(
                    user_id=user.id,
                    points_type='referral',
                    points_amount=user.points_referral,
                    description='Перенос реферальных баллов (миграция)',
                    created_at=datetime.now()
                )
                session.add(history)
                user.last_referral_points_update = datetime.now()
            
            migrated_count += 1
        
        session.commit()
        print(f"✅ Миграция завершена. Обработано {migrated_count} пользователей")
        
    except Exception as e:
        session.rollback()
        print(f"❌ Ошибка миграции: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    migrate_to_history()