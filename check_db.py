from models import Session, User, Referral

session = Session()

# Проверяем всех пользователей
users = session.query(User).all()
print(f"Всего пользователей: {len(users)}")
for user in users:
    print(f"ID: {user.id}, TG_ID: {user.tg_id}, Phone: {user.phone}, Code: {user.referral_code}")

session.close()