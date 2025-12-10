from sqlalchemy import create_engine, Column, Integer, String, BigInteger, ForeignKey, Boolean, Text, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
import sqlite3
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    tg_id = Column(BigInteger, unique=True)
    first_name = Column(String(100), nullable=True) 
    last_name = Column(String(100), nullable=True)
    username = Column(String(100), nullable=True)
    phone = Column(String(20), unique=True)
    referral_code = Column(String(50), unique=True)
    points_referral = Column(Integer, default=0)
    points_manual = Column(Integer, default=0)
    invited_by = Column(String(50), ForeignKey('users.referral_code'), nullable=True)
    last_manual_points_update = Column(DateTime, nullable=True)
    last_referral_points_update = Column(DateTime, nullable=True)

    
    def get_total_points(self):
        """Вместо свойства используем метод"""
        return (self.points_referral or 0) + (self.points_manual or 0)

class PointsHistory(Base):
    __tablename__ = 'points_history'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    points_type = Column(String(20))  # 'manual', 'referral', 'welcome', 'admin_add'
    points_amount = Column(Integer)
    description = Column(String(255), nullable=True)  # Описание начисления
    created_at = Column(DateTime, default=datetime.now)
    
    # Связь с пользователем
    user = relationship('User', backref='points_history')

class Referral(Base):
    __tablename__ = 'referrals'
    
    id = Column(Integer, primary_key=True)
    referrer_code = Column(String(50), ForeignKey('users.referral_code'))
    referred_phone = Column(String(20))
    points_awarded = Column(Integer, default=0)
    
class SupportTicket(Base):
    __tablename__ = 'support_tickets'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(BigInteger)  # ID пользователя в Telegram
    user_question = Column(Text)  # Вопрос пользователя
    group_message_id = Column(Integer)  # ID сообщения в группе
    is_answered = Column(Boolean, default=False)  # Отвечено ли
    answer_text = Column(Text, nullable=True)  # Ответ менеджера
    created_at = Column(DateTime, default=datetime.now)
    answered_at = Column(DateTime, nullable=True)

# Создание таблиц
engine = create_engine('sqlite:///bot.app.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)