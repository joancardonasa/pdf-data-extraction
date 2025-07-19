from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Commodities(Base):
    __tablename__ = 'commodities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    week_date = Column(DateTime, nullable=False)
    name = Column(String, nullable=False)
    price = Column(Float)
    one_m = Column("1M", Float)
    three_m = Column("3M", Float)
    six_m = Column("6M", Float)
    twelve_m = Column("12M", Float)
    ytd = Column("YTD", Float)
    qtd = Column("QTD", Float)
