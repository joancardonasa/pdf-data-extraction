from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Equities(Base):
    __tablename__ = "equities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    equity = Column(String, nullable=False)
    price = Column(Float, nullable=True)
    m1 = Column(Float, nullable=True)
    m3 = Column(Float, nullable=True)
    m6 = Column(Float, nullable=True)
    m12 = Column(Float, nullable=True)
    ytd = Column(Float, nullable=True)
    qtd = Column(Float, nullable=True)
    week = Column(DateTime, nullable=False)


class Rates(Base):
    __tablename__ = "rates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    rate = Column(String, nullable=False)
    yield_percent = Column(Float, nullable=True)
    m1 = Column(Float, nullable=True)
    m3 = Column(Float, nullable=True)
    m6 = Column(Float, nullable=True)
    m12 = Column(Float, nullable=True)
    ytd = Column(Float, nullable=True)
    qtd = Column(Float, nullable=True)
    week = Column(DateTime, nullable=False)


class Credit(Base):
    __tablename__ = "credit"

    id = Column(Integer, primary_key=True, autoincrement=True)
    credit = Column(String, nullable=False)
    oas_bp = Column(Float, nullable=True)
    m1 = Column(Float, nullable=True)
    m3 = Column(Float, nullable=True)
    m6 = Column(Float, nullable=True)
    m12 = Column(Float, nullable=True)
    ytd = Column(Float, nullable=True)
    qtd = Column(Float, nullable=True)
    week = Column(DateTime, nullable=False)


class Commodities(Base):
    __tablename__ = 'commodities'

    id = Column(Integer, primary_key=True, autoincrement=True)
    commodity = Column(String, nullable=False)
    price = Column(Float, nullable=True)
    m1 = Column(Float, nullable=True)
    m3 = Column(Float, nullable=True)
    m6 = Column(Float, nullable=True)
    m12 = Column(Float, nullable=True)
    ytd = Column(Float, nullable=True)
    qtd = Column(Float, nullable=True)
    week = Column(DateTime, nullable=False)


class ExchangeRates(Base):
    __tablename__ = "exchange_rates"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_rate = Column(String, nullable=False)
    price = Column(Float, nullable=True)
    m1 = Column(Float, nullable=True)
    m3 = Column(Float, nullable=True)
    m6 = Column(Float, nullable=True)
    m12 = Column(Float, nullable=True)
    ytd = Column(Float, nullable=True)
    qtd = Column(Float, nullable=True)
    week = Column(DateTime, nullable=False)

class MajorEvents(Base):
    __tablename__ = "major_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(DateTime, nullable=False)
    time = Column(String, nullable=True)
    country = Column(String, nullable=False)
    indicator_event = Column(String, nullable=False)
    period = Column(String, nullable=True)
    unicredit_estimates = Column(Float, nullable=True)
    consensus_bloomberg = Column(Float, nullable=True)
    previous = Column(Float, nullable=True)
    week = Column(DateTime, nullable=False)
