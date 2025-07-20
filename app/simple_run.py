import logging
import pandas as pd
from pdf_tables_parser import PDFMarketParser
from main import OUTPUT_PATH, INPUT_PATH, PDF_PATH
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from models import Base, Equities, Rates, Credit, Commodities, ExchangeRates, MajorEvents

logger = logging.getLogger(__name__)

DATABASE_URL = "postgresql://user:pass@db:5432/markets_weekly"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def store_equities(session: Session, parser: PDFMarketParser, df_name: str='equities'):
    if df_name not in parser._current_processed_dfs.keys():
        return
    df = parser._current_processed_dfs[df_name]
    week_start = get_week_start(datetime.now())

    for _, row in df.iterrows():
        existing = session.query(Equities).filter(
            Equities.equity == row['Equities'],
            Equities.week == week_start
        ).one_or_none()

        if existing:
            existing.price = row['Price']
            existing.m1 = row['1M']
            existing.m3 = row['3M']
            existing.m6 = row['6M']
            existing.m12 = row['12M']
            existing.ytd = row['YTD']
            existing.qtd = row['QTD']
        else:
            new_record = Equities(
                equity=row['Equities'],
                price=row['Price'],
                m1=row['1M'],
                m3=row['3M'],
                m6=row['6M'],
                m12=row['12M'],
                ytd=row['YTD'],
                qtd=row['QTD'],
                week=week_start
            )
            session.add(new_record)

    session.commit()

def store_rates(session: Session, parser: PDFMarketParser, df_name: str='rates'):
    if df_name not in parser._current_processed_dfs.keys():
        return
    df = parser._current_processed_dfs[df_name]
    week_start = get_week_start(datetime.now())

    for _, row in df.iterrows():
        existing = session.query(Rates).filter(
            Rates.rate == row['Rates (government bonds)'],
            Rates.week == week_start
        ).one_or_none()

        if existing:
            existing.yield_percent = row['Yield (%)']
            existing.m1 = row['1M']
            existing.m3 = row['3M']
            existing.m6 = row['6M']
            existing.m12 = row['12M']
            existing.ytd = row['YTD']
            existing.qtd = row['QTD']
        else:
            new_record = Rates(
                rate=row['Rates (government bonds)'],
                yield_percent=row['Yield (%)'],
                m1=row['1M'],
                m3=row['3M'],
                m6=row['6M'],
                m12=row['12M'],
                ytd=row['YTD'],
                qtd=row['QTD'],
                week=week_start
            )
            session.add(new_record)

    session.commit()

def store_credit(session: Session, parser: PDFMarketParser, df_name: str='credit'):
    if df_name not in parser._current_processed_dfs.keys():
        return
    df = parser._current_processed_dfs[df_name]
    week_start = get_week_start(datetime.now())

    for _, row in df.iterrows():
        existing = session.query(Credit).filter(
            Credit.credit == row['Credit'],
            Credit.week == week_start
        ).one_or_none()

        if existing:
            existing.oas_bp = row['OAS (bp)']
            existing.m1 = row['1M']
            existing.m3 = row['3M']
            existing.m6 = row['6M']
            existing.m12 = row['12M']
            existing.ytd = row['YTD']
            existing.qtd = row['QTD']
        else:
            new_record = Credit(
                credit=row['Credit'],
                oas_bp=row['OAS (bp)'],
                m1=row['1M'],
                m3=row['3M'],
                m6=row['6M'],
                m12=row['12M'],
                ytd=row['YTD'],
                qtd=row['QTD'],
                week=week_start
            )
            session.add(new_record)

    session.commit()

def store_commodities(session: Session, parser: PDFMarketParser, df_name: str='commodities'):
    if df_name not in parser._current_processed_dfs.keys():
        return
    df = parser._current_processed_dfs[df_name]
    for _, row in df.iterrows():
        # As a test, we're adding the current datetime, but this parameter needs to be set by the process to know which 
        # date corresponds to the PDF's data
        week_start = get_week_start(datetime.now())

        existing = session.query(Commodities).filter(
            Commodities.commodity == row['Commodities'],
            Commodities.week == week_start
        ).one_or_none()

        if existing:
            existing.price = row['Price']
            existing.m1 = row['1M']
            existing.m3 = row['3M']
            existing.m6 = row['6M']
            existing.m12 = row['12M']
            existing.ytd = row['YTD']
            existing.qtd = row['QTD']
        else:
            new_record = Commodities(
                commodity=row['Commodities'],
                price=row['Price'],
                m1=row['1M'],
                m3=row['3M'],
                m6=row['6M'],
                m12=row['12M'],
                ytd=row['YTD'],
                qtd=row['QTD'],
                week=week_start
            )
            session.add(new_record)

    session.commit()

def store_exchange_rates(session: Session, parser: PDFMarketParser, df_name: str='exchange_rates'):
    if df_name not in parser._current_processed_dfs.keys():
        return
    df = parser._current_processed_dfs[df_name]
    week_start = get_week_start(datetime.now())

    for _, row in df.iterrows():
        existing = session.query(ExchangeRates).filter(
            ExchangeRates.exchange_rate == row['Exchange rates'],
            ExchangeRates.week == week_start
        ).one_or_none()

        if existing:
            existing.price = row['Price']
            existing.m1 = row['1M']
            existing.m3 = row['3M']
            existing.m6 = row['6M']
            existing.m12 = row['12M']
            existing.ytd = row['YTD']
            existing.qtd = row['QTD']
        else:
            new_record = ExchangeRates(
                exchange_rate=row['Exchange rates'],
                price=row['Price'],
                m1=row['1M'],
                m3=row['3M'],
                m6=row['6M'],
                m12=row['12M'],
                ytd=row['YTD'],
                qtd=row['QTD'],
                week=week_start
            )
            session.add(new_record)

    session.commit()

def store_major_events(session: Session, parser: PDFMarketParser, df_name: str='major_events'):
    if df_name not in parser._current_processed_dfs.keys():
        return
    df = parser._current_processed_dfs[df_name]
    for _, row in df.iterrows():
        week_start = get_week_start(datetime.now())

        # We define "uniqueness" per country AND indicator
        existing = session.query(MajorEvents).filter(
            MajorEvents.country == row['Country'],
            MajorEvents.indicator_event == row['Indicator/Event'],
            MajorEvents.week == week_start
        ).one_or_none()

        if existing:
            existing.date = row['Date']
            existing.time = row['Time']
            existing.country = row['Country']
            existing.period = row['Period']
            existing.unicredit_estimates = row['UniCredit Estimates']
            existing.consensus_bloomberg = row['Consensus (Bloomberg)']
            existing.previous = row['Previous']
        else:
            new_record = MajorEvents(
                date=row['Date'],
                time=row['Time'],
                country=row['Country'],
                indicator_event=row['Indicator/Event'],
                period=row['Period'],
                unicredit_estimates=row['UniCredit Estimates'],
                consensus_bloomberg=row['Consensus (Bloomberg)'],
                previous=row['Previous'],
                week=week_start
            )
            session.add(new_record)

    session.commit()

def get_week_start(date: datetime) -> datetime:
    week_start = date - timedelta(days=date.weekday())
    return week_start.replace(hour=0, minute=0, second=0, microsecond=0)


def main():
    parser = PDFMarketParser(
        pdf_path=PDF_PATH,
        year=2025
    )
    parser.parse_markets_at_a_glance()
    parser.parse_major_events_next_week()

    parser.consolidate_and_export_top_bottom_markets(OUTPUT_PATH)

    parser.export_dfs_to_csv(OUTPUT_PATH)

    db = SessionLocal()
    store_equities(db, parser)
    store_rates(db, parser)
    store_credit(db, parser)
    store_commodities(db, parser)
    store_exchange_rates(db, parser)

    store_major_events(db, parser)
    db.close()

if __name__ == "__main__":
    main()
