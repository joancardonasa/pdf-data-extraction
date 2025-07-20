import logging
import pandas as pd
from pdf_tables_parser import PDFMarketParser
from main import OUTPUT_PATH, INPUT_PATH, PDF_PATH
from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import create_engine
from models import Commodities, Base

logger = logging.getLogger(__name__)

# Here we have to connect to localhost rather than db as it's outside the docker container
DATABASE_URL = "postgresql://user:pass@localhost:5432/markets_weekly"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


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


def get_week_start(date: datetime) -> datetime:
    return date - timedelta(days=date.weekday())


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
    store_commodities(db, parser)
    db.close()

if __name__ == "__main__":
    main()
