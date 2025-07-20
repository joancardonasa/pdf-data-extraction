import logging
from pdf_tables_parser import PDFMarketParser
from main import OUTPUT_PATH, INPUT_PATH, PDF_PATH

logger = logging.getLogger(__name__)


def main():
    parser = PDFMarketParser(
        pdf_path=PDF_PATH,
        year=2025
    )
    parser.parse_markets_at_a_glance()
    parser.parse_major_events_next_week()
    parser.consolidate_and_export_top_bottom_markets(OUTPUT_PATH)
    parser.export_dfs_to_csv(OUTPUT_PATH)

if __name__ == "__main__":
    main()
