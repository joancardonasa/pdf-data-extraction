import logging
from pdf_tables_parser import PDFMarketParser

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

OUTPUT_PATH = "data/output"
INPUT_PATH = "data/input"
PDF_PATH = f"{INPUT_PATH}/241025 Unicredit Macro & Markets Weekly Focus - python.pdf"

def main():
    parser = PDFMarketParser(
        pdf_path=PDF_PATH,
        year=2025
    )
    parser.parse_markets_at_a_glance()
    parser.parse_major_events_next_week()
    parser.consolidate_and_export_top_bottom_markets(OUTPUT_PATH)
    parser.export_dfs_to_csv(OUTPUT_PATH)
    # parser.display_summary()

if __name__ == "__main__":
    main()
