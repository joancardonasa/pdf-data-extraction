import pytest
from unittest.mock import MagicMock, patch
from app.pdf_tables_parser import PDFMarketParser


@pytest.fixture
def parser():
    return PDFMarketParser(pdf_path="test.pdf")

def test_parse_raises_exception_on_unexpected_number_of_tables(parser):
    with patch("app.pdf_tables_parser.camelot.read_pdf") as mock_read:
        mock_tables = MagicMock()
        mock_tables.n = 2
        mock_read.return_value = mock_tables
        with pytest.raises(Exception):
            parser.parse_markets_at_a_glance()

def test_export_writes_csv_files(parser):
    df = MagicMock()
    df.name = "table1"
    parser._current_processed_dfs = [df]
    with patch("app.pdf_tables_parser.logger"):
        parser.export_dfs_to_csv("/tmp/output")
    df.to_csv.assert_called_once_with("/tmp/output/table1.csv", index=False)

def test_export_raises_value_error_on_empty_output_path(parser):
    df = MagicMock()
    df.name = "table1"
    parser._current_processed_dfs = [df]
    with patch("app.pdf_tables_parser.logger"):
        with pytest.raises(ValueError):
            parser.export_dfs_to_csv("")
