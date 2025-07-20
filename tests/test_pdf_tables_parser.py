import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from app.pdf_tables_parser import PDFMarketParser


@pytest.fixture
def parser():
    return PDFMarketParser(pdf_path="test.pdf")

def make_major_events_table_df():
    data = [
        ['test', '', '', '', '', '', '', 'dsjfhdijkshfiudshf'],
        ['Date', 'Time', 'Country', 'Indicator/Event', 'Period', 'UniCredit Estimates', 'Consensus (Bloomberg)', 'Previous'],
        ['Mon, 01 Jan', '10:00', 'ES', 'Event 1', 'Q1', '1.1', '1.2', '1.3'],
        ['Wed, 03 Jan', '12:00', 'FR', 'Event 2', 'Q2', '2.1', '2.2', '2.3'],
    ]
    return pd.DataFrame(data)

def test_parse_raises_exception_on_unexpected_number_of_tables_maag(parser):
    with patch("app.pdf_tables_parser.camelot.read_pdf") as mock_read:
        mock_tables = MagicMock()
        mock_tables.n = 2
        mock_read.return_value = mock_tables
        with pytest.raises(Exception):
            parser.parse_markets_at_a_glance()

def test_parse_raises_exception_on_unexpected_number_of_tables_major_events(parser):
    with patch("app.pdf_tables_parser.camelot.read_pdf") as mock_read:
        mock_tables = MagicMock()
        mock_tables.n = 48
        mock_read.return_value = mock_tables
        with pytest.raises(Exception):
            parser.parse_major_events_next_week()

def test_current_processed_dfs_grows_by_one_with_parses(parser):
    with patch("app.pdf_tables_parser.camelot.read_pdf") as mock_read:
        mock_tables = MagicMock()
        mock_tables.n = 1
        mock_tables.__getitem__.return_value.df = make_major_events_table_df()
        mock_read.return_value = mock_tables

        initial_len = len(parser._current_processed_dfs)
        parser.parse_major_events_next_week()
        final_len = len(parser._current_processed_dfs)

        assert final_len == initial_len + 1

def test_export_writes_csv_files(parser):
    df = MagicMock()
    df.name = "table1"
    parser._current_processed_dfs = {"table1": df}
    with patch("app.pdf_tables_parser.logger"):
        parser.export_dfs_to_csv("/tmp/output")
    df.to_csv.assert_called_once_with("/tmp/output/table1.csv", index=False)

def test_export_raises_value_error_on_empty_output_path(parser):
    df = MagicMock()
    df.name = "table1"
    parser._current_processed_dfs = {"table1": df}
    with patch("app.pdf_tables_parser.logger"):
        with pytest.raises(ValueError):
            parser.export_dfs_to_csv("")
