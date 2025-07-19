import pandas as pd
import camelot
import logging

logger = logging.getLogger(__name__)


class PDFMarketParser:
    MARKETS_AT_A_GLANCE_TITLES = [
        "Equities",
        "Rates (government bonds)",
        "Credit",
        "Commodities",
        "Exchange rates"
    ]
    CLEAN_MAAG_TABLE_TITLES = [
        name.lower().replace(" ", "_").replace("(", "").replace(")", "") for name in MARKETS_AT_A_GLANCE_TITLES
    ]

    MARKETS_AT_A_GLANCE_NUMERIC_COLS = [
        "1M", "3M", "6M", "12M", "YTD", "QTD",
        "Price", "Yield (%)", "QAS (bp)"
    ]

    def __init__(self, pdf_path: str, year: int = 2025):
        self.pdf_path = pdf_path
        self.year = year
        self._current_processed_dfs = []

    def parse_markets_at_a_glance(self, page: int = 2) -> None:
        """
        Parses the tables found in the "Markets at a glance" page.
        """
        tables = camelot.read_pdf(self.pdf_path, pages=str(page), flavor="stream", row_tol=10)

        if tables.n != 1:
            # From experimentation we know Camelot detects the 5 "Markets at a glance" tables as one big table,
            # and we need futher filtering to separate them in 5:
            raise Exception(f"Unexpected number of tables found: {tables.n}")

        raw_table = tables[0].df
        title_indexes = raw_table.index[raw_table[0].isin(self.MARKETS_AT_A_GLANCE_TITLES)].tolist()
        title_indexes.append(len(raw_table))

        # Split raw df into the 5 desired tables
        split_tables = []
        for i in range(len(title_indexes)-1):
            start, end = title_indexes[i], title_indexes[i+1]
            df_part = raw_table.iloc[start:end].reset_index(drop=True)
            split_tables.append(df_part)

        # Perform data cleanse on each df
        cleaned_dfs = []
        for df in split_tables:
            # Keep only columns with non-empty header names in the first row
            cols_to_keep = [col for col in df.columns if df.at[0, col].strip() != ""]
            df = df[cols_to_keep]

            # First row as header
            df.columns = df.iloc[0]
            df = df.iloc[1:].reset_index(drop=True)

            # Convert numeric columns
            for col in self.MARKETS_AT_A_GLANCE_NUMERIC_COLS:
                if col in df.columns:
                    # !Remove thousand separator: ,
                    df[col] = df[col].astype(str).str.replace(",", "", regex=False)
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # We assume the YTD column will contain non-null data 
            df = df[df["YTD"].notnull()]

            cleaned_dfs.append(df)

        self._current_processed_dfs = []
        for df, name in zip(cleaned_dfs, self.CLEAN_MAAG_TABLE_TITLES):
            df.name = name.lower().replace(" ", "_").replace("(", "").replace(")", "")
            self._current_processed_dfs.append(df)

    def export_dfs_to_csv(self, output_path: str):
        """
        Export the stored processed dataframes to CSV.
        """
        if not self._current_processed_dfs:
            logger.warning("No processed dataframes to export.")
            return

        if not output_path:
            logger.error("Not a valid output path provided.")
            raise ValueError("Output path must not be empty.")

        for df in self._current_processed_dfs:
            file_path = f"{output_path}/{df.name}.csv"
            df.to_csv(file_path, index=False)
            logger.info(f"Exported dataframe '{df.name}' to '{file_path}'")
        logger.info(f"Exported {len(self._current_processed_dfs)} dataframes to {output_path}")

    def display_summary(self):
        """
        Displays a summary of the current processed dataframes.
        """
        if not self._current_processed_dfs:
            logger.warning("No processed dataframes to display.")
            return

        for df in self._current_processed_dfs:
            logger.info(f"{df.name}: {len(df)} rows")
