import pandas as pd
import numpy as np
import camelot
import logging
from typing import List

logger = logging.getLogger(__name__)


class PDFMarketParser:
    MARKETS_AT_A_GLANCE_TITLES = [
        "Equities", "Rates (government bonds)", "Credit", "Commodities", "Exchange rates"
    ]
    CLEAN_MAAG_TABLE_TITLES = [
        name.lower().replace(" ", "_").replace("(", "").replace(")", "") for name in MARKETS_AT_A_GLANCE_TITLES
    ]

    MARKETS_AT_A_GLANCE_NUMERIC_COLS = [
        "1M", "3M", "6M", "12M", "YTD", "QTD",
        "Price", "Yield (%)", "QAS (bp)"
    ]

    MAJOR_EVENTS_COLS = [
        "Date", "Time", "Country", "Indicator/Event", "Period", "UniCredit Estimates", "Consensus (Bloomberg)", "Previous"
    ]

    def __init__(self, pdf_path: str, year: int = 2025):
        self.pdf_path = pdf_path
        self.year = year
        self._current_processed_dfs: dict[str, pd.DataFrame] = {}

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

        for df, name in zip(cleaned_dfs, self.CLEAN_MAAG_TABLE_TITLES):
            df.name = name.lower().replace(" ", "_").replace("(", "").replace(")", "")
            self._current_processed_dfs[df.name] = df

    def parse_major_events_next_week(self, page: int = 3) -> None:
        # Tolerance values are VERY finnicky. The whole process can break very easily with small layout
        # changes in the source pdf docs.
        tables = camelot.read_pdf(self.pdf_path, pages=str(page), flavor="stream", row_tol=11)

        if tables.n != 1:
            raise Exception(f"Unexpected number of tables found: {tables.n}")

        raw_major_events_df = tables[0].df
        major_events_df = raw_major_events_df

        header_row_idx = None
        for idx, row in raw_major_events_df.iterrows():
            matches = sum(
                any(str(cell).lower().strip().find(h.lower()) != -1 for cell in row)
                for h in self.MAJOR_EVENTS_COLS
            )
            if matches >= 5:
                header_row_idx = idx
                break

        if header_row_idx is not None:
            major_events_df.columns = major_events_df.iloc[header_row_idx]
            # Delete rows until header row
            major_events_df = major_events_df.iloc[header_row_idx+1:].reset_index(drop=True)
            major_events_df.columns = self.MAJOR_EVENTS_COLS
        else:
            raise RuntimeError(f"Could not find header row in page {page}!")

        # Cleanup:
        major_events_df["Date"] = major_events_df["Date"].replace("", np.nan)
        major_events_df["Date"] = pd.to_datetime(
            major_events_df["Date"], 
            format="%a, %d %b", 
            errors="coerce"
        )
        major_events_df["Date"] = major_events_df["Date"].apply(lambda dt: dt.replace(year=2025) if pd.notnull(dt) else dt)
        major_events_df["Date"] = major_events_df["Date"].ffill()

        # Ensure numeric columns are numeric:
        cols_to_convert = ["UniCredit Estimates", "Consensus (Bloomberg)", "Previous"]
        major_events_df[cols_to_convert] = major_events_df[cols_to_convert].apply(
            pd.to_numeric, errors="coerce"
        )

        # We ensure we don't keep any null/empty values in the Indicator/Event column
        major_events_df = major_events_df[major_events_df['Indicator/Event'].str.strip() != ''].reset_index(drop=True)

        major_events_df.name = "Major Events".lower().replace(" ", "_").replace("(", "").replace(")", "")
        self._current_processed_dfs[major_events_df.name] = major_events_df

    def consolidate_and_export_top_bottom_markets(self, output_path: str):
        """
        For the 5 Market tables we extracted, we add a new column indicating the Market Type (the name of the table)
        and we rename the main column to Market, to be able to concatenate them and find top and bottom performer in the last 12M
        """
        if not self._current_processed_dfs:
            raise ValueError("No processed dataframes available to consolidate.")

        expected_names = set(self.CLEAN_MAAG_TABLE_TITLES)
        actual_names = set(df.name for df in self._current_processed_dfs.values())

        if not expected_names.issubset(actual_names):
            raise ValueError(f"Processed DataFrames names {actual_names} do not match expected {expected_names}")

        rename_map = {
            "Equities": "Market",
            "Rates (government bonds)": "Market",
            "Credit": "Market",
            "Commodities": "Market",
            "Exchange rates": "Market",
        }

        dfs = []
        for df, market_type_name in zip(self._current_processed_dfs.values(), self.MARKETS_AT_A_GLANCE_TITLES):
            df = df.copy()
            df["Market Type"] = market_type_name
            df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
            dfs.append(df)

        consolidated_df = pd.concat(dfs)
        consolidated_df = consolidated_df.drop(["Price", "Yield (%)", "OAS (bp)"], axis=1)
        consolidated_df.to_csv(f"{output_path}/performance_metrics.csv", index=False)

        if '12M' not in consolidated_df.columns:
            raise ValueError("12M column not found in consolidated performance metrics data")

        consolidated_df['12M'] = pd.to_numeric(consolidated_df['12M'], errors='coerce')
        valid_data = consolidated_df.dropna(subset=['12M'])

        if len(valid_data) < 6:
            raise Exception(f"Only {len(valid_data)} valid entries found. Not have enough data for top 3 and bottom 3.")
        sorted_df = valid_data.sort_values('12M', ascending=False)

        top_3_df = sorted_df.head(3).copy()
        bottom_3_df = sorted_df.tail(3).copy()
        
        top_3_df.to_csv(f"{output_path}/top_3_markets_12M.csv", index=False)
        bottom_3_df.to_csv(f"{output_path}/bottom_3_markets_12M.csv", index=False)

    def export_dfs_to_csv(self, output_path: str) -> List:
        """
        Export the stored processed dataframes to CSV.
        """
        output_paths = []
        if not self._current_processed_dfs:
            logger.warning("No processed dataframes to export.")
            return

        if not output_path:
            logger.error("Not a valid output path provided.")
            raise ValueError("Output path must not be empty.")

        for df in self._current_processed_dfs.values():
            file_path = f"{output_path}/{df.name}.csv"
            output_paths.append(file_path)
            df.to_csv(file_path, index=False)
            logger.info(f"Exported dataframe '{df.name}' to '{file_path}'")
        logger.info(f"Exported {len(self._current_processed_dfs)} dataframes to {output_path}")
        return output_paths

    def display_summary(self):
        """
        Displays a summary of the current processed dataframes.
        """
        if not self._current_processed_dfs:
            logger.warning("No processed dataframes to display.")
            return

        for df in self._current_processed_dfs.values():
            logger.info(f"{df.name}: {len(df)} rows")
