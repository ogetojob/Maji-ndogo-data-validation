"""
field_data_processor.py

This module provides a class for processing field data for the
Maji Ndogo project. It handles:
- Ingesting data from a SQLite database
- Renaming columns
- Correcting crop values and ensuring numeric consistency
- Merging weather station mapping data

Author: Job Ogeto
Date: 2025-10-04
"""

import logging
import pandas as pd

# Local imports
from data_ingestion import create_db_engine, query_data, read_from_web_CSV


class FieldDataProcessor:
    """
    A class for processing field data from the Maji Ndogo farm database.

    Attributes
    ----------
    db_path : str
        Path to the SQLite database.
    sql_query : str
        SQL query for data extraction.
    columns_to_rename : dict
        Dictionary mapping old to new column names.
    values_to_rename : dict
        Dictionary mapping incorrect crop values to correct ones.
    weather_mapping_csv : str
        URL of the weather mapping CSV.
    engine : sqlalchemy.Engine
        SQLAlchemy database engine.
    df : pandas.DataFrame
        Processed field data.

    Methods
    -------
    ingest_sql_data():
        Load field data from the SQLite database.
    rename_columns():
        Swap column names based on configuration.
    apply_corrections():
        Correct crop names and fix elevation values.
    weather_station_mapping():
        Merge weather station mapping into the field dataset.
    process():
        Run the full processing pipeline.
    """

    def __init__(self, config_params: dict, logging_level: int = logging.INFO):
        """
        Initialize the FieldDataProcessor with configuration parameters.

        Parameters
        ----------
        config_params : dict
            Configuration parameters containing:
            - 'db_path' : str
                Path to the SQLite database.
            - 'sql_query' : str
                SQL query string for extracting data.
            - 'columns_to_rename' : dict
                Dictionary mapping columns to swap.
            - 'values_to_rename' : dict
                Dictionary mapping incorrect crop values to corrected ones.
            - 'weather_mapping_csv_path' : str
                URL for the weather mapping CSV file.
        logging_level : int, optional
            Logging verbosity (default is logging.INFO).

        Raises
        ------
        ValueError
            If any required configuration parameter is missing.
        """
        self.db_path = config_params['db_path']
        self.sql_query = config_params['sql_query']
        self.columns_to_rename = config_params['columns_to_rename']
        self.values_to_rename = config_params['values_to_rename']
        self.weather_mapping_csv = config_params['weather_mapping_csv_path']

        # Logging setup
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initialize_logging(logging_level)

        # Placeholders
        self.engine = None
        self.df = None

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    def _initialize_logging(self, logging_level: int):
        """
        Configure the logger for this class.

        Parameters
        ----------
        logging_level : int
            Logging level (e.g., logging.INFO, logging.DEBUG).

        Returns
        -------
        None
        """
        self.logger.setLevel(logging_level)
        if not self.logger.handlers:
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging_level)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)

    # ------------------------------------------------------------------
    # 1. Data Ingestion
    # ------------------------------------------------------------------
    def ingest_sql_data(self) -> pd.DataFrame:
        """
        Create a database engine, execute the SQL query, and load the data
        into a pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
            The ingested dataset containing field, soil, and crop features.

        Raises
        ------
        ValueError
            If the database path or SQL query is invalid.
        """
        self.logger.info("Creating database engine...")
        self.engine = create_db_engine(self.db_path)
        self.logger.info("Executing SQL query to load field data...")
        self.df = query_data(self.engine, self.sql_query)
        self.logger.info(f"Data ingestion complete. Rows loaded: {len(self.df)}")
        return self.df

    # ------------------------------------------------------------------
    # 2. Column Renaming
    # ------------------------------------------------------------------
    def rename_columns(self):
        """
        Swap two column names based on the configuration dictionary.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the DataFrame is empty or not yet ingested.
        """
        if self.df is None:
            raise ValueError("DataFrame is empty. Run ingest_sql_data() first.")

        self.logger.info("Renaming columns...")
        col1, col2 = list(self.columns_to_rename.keys())[0], list(self.columns_to_rename.values())[0]
        temp_name = "__temp_swap__"
        while temp_name in self.df.columns:
            temp_name += "_"

        self.df.rename(columns={col1: temp_name, col2: col1}, inplace=True)
        self.df.rename(columns={temp_name: col2}, inplace=True)
        self.logger.info(f"Swapped columns: {col1} ↔ {col2}")

    # ------------------------------------------------------------------
    # 3. Data Corrections
    # ------------------------------------------------------------------
    def apply_corrections(self, column_name: str = 'Crop_type', abs_column: str = 'Elevation'):
        """
        Apply corrections to crop type values and ensure elevation values are absolute.

        Parameters
        ----------
        column_name : str, optional
            The column containing crop types (default is 'Crop_type').
        abs_column : str, optional
            The column to convert values to absolute (default is 'Elevation').

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the DataFrame is empty or not yet ingested.
        """
        if self.df is None:
            raise ValueError("DataFrame is empty. Run ingest_sql_data() first.")

        self.logger.info("Applying crop corrections and numeric adjustments...")
        self.df[abs_column] = self.df[abs_column].abs()
        self.df[column_name] = self.df[column_name].apply(
            lambda val: self.values_to_rename.get(val, val)
        )
        self.logger.info("Corrections applied successfully.")

    # ------------------------------------------------------------------
    # 4. Weather Station Mapping
    # ------------------------------------------------------------------
    def weather_station_mapping(self):
        """
        Merge weather station mapping data from a CSV into the field data.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If the DataFrame is empty or not yet ingested.
        Exception
            If the CSV cannot be read or merged.
        """
        if self.df is None:
            raise ValueError("DataFrame is empty. Run ingest_sql_data() first.")

        self.logger.info("Merging weather station mapping data...")
        try:
            mapping_df = read_from_web_CSV(self.weather_mapping_csv)
            self.df = self.df.merge(mapping_df, on="Field_ID", how="left")

            # ✅ Fix column naming for consistency
            if "Weather_station_ID" in self.df.columns:
                self.df.rename(columns={"Weather_station_ID": "Weather_station"}, inplace=True)

            self.logger.info(
                f"Weather mapping merged. Added columns: {list(mapping_df.columns)}"
            )
        except Exception as e:
            self.logger.error(f"Error merging weather mapping data: {e}")
            raise

    # ------------------------------------------------------------------
    # 5. Full Processing Pipeline
    # ------------------------------------------------------------------
    def process(self) -> pd.DataFrame:
        """
        Execute the full data processing pipeline.

        The pipeline consists of:
        1. Ingesting SQL data from the database.
        2. Renaming columns for consistency.
        3. Applying corrections to crop names and elevation values.
        4. Merging weather station mapping data.

        Returns
        -------
        pandas.DataFrame
            The fully processed dataset ready for analysis.

        Raises
        ------
        ValueError
            If processing fails and the DataFrame remains None.
        """
        self.ingest_sql_data()
        self.rename_columns()
        self.apply_corrections()
        self.weather_station_mapping()
        self.logger.info("✅ Field data processing pipeline completed.")
        if self.df is None:
            raise ValueError("Processing failed: DataFrame is None.")
        return self.df
