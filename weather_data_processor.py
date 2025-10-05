"""
weather_data_processor.py

This module handles weather data processing for the Maji Ndogo project.
It provides functionality to:
- Load weather station data and field-to-weather mappings.
- Clean and extract numeric values from raw text using regex patterns.
- Merge weather data with field survey data.
- Produce a clean, analysis-ready weather DataFrame.

Author: Job Ogeto
Date: 2025-10-04
"""

import logging
import re
import pandas as pd


# -------------------------------------------------------------------
# Logging Configuration
# -------------------------------------------------------------------
logger = logging.getLogger("weather_data_processor")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


# -------------------------------------------------------------------
# Class Definition
# -------------------------------------------------------------------
class WeatherDataProcessor:
    """
    A class for processing weather station data and mapping it to fields.

    Attributes
    ----------
    weather_csv_path : str
        URL for the raw weather station CSV data.
    weather_mapping_csv_path : str
        URL for the field-to-weather-station mapping CSV.
    regex_patterns : dict
        Dictionary of regex patterns to extract weather metrics.
    weather_df : pandas.DataFrame
        Loaded weather station dataset.
    mapping_df : pandas.DataFrame
        Loaded field-weather mapping dataset.
    processed_df : pandas.DataFrame
        Cleaned and merged weather dataset.

    Methods
    -------
    load_data():
        Loads weather station and mapping data from CSV URLs.
    clean_weather_data():
        Extracts numeric values from raw text messages using regex.
    merge_with_mapping():
        Merges cleaned weather data with field-weather mapping.
    process():
        Executes the full pipeline: load, clean, merge.
    """

    def __init__(self, config_params, logging_level=logging.INFO):
        """
        Initialize the WeatherDataProcessor with configuration parameters.

        Parameters
        ----------
        config_params : dict
            Configuration dictionary containing:
            - "weather_csv_path" : str
                URL to the weather station CSV file.
            - "weather_mapping_csv_path" : str
                URL to the field-to-weather mapping CSV file.
            - "regex_patterns" : dict
                Dictionary of regex patterns for extracting values.
        logging_level : int, optional
            Logging level (default is logging.INFO).

        Raises
        ------
        ValueError
            If any required configuration parameter is missing or None.
        """
        required_keys = ["weather_csv_path", "weather_mapping_csv_path", "regex_patterns"]
        missing_keys = [k for k in required_keys if k not in config_params or config_params[k] is None]

        if missing_keys:
            raise ValueError(
                f"Missing required configuration parameter(s): {', '.join(missing_keys)}. "
                f"Please check your config_params dictionary."
            )

        self.weather_csv_path = config_params["weather_csv_path"]
        self.weather_mapping_csv_path = config_params["weather_mapping_csv_path"]
        self.regex_patterns = config_params["regex_patterns"]

        # Logger setup
        self.logger = logging.getLogger(self.__class__.__name__)
        self._initialize_logging(logging_level)

        # Data placeholders
        self.weather_df = None
        self.mapping_df = None
        self.processed_df = None

        self.logger.info("✅ WeatherDataProcessor initialized successfully.")

    # -------------------------------------------------------------------
    # Logging Helper
    # -------------------------------------------------------------------
    def _initialize_logging(self, logging_level):
        """
        Configure logging for the WeatherDataProcessor instance.

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
            handler = logging.StreamHandler()
            handler.setLevel(logging_level)
            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            )
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    # -------------------------------------------------------------------
    # 1. Load Data
    # -------------------------------------------------------------------
    def load_data(self):
        """
        Load weather station data and field-weather mapping data from web URLs.

        This method reads two CSV files:
        - The weather station dataset containing raw sensor messages.
        - The mapping dataset linking field IDs to weather station IDs.

        Returns
        -------
        None

        Raises
        ------
        FileNotFoundError
            If the CSV file cannot be accessed at the given URL.
        pd.errors.ParserError
            If the CSV file is malformed and cannot be parsed.
        """
        self.logger.info("Loading weather station data...")
        self.weather_df = pd.read_csv(self.weather_csv_path)
        self.logger.info(f"Weather data loaded: {len(self.weather_df)} records.")

        self.logger.info("Loading weather station mapping data...")
        self.mapping_df = pd.read_csv(self.weather_mapping_csv_path)
        self.logger.info(f"Mapping data loaded: {len(self.mapping_df)} records.")

    # -------------------------------------------------------------------
    # 2. Clean Weather Data with Regex
    # -------------------------------------------------------------------
    def clean_weather_data(self):
        """
        Apply regex patterns to extract numeric values for rainfall, temperature,
        and pollution level from the raw 'Message' column.

        The method uses the regex patterns provided in the configuration to
        identify and extract numeric values. It creates new columns in the
        DataFrame for each measurement type.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If weather data has not been loaded before calling this method.
        KeyError
            If the 'Message' column is missing from the dataset.
        """
        if self.weather_df is None:
            raise ValueError("Weather data is not loaded. Run load_data() first.")

        if "Message" not in self.weather_df.columns:
            raise KeyError("The weather data does not contain a 'Message' column.")

        self.logger.info("Cleaning weather data using regex patterns...")

        def extract_value(pattern, text):
            match = re.search(pattern, str(text))
            if match:
                for i in range(1, (match.lastindex or 0) + 1):
                    group_val = match.group(i)
                    if group_val is not None:
                        try:
                            return float(group_val)
                        except ValueError:
                            continue
            return None

        for col_name, pattern in self.regex_patterns.items():
            self.weather_df[col_name] = self.weather_df["Message"].apply(
                lambda msg: extract_value(pattern, msg)
            )
            self.logger.info(f"Extracted values for: {col_name}")

    # -------------------------------------------------------------------
    # 3. Merge with Mapping Data
    # -------------------------------------------------------------------
    def merge_with_mapping(self):
        """
        Merge the cleaned weather data with the field-weather station mapping data.

        This method standardizes column names, ensures consistency in station IDs,
        and merges the weather dataset with the mapping dataset to produce a
        combined DataFrame.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If either the weather data or mapping data has not been loaded/cleaned.
        """
        if self.weather_df is None or self.mapping_df is None:
            raise ValueError("Data not loaded or cleaned. Run load_data() and clean_weather_data() first.")

        self.logger.info("Merging weather data with field-weather mapping...")

        # Standardize column names
        self.mapping_df.columns = self.mapping_df.columns.str.strip().str.replace(" ", "_")
        self.weather_df.columns = self.weather_df.columns.str.strip().str.replace(" ", "_")

        # Rename for consistency
        if "Weather_station_ID" in self.weather_df.columns:
            self.weather_df.rename(columns={"Weather_station_ID": "Weather_station"}, inplace=True)

        self.processed_df = self.mapping_df.merge(
            self.weather_df,
            on="Weather_station",
            how="left"
        )

        self.logger.info(f"Merged data successfully. Final shape: {self.processed_df.shape}")

    # -------------------------------------------------------------------
    # 4. Full Pipeline
    # -------------------------------------------------------------------
    def process(self):
        """
        Execute the full weather data processing pipeline.

        The pipeline consists of:
        1. Loading the weather and mapping datasets.
        2. Cleaning the weather data using regex patterns.
        3. Merging the cleaned weather data with the mapping dataset.

        Returns
        -------
        None

        Notes
        -----
        After running this method, the processed dataset is available in
        `self.processed_df`.
        """
        self.load_data()
        self.clean_weather_data()
        self.merge_with_mapping()
        self.logger.info("🌤 Weather data processing pipeline completed successfully.")
