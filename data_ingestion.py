"""
data_ingestion.py

This module handles data ingestion for the Maji Ndogo project.
It provides utility functions to:
- Connect to a SQLite database
- Run SQL queries and return data as DataFrames
- Load weather or mapping data from online CSV files

Author: Job Ogeto
Date: 2025-10-03
"""

import logging
import pandas as pd
from sqlalchemy import create_engine, text

# ----------------------------
# Logging Configuration
# ----------------------------
logger = logging.getLogger('data_ingestion')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


# ----------------------------
# Function Definitions
# ----------------------------

def create_db_engine(db_path: str):
    """
    Create a SQLAlchemy engine to connect to a SQLite database.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database. Must include the prefix 'sqlite:///' 
        followed by the database filename (e.g., 'sqlite:///Maji_Ndogo_farm_survey_small.db').

    Returns
    -------
    sqlalchemy.engine.Engine
        A SQLAlchemy Engine object for database connections.

    Raises
    ------
    ValueError
        If the db_path is not a valid SQLite connection string.

    Notes
    -----
    This function only creates the engine. Use `engine.connect()` or pass
    the engine to `query_data` to actually run queries.
    """
    logger.info(f"Creating database engine for {db_path}")
    engine = create_engine(db_path)
    return engine


def query_data(engine, query: str) -> pd.DataFrame:
    """
    Execute a SQL query and return the results as a pandas DataFrame.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database connection engine created with `create_db_engine`.
    query : str
        SQL query to execute.

    Returns
    -------
    pandas.DataFrame
        Query results as a DataFrame.

    Raises
    ------
    sqlalchemy.exc.DatabaseError
        If the query is invalid or the database cannot be accessed.

    Notes
    -----
    This function automatically opens and closes the connection.
    """
    logger.info("Executing SQL query...")
    with engine.connect() as connection:
        df = pd.read_sql_query(text(query), connection)
    logger.info(f"Query returned {len(df)} rows.")
    return df


def read_from_web_CSV(url: str) -> pd.DataFrame:
    """
    Read a CSV file directly from a web URL into a pandas DataFrame.

    Parameters
    ----------
    url : str
        URL pointing to the CSV file.

    Returns
    -------
    pandas.DataFrame
        DataFrame containing the CSV data.

    Raises
    ------
    ValueError
        If the URL is invalid or the file cannot be read.
    pd.errors.ParserError
        If the CSV file is malformed and cannot be parsed.

    Notes
    -----
    This function is useful for ingesting mapping or weather data that is
    hosted online and updated regularly.
    """
    logger.info(f"Reading CSV data from {url}")
    df = pd.read_csv(url)
    logger.info(f"Loaded {len(df)} records from web CSV.")
    return df
