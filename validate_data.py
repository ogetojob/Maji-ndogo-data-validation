import pandas as pd
import pytest

# Load the sampled CSVs
weather_csv_path = 'sampled_weather_df.csv'
field_csv_path = 'sampled_field_df.csv'

@pytest.fixture
def weather_df():
    return pd.read_csv(weather_csv_path)

@pytest.fixture
def field_df():
    return pd.read_csv(field_csv_path)

# -----------------------------
# Test 1: Weather DataFrame shape
# -----------------------------
def test_read_weather_DataFrame_shape(weather_df):
    assert weather_df.shape[0] > 0, "Weather DataFrame should have rows"
    assert weather_df.shape[1] > 0, "Weather DataFrame should have columns"

# -----------------------------
# Test 2: Field DataFrame shape
# -----------------------------
def test_read_field_DataFrame_shape(field_df):
    assert field_df.shape[0] > 0, "Field DataFrame should have rows"
    assert field_df.shape[1] > 0, "Field DataFrame should have columns"

# -----------------------------
# Test 3: Weather DataFrame columns
# -----------------------------
def test_weather_DataFrame_columns(weather_df):
    expected_columns = ['Field_ID', 'Weather_station', 'Message', 'Rainfall', 'Temperature', 'Pollution_level', 'Measurement']
    for col in expected_columns:
        assert col in weather_df.columns, f"Missing column in Weather DataFrame: {col}"

# -----------------------------
# Test 4: Field DataFrame columns
# -----------------------------
def test_field_DataFrame_columns(field_df):
    expected_columns = ['Field_ID', 'Latitude', 'Longitude', 'Crop_type', 'Annual_yield', 'Soil_type', 'Elevation', 'Farm_management_practices']
    for col in expected_columns:
        assert col in field_df.columns, f"Missing column in Field DataFrame: {col}"

# -----------------------------
# Test 5: Non-negative elevation values
# -----------------------------
def test_field_DataFrame_non_negative_elevation(field_df):
    assert (field_df['Elevation'] >= 0).all(), "All elevation values must be non-negative"

# -----------------------------
# Test 6: Crop types are valid
# -----------------------------
def test_crop_types_are_valid(field_df):
    valid_crops = ['cassava', 'wheat', 'tea', 'maize', 'sorghum', 'barley']
    invalid_crops = set(field_df['Crop_type'].unique()) - set(valid_crops)
    assert not invalid_crops, f"Invalid crop types found: {invalid_crops}"

# -----------------------------
# Test 7: Positive rainfall values
# -----------------------------
def test_positive_rainfall_values(weather_df):
    assert (weather_df['Rainfall'].dropna() >= 0).all(), "Rainfall values must be non-negative"

