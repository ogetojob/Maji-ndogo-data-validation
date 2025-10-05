# 🌾 Maji Ndogo Data Validation

![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Tests](https://img.shields.io/badge/tests-pytest-passing-brightgreen)

A data validation and hypothesis testing pipeline for agricultural and weather data from **Maji Ndogo**.  
This project ensures data consistency between **field surveys** and **weather station datasets**, and statistically verifies if the field data represents real environmental conditions.

---

## 📌 Overview

The goal of this project is to:
- ✅ Clean and process field and weather data from a SQLite database and remote CSV files.
- 🧪 Validate the processed datasets using automated `pytest` scripts.
- 📊 Perform statistical hypothesis testing to check for significant differences between field and weather station measurements (Temperature, Rainfall, Pollution).

This project is part of the **Integrated Data Validation Module** and demonstrates modular coding, data cleaning, and statistical reasoning.

---

## 🛠️ Tech Stack

- **Language:** Python 3.10+
- **Database:** SQLite
- **Libraries:**  
  - `pandas` — data processing  
  - `numpy` — numerical computation  
  - `pytest` — data validation tests  
  - `scipy` — hypothesis testing  
- **Version Control:** Git + GitHub

---

## 📂 Project Structure



Maji-ndogo-data-validation/
│
├── field_data_processor.py # Cleans and processes field data
├── weather_data_processor.py # Cleans and processes weather station data
├── validate_data.py # Pytest-based validation script
├── Integrated_project_P3_Validating_our_data_student.ipynb # Main notebook
├── README.md # Project documentation
└── requirements.txt # Python dependencies


---

## ⚡ Setup Instructions

1. **Clone the repository**  
   ```bash
   git clone https://github.com/ogetojob/Maji-ndogo-data-validation.git
   cd Maji-ndogo-data-validation


Create and activate a virtual environment

python -m venv venv
source venv/bin/activate       # macOS/Linux
venv\Scripts\activate          # Windows


Install dependencies

pip install -r requirements.txt


Run the Jupyter Notebook

jupyter notebook

✅ Data Validation

After processing, the datasets are saved as CSV files and validated with pytest.
Run the tests with:

pytest validate_data.py -v


Expected test output:

============================ test session starts =============================
platform win32 -- Python 3.12.1, pytest-8.0.0, pluggy-1.4.0 -- ...
cachedir: .pytest_cache
rootdir: ...
collecting ... collected 7 items

validate_data.py::test_read_weather_DataFrame_shape PASSED               [ 14%]
validate_data.py::test_read_field_DataFrame_shape PASSED                 [ 28%]
validate_data.py::test_weather_DataFrame_columns PASSED                  [ 42%]
validate_data.py::test_field_DataFrame_columns PASSED                    [ 57%]
validate_data.py::test_field_DataFrame_non_negative_elevation PASSED     [ 71%]
validate_data.py::test_crop_types_are_valid PASSED                       [ 85%]
validate_data.py::test_positive_rainfall_values PASSED                   [100%]

📊 Hypothesis Testing

We perform a two-tailed t-test using scipy.stats.ttest_ind to compare mean values between field data and weather station data for:

🌡 Temperature

🌧 Rainfall

🏭 Pollution levels

Null Hypothesis (H₀): μ_field = μ_weather

Alternative Hypothesis (H₁): μ_field ≠ μ_weather

If p ≤ 0.05, we reject H₀ (significant difference).
Otherwise, we do not reject H₀.

Sample output:

No significant difference in Temperature detected at Station 0, (P-Value: 0.90761 > 0.05). Null hypothesis not rejected.
No significant difference in Rainfall detected at Station 0, (P-Value: 0.21621 > 0.05). Null hypothesis not rejected.
No significant difference in Pollution_level detected at Station 0, (P-Value: 0.56418 > 0.05). Null hypothesis not rejected.
...

👨‍💻 Author

Job Ogeto
📧 ogetojob@gmail.com

🌐 GitHub Profile

📝 License

This project is licensed under the MIT License — feel free to use and modify it.

⭐ Acknowledgements

This project was developed as part of the ALX Data Science Program.
