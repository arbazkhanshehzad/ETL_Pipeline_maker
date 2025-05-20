# 📊 ETL Pipeline Maker

> Transform your nested API responses into flat, clean CSVs & push to MySQL — with just a few clicks.

### Made with ❤️ by Arbaz Khan Shehzad

---

## 🚀 Overview

This app simplifies the extraction and transformation of deeply nested JSON from APIs into flat, analysis-ready CSV files. It allows you to:

- Normalize JSON data using a specific key
- Flatten nested dictionaries
- Explode lists of dictionaries
- Extract items from lists of strings
- Rename and select columns
- Export CSVs locally
- Upload the transformed data directly to a MySQL database
- Auto-generate a ready-to-use Python ETL pipeline

---

## 📦 Installation

Install the required libraries:

```bash
pip install streamlit==1.25 pandas requests sqlalchemy textwrap datetime
```

-----

## 📁 Project Structure

```
├── app.py              # Streamlit frontend
├── utils.py            # Data transformation & utility functions
└── outputs/            # Saved CSVs
```

-----

## ▶️ How to Run

From the project directory, start the Streamlit app:

```bash
streamlit run app.py
```

The app will open in your browser.

-----

## ⚙️ Features

### 🔹 Step-by-Step ETL Workflow

1.  **API Call**: Enter a JSON API endpoint.
2.  **Key-Based Normalization**: Provide the key (e.g., `data`) to normalize nested content.
3.  **Column Type Detection**:
      * Lists of dictionaries $\rightarrow$ explode
      * Lists of strings $\rightarrow$ extract specific indexes
      * Dictionaries $\rightarrow$ flatten to columns
4.  **Custom Transformations**: Select which columns to keep and rename columns.
5.  **Export Options**:
      * 💾 Download as CSV (`/outputs/result.csv`)
      * 🛢️ Push to MySQL
      * 🧪 Auto-generate a Python ETL pipeline script

-----

## 🧪 Sample Usage

After providing your API URL and the `data` key:

  * Select columns like `tags`, `author`, `comments`, etc.
  * Apply desired transformations
  * Download the CSV or push to a MySQL database
  * Optionally, generate a full `etl_pipeline_<timestamp>.py` script for automation

-----

## 🛠️ Under the Hood

**Main Functions from `utils.py`:**

  * `process_json_to_df`: Parses and flattens JSON using a normalization key.
  * `explode_list_of_dicts`: Explodes and flattens nested list-of-dictionary columns.
  * `extract_list_of_strings`: Extracts elements from list-of-string columns by index.
  * `flatten_single_dict`: Flattens dictionary columns into flat columns.
  * `push_to_mysql`: Uploads a DataFrame to a MySQL table.
  * `generate_etl_script`: Exports a full ETL Python script with all transformation steps.

-----

## 🐬 MySQL Export Requirements

To use the MySQL upload feature, ensure:

  * MySQL is running locally or on a remote host.
  * Correct credentials (host, port, username, password, database, and table) are provided.
  * `mysql-connector-python` is installed (optional: `pip install mysql-connector-python`).

-----

## 📥 Output Example

  * **CSV File**: `/outputs/result.csv`
  * **ETL Script**: `/outputs/etl_pipeline_<timestamp>.py`

ETL scripts can be reused or scheduled for recurring data pulls.

-----

## 📸 UI Preview
![alt text](image.png)
-----

## 🙌 Acknowledgments

Special thanks to the Python open-source ecosystem and the Streamlit community\!

-----

## 📫 Contact

**Arbaz Khan Shehzad**
📧 [arbazkhanshehzad@gmail.com](mailto:arbazkhanshehzad@gmail.com)
📍 Pakistan

-----
