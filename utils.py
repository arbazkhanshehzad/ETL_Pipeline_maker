import textwrap
from datetime import datetime
from sqlalchemy import create_engine
import pandas as pd

def preprocess_entry(entry):
    """
    Sanitize each entry recursively:
    - Replace empty lists with None
    - Replace None with {}
    - Ensure consistent structure
    """
    if isinstance(entry, list):
        if not entry:
            return None
        return [preprocess_entry(i) for i in entry]

    elif isinstance(entry, dict):
        return {k: preprocess_entry(v) for k, v in entry.items()}

    elif entry is None:
        return None

    return entry

def process_json_to_df(json_data, key_to_normalize):
    raw = json_data.get(key_to_normalize, [])

    # Preprocess each item
    sanitized_data = [preprocess_entry(entry) for entry in raw]

    try:
        df = pd.json_normalize(sanitized_data, sep='.', errors='ignore')
    except Exception as e:
        raise ValueError(f"json_normalize failed: {e}")

    return df, sanitized_data

def find_nested_columns(df):
    list_of_dict_cols = []
    single_dict_cols = []
    list_of_str_cols = []

    for col in df.columns:
        sample = df[col].dropna().head(1)
        if not sample.empty:
            val = sample.values[0]
            if isinstance(val, list):
                if all(isinstance(i, dict) for i in val if i is not None):
                    list_of_dict_cols.append(col)
                elif all(isinstance(i, str) for i in val if i is not None):
                    list_of_str_cols.append(col)
            elif isinstance(val, dict):
                single_dict_cols.append(col)

    return list_of_dict_cols, single_dict_cols, list_of_str_cols

def explode_list_of_dicts(df, col, id_col):
    df = df.explode(col)
    nested_df = pd.json_normalize(df[col])
    nested_df.index = df.index
    # Add suffix to prevent overlaps
    nested_df.columns = [f"{col}.{subcol}" for subcol in nested_df.columns]
    df = df.drop(columns=[col]).reset_index(drop=True).join(nested_df)
    return df

def flatten_single_dict(df, col):
    nested_df = pd.json_normalize(df[col])
    # Add suffix to prevent overlaps
    nested_df.columns = [f"{col}.{subcol}" for subcol in nested_df.columns]
    df = df.drop(columns=[col]).reset_index(drop=True).join(nested_df)
    return df


def extract_list_of_strings(df, col, indexes_to_keep):
    df[col] = df[col].apply(lambda lst: [lst[i] if isinstance(lst, list) and i < len(lst) else None for i in indexes_to_keep])
    extracted_cols = pd.DataFrame(df[col].tolist(), columns=[f"{col}_{i}" for i in indexes_to_keep])
    df = df.drop(columns=[col]).join(extracted_cols)
    return df



def push_to_mysql(df, host, port, user, password, database, table_name):
    try:
        conn_str = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(conn_str)
        df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')
        return True, f"✅ Successfully pushed to `{table_name}` in `{database}`"
    except Exception as e:
        return False, f"❌ Failed to push to MySQL: {e}"



def generate_etl_script(df, api_url, json_key, mysql_info, etl_steps):
    from datetime import datetime
    import textwrap

    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"etl_pipeline_{now}.py"

    # Format all transformation steps into script lines
    transformations = "\n".join(etl_steps)

    content = f"""
import requests
import pandas as pd
from sqlalchemy import create_engine

# Step 1: Fetch Data
url = "{api_url}"
response = requests.get(url)
data = response.json()

# Step 2: Normalize JSON with preprocessing
def preprocess_entry(entry):
    if isinstance(entry, list):
        if not entry:
            return None
        return [preprocess_entry(i) for i in entry]
    elif isinstance(entry, dict):
        return {{k: preprocess_entry(v) for k, v in entry.items()}}
    elif entry is None:
        return None
    return entry

raw = data["{json_key}"]
sanitized_data = [preprocess_entry(entry) for entry in raw]
df = pd.json_normalize(sanitized_data)

# Step 3: Clean / Transform
{transformations}

# Step 4: Save to CSV
df.to_csv("final_output.csv", index=False)

# Step 5: Push to MySQL
engine = create_engine("mysql+mysqlconnector://{mysql_info['user']}:{mysql_info['password']}@{mysql_info['host']}:{mysql_info['port']}/{mysql_info['database']}")
df.to_sql("{mysql_info['table']}", engine, index=False, if_exists="replace")

print("✅ ETL complete! Data saved to MySQL and CSV.")
    """

    # Remove indentation
    content = textwrap.dedent(content)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(content)

    return filename

