import streamlit as st
import pandas as pd
import requests
from utils import (
    process_json_to_df,
    find_nested_columns,
    explode_list_of_dicts,
    flatten_single_dict,
    extract_list_of_strings,
    generate_etl_script,
    push_to_mysql
)
st.title("📊 ETL Pipeline Maker")

# Step 1: User provides API URL
url = st.text_input("Enter the API URL")
etl_steps = []
if url:
    try:
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        st.success("API call successful.")

        # Step 2: Show preview
        st.subheader("📥 Raw JSON Response")
        st.json(json_data, expanded=False)

        # Step 3: Select normalization key
        normalize_key = st.text_input("Enter the key to normalize (e.g., `data`)")

        if normalize_key and normalize_key in json_data:
            df, raw = process_json_to_df(json_data, normalize_key)
            # Detect nested columns
            list_of_dict_cols, single_dict_cols, list_of_str_cols = find_nested_columns(df)

            st.subheader("🔧 Select Transformations")

            # Selections first
            cols_to_explode = st.multiselect("Select list-of-dict columns to explode", options=list_of_dict_cols)
            cols_to_extract = st.multiselect("Select list-of-string columns to extract from", options=list_of_str_cols)
            cols_to_flatten = st.multiselect("Select single-dict columns to flatten", options=single_dict_cols)

            # Select ID column only if explode is used
            id_col = None
            if cols_to_explode:
                id_col = st.selectbox("Select unique ID column for merge", df.columns.tolist(), index=df.columns.get_loc("_id") if "_id" in df.columns else 0)

            # Button to trigger transformation
            if st.button("🚀 Start Transformation"):
                # Explode list-of-dict columns
                for col in cols_to_explode:
                    etl_steps.append(f"""
            df = df.explode("{col}")
            nested = pd.json_normalize(df["{col}"])
            nested.columns = [f"{col}." + sub for sub in nested.columns]
            df = df.drop(columns=["{col}"]).reset_index(drop=True).join(nested)
            """)
                    try:
                        df = explode_list_of_dicts(df, col, id_col)
                        st.success(f"Exploded and merged column: {col}")
                    except Exception as e:
                        st.warning(f"Failed to explode {col}: {e}")

                # Extract from list-of-strings
                for col in cols_to_extract:
                    sample_list = df[col].dropna().head(1).values[0] if not df[col].dropna().empty else []
                    st.markdown(f"**{col}** sample: `{sample_list}`")
                    indexes = st.text_input(f"Enter indexes to keep for `{col}` (comma-separated)", "0", key=f"{col}_indexes")
                    try:
                        index_list = [int(i.strip()) for i in indexes.split(",") if i.strip().isdigit()]
                        df = extract_list_of_strings(df, col, index_list)
                        etl_steps.extend([
                            f'df["{col}_{idx}"] = df["{col}"].apply(lambda x: x[{idx}] if isinstance(x, list) and len(x) > {idx} else None)'
                            for idx in index_list
                        ])
                        etl_steps.append(f'df = df.drop(columns=["{col}"])')
                        st.success(f"Extracted indexes from {col}")
                    except Exception as e:
                        st.warning(f"Could not extract from {col}: {e}")

                # Flatten single dictionaries
                for col in cols_to_flatten:
                    try:
                        df = flatten_single_dict(df, col)
                        etl_steps.append(f"""
            nested = pd.json_normalize(df["{col}"])
            nested.columns = [f"{col}." + sub for sub in nested.columns]
            df = df.drop(columns=["{col}"]).reset_index(drop=True).join(nested)
            """)
                        st.success(f"Flattened column: {col}")
                    except Exception as e:
                        st.warning(f"Failed to flatten {col}: {e}")

            # ✅ Final column selection & renaming
            st.subheader("🧹 Final Cleanup")
            selected_cols = st.multiselect("Select columns to keep", df.columns.tolist(), default=df.columns.tolist())
            df = df[selected_cols]
            etl_steps.append(f'df = df[{repr(selected_cols)}]')

            new_names = {}
            st.markdown("Rename Columns:")
            for col in selected_cols:
                new_name = st.text_input(f"Rename `{col}`", value=col)
                new_names[col] = new_name
            df.rename(columns=new_names, inplace=True)
            etl_steps.append(f'df = df.rename(columns={new_names})')

            # 💾 Save
            if st.button("💾 Save Final CSV"):
                output_path = "outputs/result.csv"
                df.to_csv(output_path, index=False)
                st.success(f"Saved final DataFrame to {output_path}")

                st.subheader("🛠️ Save to MySQL Database")

            with st.form("mysql_form"):
                host = st.text_input("MySQL Host", value="localhost")
                port = st.text_input("Port", value="3306")
                user = st.text_input("Username", value="root")
                password = st.text_input("Password", type="password")
                database = st.text_input("Database Name")
                table_name = st.text_input("Table Name", value="my_table")

                submit_btn = st.form_submit_button("Upload to MySQL")

                if submit_btn:
                    if not database or not table_name:
                        st.warning("Please fill in all fields.")
                    else:
                        success, msg = push_to_mysql(df, host, port, user, password, database, table_name)
                        if success:
                            st.success(msg)
                        else:
                            st.error(msg)
            st.subheader("⚙️ Generate Python ETL Script")

            if st.button("Generate ETL Script"):
                mysql_info = {
                    "host": host,
                    "port": port,
                    "user": user,
                    "password": password,
                    "database": database,
                    "table": table_name
                }
                script_path = generate_etl_script(df, url, normalize_key, mysql_info, etl_steps)
                
                with open(script_path, "rb") as f:
                    st.download_button(
                        label="📥 Download ETL Script",
                        data=f,
                        file_name=script_path,
                        mime="text/x-python"
                    )
                st.success(f"Script saved to {script_path}")
                st.code(open(script_path).read(), language="python")


    except Exception as e:
        st.error(f"Error fetching API: {e}")
