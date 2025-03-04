import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus  # For URL encoding

# Initialize session state for data storage
if 'db_host' not in st.session_state:
    st.session_state.db_host = ""
if 'db_user' not in st.session_state:
    st.session_state.db_user = ""
if 'db_password' not in st.session_state:
    st.session_state.db_password = ""
if 'selected_db' not in st.session_state:
    st.session_state.selected_db = ""
if 'selected_table' not in st.session_state:
    st.session_state.selected_table = ""
if 'raw_data' not in st.session_state:
    st.session_state.raw_data = pd.DataFrame()
if 'cleaned_data' not in st.session_state:
    st.session_state.cleaned_data = pd.DataFrame()

# Helper Functions
def get_databases():
    try:
        connection = pymysql.connect(host=st.session_state.db_host, user=st.session_state.db_user, password=st.session_state.db_password)
        query = "SHOW DATABASES;"
        databases = pd.read_sql(query, connection)
        connection.close()
        return databases
    except Exception as e:
        st.error(f"Error fetching databases: {e}")
        return pd.DataFrame()

def get_tables(database_name):
    try:
        connection = pymysql.connect(host=st.session_state.db_host, user=st.session_state.db_user, password=st.session_state.db_password, database=database_name)
        query = "SHOW TABLES;"
        tables = pd.read_sql(query, connection)
        connection.close()
        return tables
    except Exception as e:
        st.error(f"Error fetching tables from database '{database_name}': {e}")
        return pd.DataFrame()

def extract_data(database_name, table_name):
    try:
        engine = create_engine(f'mysql+pymysql://{st.session_state.db_user}:{quote_plus(st.session_state.db_password)}@{st.session_state.db_host}/{database_name}')
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error extracting data from table '{table_name}': {e}")
        return pd.DataFrame()

def clean_data(df, cleaning_options):
    try:
        st.write("Raw Data Before Cleaning:")
        st.dataframe(df)

        for column, options in cleaning_options.items():
            if 'drop_duplicates' in options:
                df = df.drop_duplicates(subset=[column], keep='first')
                st.write(f"Dropped duplicates in column '{column}'.")
            if 'remove_nulls' in options:
                df = df[df[column].notnull()]
                st.write(f"Removed null values in column '{column}'.")
            if 'validate_string' in options:
                df = df[df[column].astype(str).str.strip() != '']
                st.write(f"Removed empty or whitespace-only strings in column '{column}'.")
            if 'validate_length' in options:
                df = df[df[column].astype(str).str.len() >= 3]
                st.write(f"Kept rows where length of column '{column}' is >= 3.")
            if 'validate_numeric' in options:
                df = df[pd.to_numeric(df[column], errors='coerce').notnull()]
                st.write(f"Kept rows where column '{column}' contains valid numeric values.")

        df['id'] = range(1, len(df) + 1)
        st.write("Cleaned Data After Processing:")
        st.dataframe(df)
        return df
    except Exception as e:
        st.error(f"Data cleaning failed: {e}")
        return pd.DataFrame()

def load_cleaned_data(cleaned_df, database_name):
    try:
        engine = create_engine(f'mysql+pymysql://{st.session_state.db_user}:{quote_plus(st.session_state.db_password)}@{st.session_state.db_host}/{database_name}')
        cleaned_df.to_sql('cleaned_employee_data', engine, schema='etl', if_exists='replace', index=False)
        st.success("Cleaned data successfully loaded into 'etl.cleaned_employee_data'!")
    except Exception as e:
        st.error(f"Data loading failed: {e}")

# Main App Layout
st.title("Database Interaction and ETL Process")

# Step 1: Server Connection
with st.expander("Step 1: Database Connection", expanded=True):
    st.session_state.db_host = st.text_input('Enter Database Host (e.g., localhost):', value=st.session_state.db_host)
    st.session_state.db_user = st.text_input('Enter Database User (e.g., root):', value=st.session_state.db_user)
    st.session_state.db_password = st.text_input('Enter Database Password:', type="password", value=st.session_state.db_password)

# Step 2: Data Extraction
with st.expander("Step 2: Data Extraction", expanded=False):
    databases = get_databases()
    if not databases.empty:
        db_names = databases.iloc[:, 0].tolist()
        st.session_state.selected_db = st.selectbox('Select Database:', db_names, index=db_names.index(st.session_state.selected_db) if st.session_state.selected_db in db_names else 0)
        tables = get_tables(st.session_state.selected_db)
        if not tables.empty:
            table_names = tables.iloc[:, 0].tolist()
            st.session_state.selected_table = st.selectbox('Select Table:', table_names, index=table_names.index(st.session_state.selected_table) if st.session_state.selected_table in table_names else 0)
            if st.button("Extract Data"):
                st.session_state.raw_data = extract_data(st.session_state.selected_db, st.session_state.selected_table)
                st.write("Extracted Data:")
                st.dataframe(st.session_state.raw_data)
        else:
            st.warning(f"No tables found in database '{st.session_state.selected_db}'.")
    else:
        st.warning("No databases found or could not connect to the database server.")

# Step 3: Data Cleaning
with st.expander("Step 3: Data Cleaning", expanded=False):
    if not st.session_state.raw_data.empty:
        st.write("Raw Data:")
        st.dataframe(st.session_state.raw_data)
        cleaning_options = {}
        for column in st.session_state.raw_data.columns:
            st.write(f"Cleaning options for column: **{column}**")
            options = st.multiselect(
                f"Select cleaning operations for column '{column}':",
                ['drop_duplicates', 'remove_nulls', 'validate_string', 'validate_length', 'validate_numeric']
            )
            cleaning_options[column] = options
        if st.button("Clean Data"):
            st.session_state.cleaned_data = clean_data(st.session_state.raw_data, cleaning_options)
    else:
        st.warning("No raw data available for cleaning.")

# Step 4: Data Loading
with st.expander("Step 4: Data Loading", expanded=False):
    if not st.session_state.cleaned_data.empty:
        st.write("Cleaned Data:")
        st.dataframe(st.session_state.cleaned_data)
        if st.button("Load Cleaned Data"):
            load_cleaned_data(st.session_state.cleaned_data, st.session_state.selected_db)
    else:
        st.warning("No cleaned data available for loading.")