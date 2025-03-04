import streamlit as st
import pandas as pd
import pymysql
from sqlalchemy import create_engine
from urllib.parse import quote_plus  # For URL encoding

# Custom CSS for styling
st.markdown("""
<style>
    .stButton > button {
        width: 100%;
        border-radius: 5px;
        height: 50px;
        font-size: 18px;
    }
    .sidebar .sidebar-content {
        background-color: #f4f6f9;
    }
    .stDataFrame {
        border: 1px solid #ddd;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state for navigation and data storage
if 'page' not in st.session_state:
    st.session_state.page = "server_connection"

fields = ['db_host', 'db_user', 'db_password', 'selected_db', 'selected_table', 'raw_data', 'cleaned_data']
for field in fields:
    if field not in st.session_state:
        st.session_state[field] = "" if field != 'raw_data' and field != 'cleaned_data' else pd.DataFrame()

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

        # Normalize column names to avoid case sensitivity or whitespace issues
        df.columns = df.columns.str.strip().str.lower()

        progress_bar = st.progress(0)
        total_steps = len(cleaning_options)
        current_step = 0

        for column, options in cleaning_options.items():
            if column not in df.columns:
                st.warning(f"Column '{column}' not found in the data. Skipping cleaning for this column.")
                continue

            if 'drop_duplicates' in options:
                st.write(f"Dropping duplicates in column '{column}'.")
                df = df.drop_duplicates(subset=[column], keep='first')

            if 'remove_nulls' in options:
                st.write(f"Removing null values in column '{column}'.")
                df = df[df[column].notnull()]

            if 'validate_string' in options:
                st.write(f"Removing empty or whitespace-only strings in column '{column}'.")
                df = df[df[column].astype(str).str.strip() != '']

            if 'validate_length' in options:
                st.write(f"Keeping rows where length of column '{column}' is >= 3.")
                df = df[df[column].astype(str).str.len() >= 3]

            if 'validate_numeric' in options:
                st.write(f"Keeping rows where column '{column}' contains valid numeric values.")
                df = df[pd.to_numeric(df[column], errors='coerce').notnull()]

            current_step += 1
            progress_bar.progress(current_step / total_steps)

        if df.empty:
            st.warning("All rows were removed during cleaning. Please review your cleaning options.")
            return pd.DataFrame()

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

# Navigation Buttons
def navigation_buttons(current_page, next_page=None, prev_page=None):
    col1, col2 = st.columns([1, 1])
    with col1:
        if prev_page and st.button("Previous", key=f"prev_{current_page}"):
            st.session_state.page = prev_page
    with col2:
        if next_page and st.button("Next", key=f"next_{current_page}"):
            st.session_state.page = next_page

# Sidebar Navigation
def sidebar_navigation():
    st.sidebar.title("Navigation")
    pages = ["Server Connection", "Data Extraction", "Data Cleaning", "Data Loading", "Completion"]
    current_index = pages.index(st.session_state.page.replace("_", " ").title())
    selected_page = st.sidebar.selectbox("Go to Step:", pages, index=current_index)
    st.session_state.page = selected_page.lower().replace(" ", "_")

# Page: Server Connection
def server_connection_page():
    st.title("Step 1: Database Connection")
    st.session_state.db_host = st.text_input('Enter Database Host (e.g., localhost):', value=st.session_state.db_host)
    st.session_state.db_user = st.text_input('Enter Database User (e.g., root):', value=st.session_state.db_user)
    st.session_state.db_password = st.text_input('Enter Database Password:', type="password", value=st.session_state.db_password)
    navigation_buttons("server_connection", next_page="data_extraction")

# Page: Data Extraction
def data_extraction_page():
    st.title("Step 2: Data Extraction")
    databases = get_databases()
    if not databases.empty:
        db_names = databases.iloc[:, 0].tolist()
        st.session_state.selected_db = st.selectbox('Select Database:', db_names, index=db_names.index(st.session_state.selected_db) if st.session_state.selected_db in db_names else 0)
        tables = get_tables(st.session_state.selected_db)
        if not tables.empty:
            table_names = tables.iloc[:, 0].tolist()
            st.session_state.selected_table = st.selectbox('Select Table:', table_names, index=table_names.index(st.session_state.selected_table) if st.session_state.selected_table in table_names else 0)
            if st.button("Extract Data"):
                with st.spinner("Extracting data..."):
                    st.session_state.raw_data = extract_data(st.session_state.selected_db, st.session_state.selected_table)
                st.write("Extracted Data:")
                st.dataframe(st.session_state.raw_data)
                st.session_state.page = "data_cleaning"
        else:
            st.warning(f"No tables found in database '{st.session_state.selected_db}'.")
    else:
        st.warning("No databases found or could not connect to the database server.")
    navigation_buttons("data_extraction", next_page="data_cleaning", prev_page="server_connection")

# Page: Data Cleaning
def data_cleaning_page():
    st.title("Step 3: Data Cleaning")
    if not st.session_state.raw_data.empty:
        st.write("Raw Data:")
        st.dataframe(st.session_state.raw_data)
        cleaning_options = {}

        for column in st.session_state.raw_data.columns:
            with st.expander(f"Cleaning Options for Column: **{column}**"):
                options = st.multiselect(
                    f"Select cleaning operations for column '{column}':",
                    [
                        "Drop Duplicates",
                        "Remove Nulls",
                        "Validate String (Remove Empty/Whitespace)",
                        "Validate Length (Keep Rows with Length >= 3)",
                        "Validate Numeric (Keep Rows with Valid Numbers)"
                    ],
                    key=f"cleaning_{column}"
                )
                cleaning_options[column] = []
                if "Drop Duplicates" in options:
                    cleaning_options[column].append("drop_duplicates")
                if "Remove Nulls" in options:
                    cleaning_options[column].append("remove_nulls")
                if "Validate String (Remove Empty/Whitespace)" in options:
                    cleaning_options[column].append("validate_string")
                if "Validate Length (Keep Rows with Length >= 3)" in options:
                    cleaning_options[column].append("validate_length")
                if "Validate Numeric (Keep Rows with Valid Numbers)" in options:
                    cleaning_options[column].append("validate_numeric")

        if st.button("Clean Data"):
            with st.spinner("Cleaning data..."):
                st.session_state.cleaned_data = clean_data(st.session_state.raw_data, cleaning_options)
            st.session_state.page = "data_loading"
    else:
        st.warning("No raw data available for cleaning.")
    navigation_buttons("data_cleaning", next_page="data_loading", prev_page="data_extraction")

# Page: Data Loading
def data_loading_page():
    st.title("Step 4: Data Loading")
    if not st.session_state.cleaned_data.empty:
        st.write("Cleaned Data:")
        st.dataframe(st.session_state.cleaned_data)

        databases = get_databases()
        if not databases.empty:
            db_names = databases.iloc[:, 0].tolist()
            target_db = st.selectbox('Select Target Database:', db_names, index=db_names.index(st.session_state.selected_db) if st.session_state.selected_db in db_names else 0)
            load_option = st.radio("Choose Load Option:", ["Create New Table", "Append to Existing Table"])

            if load_option == "Create New Table":
                target_table = st.text_input("Enter New Table Name:", value="new_table")
                if_exists_option = 'replace'
            else:
                tables = get_tables(target_db)
                if not tables.empty:
                    table_names = tables.iloc[:, 0].tolist()
                    target_table = st.selectbox('Select Existing Table:', table_names)
                    if_exists_option = 'append'
                else:
                    st.warning(f"No tables found in database '{target_db}'. Please create a new table.")
                    return

            target_schema = st.text_input("Enter Target Schema (Optional):", value="etl")

            if st.button("Load Cleaned Data"):
                with st.spinner("Loading data..."):
                    try:
                        engine = create_engine(f'mysql+pymysql://{st.session_state.db_user}:{quote_plus(st.session_state.db_password)}@{st.session_state.db_host}/{target_db}')
                        cleaned_df = st.session_state.cleaned_data
                        cleaned_df.to_sql(
                            target_table,
                            engine,
                            schema=target_schema if target_schema else None,
                            if_exists=if_exists_option,
                            index=False
                        )
                        st.success(f"Cleaned data successfully loaded into '{target_db}.{target_schema}.{target_table}'!")
                        st.session_state.page = "completion"
                    except Exception as e:
                        st.error(f"Data loading failed: {e}")
        else:
            st.warning("No databases found or could not connect to the database server.")
    else:
        st.warning("No cleaned data available for loading.")
    navigation_buttons("data_loading", prev_page="data_cleaning")

# Page: Completion
def completion_page():
    st.title("Process Completed!")
    st.success("Data has been successfully extracted, cleaned, and loaded into the database.")
    if st.button("Restart Process"):
        st.session_state.page = "server_connection"

# Main App Logic
def main():
    sidebar_navigation()
    if st.session_state.page == "server_connection":
        server_connection_page()
    elif st.session_state.page == "data_extraction":
        data_extraction_page()
    elif st.session_state.page == "data_cleaning":
        data_cleaning_page()
    elif st.session_state.page == "data_loading":
        data_loading_page()
    elif st.session_state.page == "completion":
        completion_page()

# Run the app
if __name__ == "__main__":
    main()