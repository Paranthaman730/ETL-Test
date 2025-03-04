# Streamlit ETL Application

## Table of Contents

1. [GitHub Repo](#github-repo)
2. [Overview](#overview)
3. [Features](#features)
4. [Prerequisites](#prerequisites)
5. [Requirements File](#requirements-file)
6. [Setup Instructions](#setup-instructions)
   - [Step 1: Clone the Repository](#step-1-clone-the-repository)
   - [Step 2: Install Dependencies](#step-2-install-dependencies)
   - [Step 3: Run the Application](#step-3-run-the-application)
7. [Access the App](#access-the-app)
8. [Usage Guide](#usage-guide)
   - [Step 1: Database Connection](#step-1-database-connection)
   - [Step 2: Data Extraction](#step-2-data-extraction)
   - [Step 3: Data Cleaning](#step-3-data-cleaning)
   - [Step 4: Data Loading](#step-4-data-loading)
   - [Step 5: Completion](#step-5-completion)
9. [Customization](#customization)
10. [Troubleshooting](#troubleshooting)
11. [Contribution](#contribution)

## GitHub Repo

[Link to GitHub Repository](https://github.com/SHARAVANAKUMAR21/Test-ETL)

## Overview

This Streamlit-based ETL (Extract, Transform, Load) application allows users to connect to a MySQL database, extract data from a selected table, clean the data based on customizable options, and load the cleaned data back into a target database. The app is designed to be interactive, user-friendly, and visually appealing, making it easy for users to perform data processing tasks without writing code.

## Features

- **Database Connection**: Connect to a MySQL database by providing host, username, and password.
- **Data Extraction**: Select a database and table to extract raw data.
- **Data Cleaning**: Apply customizable cleaning operations (e.g., drop duplicates, remove nulls, validate strings/numbers) to the extracted data.
- **Data Loading**: Load the cleaned data into a target database, either by creating a new table or appending to an existing one.
- **Interactive Interface**: Use navigation buttons, progress bars, and expandable sections for a seamless user experience.
- **Error Handling**: Provides descriptive error messages and warnings for failed operations.

## Prerequisites

Before running the application, ensure you have the following installed:

- Python 3.8 or higher
- **Streamlit**: Install using `pip install streamlit`
- **Pandas**: Install using `pip install pandas`
- **PyMySQL**: Install using `pip install pymysql`
- **SQLAlchemy**: Install using `pip install sqlalchemy`

You can install all dependencies at once using the following command:

```bash
pip install -r requirements.txt
```

## Requirements File

```
streamlit
pandas
pymysql
sqlalchemy
```

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone https://github.com/SHARAVANAKUMAR21/Test-ETL.git
cd Test-ETL
```

### Step 2: Install Dependencies

```bash
pip install -r requirements.txt
```

### Step 3: Run the Application

Start the Streamlit app by running:

```bash
streamlit run app.py
```

## Access the App

Open your browser and navigate to the URL provided in the terminal (usually [http://localhost:8501](http://localhost:8501/)).

## Usage Guide

### Step 1: Database Connection

- Enter the database host (e.g., `localhost`), username, and password.
- Click **"Next"** to proceed to the data extraction step.

### Step 2: Data Extraction

- Select a database from the dropdown menu.
- Choose a table from the list of available tables.
- Click **"Extract Data"** to fetch the raw data from the selected table.

### Step 3: Data Cleaning

For each column in the raw data, select the desired cleaning operations:

- Drop Duplicates
- Remove Nulls
- Validate String (Remove Empty/Whitespace)
- Validate Length (Keep Rows with Length >= 3)
- Validate Numeric (Keep Rows with Valid Numbers)

Click **"Clean Data"** to apply the selected operations and view the cleaned data.

### Step 4: Data Loading

- Select the target database where the cleaned data will be loaded.
- Choose whether to create a new table or append to an existing one.
- Optionally, specify a schema for the target table.
- Click **"Load Cleaned Data"** to complete the process.

### Step 5: Completion

- A success message confirms that the data has been successfully processed and loaded.
- Click **"Restart Process"** to start over.

## Customization

- **Styling**: Modify the custom CSS in the `st.markdown` section of the code to change the appearance of buttons, dataframes, and other elements.
- **Cleaning Options**: Add or remove cleaning operations in the `clean_data` function to suit your specific needs.
- **Database Support**: Extend the app to support additional database systems by modifying the connection logic.

## Troubleshooting

- **Connection Errors**: Ensure the database credentials are correct and the server is accessible.
- **Empty Data**: Verify that the selected table contains data and that cleaning operations are not too restrictive.
- **Loading Failures**: Check the target database permissions and schema configuration.

## Contribution

Contributions are welcome! If you find any issues or have suggestions for improvement, please open an issue or submit a pull request.

[Link to Issues Page](https://github.com/SHARAVANAKUMAR21/Test-ETL/issues)

## Final Notes

This documentation provides a comprehensive guide to setting up, using, and customizing the Streamlit ETL application. If you encounter any issues or need further clarification, feel free to reach out via the GitHub repository.

