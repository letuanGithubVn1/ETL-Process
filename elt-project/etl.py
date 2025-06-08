import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import requests
import os


# ========================
# Extract data
# ========================

def download_google_sheet(sheet_id: str) -> pd.DataFrame:
    """
    Download and load a specific sheet named 'enrollies' from a public Google Sheet.

    Args:
        sheet_id (str): The unique ID of the Google Sheet (from its URL).

    Returns:
        pd.DataFrame: A DataFrame containing the contents of the 'enrollies' sheet.

    Notes:
        - The sheet must be publicly accessible.
        - This function specifically reads the sheet named 'enrollies'.
    """
    url = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=xlsx'
    df = pd.read_excel(url, sheet_name='enrollies')
    print("Google Sheet downloaded and loaded.")
    return df

def download_and_read_excel(excel_url: str, save_path: str = "data/enrollies_education.xlsx") -> pd.DataFrame:
    """
    Download an Excel file from a URL and load it into a pandas DataFrame.

    Args:
        excel_url (str): The direct URL to the Excel (.xlsx) file.
        save_path (str): Local path to temporarily save the Excel file.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
   
    # Tạo thư mục nếu chưa có
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Tải file Excel từ internet
    response = requests.get(excel_url)
    response.raise_for_status()

    # Ghi file ra ổ đĩa
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"Excel file downloaded and saved to {save_path}")

    # Đọc file Excel thành DataFrame
    df_excel = pd.read_excel(save_path)
    print(f"Excel file loaded into DataFrame.")
    return df_excel

def download_and_read_csv(csv_url: str, save_path: str = "data/work_experience.csv") -> pd.DataFrame:
    """
    Download a CSV file from a URL and load it into a pandas DataFrame.

    Args:
        csv_url (str): The direct URL to the CSV file.
        save_path (str): Local path to temporarily save the CSV file.

    Returns:
        pd.DataFrame: The loaded DataFrame.
    """
    # Tạo thư mục nếu chưa có
    os.makedirs(os.path.dirname(save_path), exist_ok=True)

    # Tải file từ internet
    response = requests.get(csv_url)
    response.raise_for_status()

    # Ghi file ra ổ đĩa
    with open(save_path, 'wb') as f:
        f.write(response.content)
    print(f"CSV file downloaded and saved to {save_path}")

    df_csv = pd.read_csv(save_path)
    print(f"CSV file loaded into DataFrame.")
    return df_csv


def load_mysql_table(table_name: str, user: str, password: str, host: str, port: int, db: str) -> pd.DataFrame:
    """
    Connect to a MySQL database and load a specific table into a DataFrame.

    Args:
        table_name (str): Name of the table to load.
        user (str): MySQL username.
        password (str): MySQL password.
        host (str): Host address of the MySQL server.
        port (int): Port number of the MySQL server (e.g., 3306).
        db (str): Name of the database.

    Returns:
        pd.DataFrame: A DataFrame containing the data from the specified MySQL table.

    Requirements:
        - Requires `sqlalchemy` and `pymysql` libraries.
    """
    from sqlalchemy import create_engine
    import pymysql

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    df = pd.read_sql_table(table_name, con=engine)
    print(f"MySQL table loaded: {table_name}")
    return df

def load_html_table(url: str) -> pd.DataFrame:
    """
    Load the first HTML table from a webpage into a DataFrame.

    Args:
        url (str): The URL of the webpage containing HTML tables.

    Returns:
        pd.DataFrame: The first HTML table found on the page.
    """

    tables = pd.read_html(url)
    print(f"HTML table loaded from {url}")
    return tables[0]

# ========================
# Transform data
# ========================

def transform_enrollies_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fills missing gender values and converts specified columns to categorical
    data types for memory optimization.

    Args:
        df (pd.DataFrame): The enrollies data.

    Returns:
        pd.DataFrame: The transformed enrollies data.
    """
    df['gender'] = df['gender'].fillna('Other')
    df[['gender', 'city']] = df[['gender', 'city']].astype('category')
    print("Transformed: enrollies_data")
    return df

def transform_education_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles missing values and converts specific columns to categorical data
    types for better efficiency.

    Args:
        df (pd.DataFrame): The education data of enrollies.

    Returns:
        pd.DataFrame: The transformed education data.
    """
    df['education_level'] = df['education_level'].astype('string')
    df.fillna({'enrolled_university': 'no_enrollment'}, inplace=True)
    df.fillna({'major_discipline': 'Other'}, inplace=True)
    df.fillna({'education_level': df['education_level'].mode().iloc[0]}, inplace=True)
    
    df[['enrolled_university', 'education_level', 'major_discipline']] = df[[
        'enrolled_university', 'education_level', 'major_discipline']].astype('category')
    
    print("Transformed: enrollies_education")
    return df


def transform_work_experience_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes specific fields, fills in missing values, and optimizes data types.

    Args:
        df (pd.DataFrame): The work experience data of enrollies.

    Returns:
        pd.DataFrame: The transformed work experience data.
    """
    df = df.convert_dtypes()
    
    df['experience'] = df['experience'].replace({
        '>20': '21', 
        '<1': '0'
    })
    
    df['last_new_job'] = df['last_new_job'].replace({
        '>4': '4+',
    })
    
    df['company_size'] = df['company_size'].replace({
        '<10': '0-9',
        '10/49': '10-49',
    })
    
    df.fillna({'experience': df['experience'].mode().iloc[0]}, inplace=True)
    df.fillna({'company_size': df['company_size'].mode().iloc[0]}, inplace=True)
    df.fillna({'last_new_job': 'never'}, inplace=True)
    df.fillna({'company_type': df['company_size'].mode().iloc[0]}, inplace=True)
    
    print("Transformed: work_experience")
    return df

# ========================
# Load data
# ========================


def save_df_to_sqlite(df: pd.DataFrame, table_name: str, db_path: str = "data/data_warehouse.db"):
    """
    Save a single DataFrame to a specified table in a SQLite database.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        table_name (str): The name of the table in the database.
        db_path (str): Path to the SQLite database file (default: 'data/data_warehouse.db').

    Returns:
        None
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    engine = create_engine(f'sqlite:///{db_path}')
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Saved to table: {table_name}")


# ========================
# Main ETL Pipeline
# ========================

def main():
    os.makedirs("data", exist_ok=True)

    # Extract
    enrollies_df = download_google_sheet('1VCkHwBjJGRJ21asd9pxW4_0z2PWuKhbLR3gUHm-p4GI')
    education_df = download_and_read_excel('https://assets.swisscoding.edu.vn/company_course/enrollies_education.xlsx')
    work_df = download_and_read_csv('https://assets.swisscoding.edu.vn/company_course/work_experience.csv')
    city_df = load_html_table('https://sca-programming-school.github.io/city_development_index/index.html')
    training_df = load_mysql_table('training_hours', 'etl_practice', '550814', '112.213.86.31', 3360, 'company_course')
    employment_df = load_mysql_table('employment', 'etl_practice', '550814', '112.213.86.31', 3360, 'company_course')

    # Transform
    enrollies_df = transform_enrollies_data(enrollies_df)
    education_df = transform_education_data(education_df)
    work_df = transform_work_experience_data(work_df)

    # Load
    save_df_to_sqlite(enrollies_df, 'Dim_Enrollies')
    save_df_to_sqlite(education_df, 'Dim_Enrollies_Education')
    save_df_to_sqlite(work_df, 'Dim_Work_Experience')
    save_df_to_sqlite(city_df, 'Dim_Training_Hours')
    save_df_to_sqlite(training_df, 'Dim_Cities')
    save_df_to_sqlite(employment_df, 'Fact_Employment')

    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
