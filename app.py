import os
import requests
import pandas as pd
from sqlalchemy import create_engine, exc
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import streamlit as st

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for configuration
API_URL = os.getenv("DATAUSA_API_URL", "https://datausa.io/api/data?...")  # Default or override via env
DB_URL = os.getenv("DATABASE_URL", "sqlite:///health_prosperity_index.db")

def fetch_data(api_url=API_URL):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # Raises an error for 4XX/5XX responses
        data = response.json()
        df = pd.DataFrame(data['data'])
        
        # Validate required columns are present
        required_columns = [
            'life_expectancy', 
            'median_household_income', 
            'unemployment_rate', 
            'obesity_rate', 
            'poverty_rate', 
            'access_to_healthcare'
        ]
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Missing required columns in API response: {required_columns}")
            return pd.DataFrame()

        if df.empty:
            logging.warning("No data received from API.")
        return df
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on failure

def normalize_column(df, column_name):
    """Normalize the column by min-max scaling."""
    min_val = df[column_name].min()
    max_val = df[column_name].max()
    df[column_name + '_norm'] = (df[column_name] - min_val) / (max_val - min_val)
    return df

def calculate_index(df):
    try:
        # Normalize variables
        df = normalize_column(df, 'life_expectancy')
        df = normalize_column(df, 'median_household_income')
        df = normalize_column(df, 'unemployment_rate')
        df = normalize_column(df, 'obesity_rate')
        df = normalize_column(df, 'poverty_rate')
        df = normalize_column(df, 'access_to_healthcare')
        
        # Ensure columns exist before proceeding
        required_columns = [
            'life_expectancy_norm', 
            'median_household_income_norm', 
            'unemployment_rate_norm', 
            'obesity_rate_norm', 
            'poverty_rate_norm', 
            'access_to_healthcare_norm'
        ]
        if not all(col in df.columns for col in required_columns):
            logging.error(f"Missing required columns in data: {required_columns}")
            return pd.DataFrame()

        # Calculate the index
        df['index'] = (
            df['life_expectancy_norm'] * 0.2 +
            df['median_household_income_norm'] * 0.2 +
            df['unemployment_rate_norm'] * 0.2 +
            df['obesity_rate_norm'] * 0.15 +
            df['poverty_rate_norm'] * 0.05 +
            df['access_to_healthcare_norm'] * 0.2
        )
        return df
    except Exception as e:
        logging.error(f"Error calculating index: {e}")
        return pd.DataFrame()

def update_data():
    df = fetch_data()
    if not df.empty:
        df = calculate_index(df)
        if not df.empty:
            try:
                engine = create_engine(DB_URL)
                df.to_sql('index_data', engine, if_exists='replace', index=False)
                logging.info("Database updated successfully.")
            except exc.SQLAlchemyError as e:
                logging.error(f"Database error: {e}")
        else:
            logging.warning("Index calculation failed; no data to save.")
    else:
        logging.warning("Data fetch failed; no data to process.")

def schedule_task():
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_data, 'interval', days=1)
    scheduler.start()

def display_data():
    try:
        engine = create_engine(DB_URL)
        df = pd.read_sql('index_data', engine)
        st.title("Health and Prosperity Index")
        st.bar_chart(df.set_index('state')['index'])
    except exc.SQLAlchemyError as e:
        st.error(f"Error retrieving data from database: {e}")
        logging.error(f"Error retrieving data from database: {e}")

if __name__ == '__main__':
    schedule_task()
    display_data()
