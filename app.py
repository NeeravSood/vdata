import os
import requests
import pandas as pd
from apscheduler.schedulers.background import BackgroundScheduler
import logging
import streamlit as st
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Environment variables for configuration
API_URL = os.getenv("DATAUSA_API_URL", "https://datausa.io/about/api/")  # Default or override via env
DATA_FILE_PATH = os.getenv("DATA_FILE_PATH", "index_data.csv")

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
                # Save to a CSV file instead of a database
                df.to_csv(DATA_FILE_PATH, index=False)
                logging.info(f"Data updated successfully. File saved to {DATA_FILE_PATH}")
            except Exception as e:
                logging.error(f"Error saving data to file: {e}")
        else:
            logging.warning("Index calculation failed; no data to save.")
    else:
        logging.warning("Data fetch failed; no data to process.")

def schedule_task():
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_data, 'interval', days=1)
    scheduler.start()

def display_data():
    # Retry mechanism to handle file access conflicts
    retries = 3
    for attempt in range(retries):
        try:
            # Load data from the CSV file
            df = pd.read_csv(DATA_FILE_PATH)
            st.title("Health and Prosperity Index")
            st.bar_chart(df.set_index('state')['index'])
            break  # Exit the loop if successful
        except FileNotFoundError:
            st.error("Data file not found. Please run the update first.")
            logging.error("Data file not found.")
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            if attempt < retries - 1:
                time.sleep(1)  # Wait before retrying
            else:
                st.error(f"Error loading data after {retries} attempts: {e}")

if __name__ == '__main__':
    update_data()  # Ensure data is fetched and saved initially
    schedule_task()
    display_data()
