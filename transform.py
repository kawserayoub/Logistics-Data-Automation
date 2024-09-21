import pandas as pd
import logging
from extract import extract_data , validate_data

logging.basicConfig(
    filename='etl.log', 
    level=logging.INFO, 
    format='%(asctime)s:%(levelname)s:%(message)s')

def transform_data(df):
    """
    Cleans and transforms the raw data by adding new features and 
    performing aggregations.
    """
    try:
        # Clean the data by replacing missing values with 0
        df.fillna(0, inplace=True)
        
        # Convert date columns to datetime objects
        df['order date (DateOrders)'] = pd.to_datetime(df['order date (DateOrders)'], errors='coerce')
        df['shipping date (DateOrders)'] = pd.to_datetime(df['shipping date (DateOrders)'], errors='coerce')

        # Calculate delivery delay in days
        df['Delivery Delay'] = (df['shipping date (DateOrders)'] - df['order date (DateOrders)']).dt.days
        
        # Mark if there is a risk of late delivery
        df['Late_delivery_risk'] = (df['Delivery Delay'] > 0).astype(int)
        
        # Calculate the benefit per order and profit margin
        df['Benefit per Order'] = df['Product Price'] - df['Order Item Total']
        df['Profit Margin'] = (df['Benefit per Order'] / df['Product Price']) * 100

        logging.info("Data cleaned, and new features added successfully.")

        # Average delivery delay grouped by region
        region_shipping_avg = df.groupby('Order Region')['Delivery Delay'].mean().reset_index()

        # Group sales by month
        df['Order Month'] = df['order date (DateOrders)'].dt.to_period('M')
        monthly_sales = df.groupby('Order Month')['Sales'].sum().reset_index()

        logging.info("Aggregations completed successfully.")
        
        return df, region_shipping_avg, monthly_sales

    except Exception as e:
        logging.error(f"Error during data transformation: {e}")
        raise

if __name__ == "__main__":
    try:
        file_path = "C:/DataCoSupplyChainDataset.csv"
        df = extract_data(file_path)
        validate_data(df)

        df_transformed, region_shipping_avg, monthly_sales = transform_data(df)

        # Export the transformed data and aggregated data to CSV files
        df_transformed.to_csv("transformed_data.csv", index=False)
        region_shipping_avg.to_csv("region_shipping_avg.csv", index=False)
        monthly_sales.to_csv("monthly_sales.csv", index=False)

        logging.info("Data transformation process completed successfully.")
    
    except Exception as e:
        logging.error(f"Data transformation process failed: {e}")
        raise