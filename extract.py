import pandas as pd
import logging

logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s:%(levelname)s:%(message)s'
)

def extract_data(file_path):
    """
    Extracts data from the CSV file and logs the process.
    """
    try:
        data = pd.read_csv(file_path, encoding='Latin-1')
        logging.info(f"Sucessfully extracted data from {file_path}.")
        return data
    except FileNotFoundError as fnf_error:
        logging.error(f"File not found: {fnf_error}")
        raise
    except Exception as e:
        logging.error(f"Error extracting data: {e}, exc_info=True")
        raise

def validate_data(df):
    """
    Validates the extracted data by checking for missing columns, 
    null values, data types, and valid ranges.
    """
    required_columns = [
        'order date (DateOrders)', 'shipping date (DateOrders)', 
        'Product Price', 'Order Item Quantity', 'Order Item Total', 
        'Customer Id', 'Order Id', 'Shipping Mode'
    ]
    
    # Check for missing columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        error_message = f"Missing columns: {', '.join(missing_columns)}"
        logging.error(error_message)
        raise ValueError(error_message)
    
    # Check for missing values
    for col in required_columns:
        if df[col].isnull().any():
            error_message = f"Missing values in column: {col}"
            logging.error(error_message)
            raise ValueError(error_message)
    
    # Check numeric columns for valid values
    if (df['Product Price'] < 0).any():
        error_message = "Product Price contains negative values."
        logging.error(error_message)
        raise ValueError(error_message)
    
    if (df['Order Item Quantity'] <= 0).any():
        error_message = "Order Item Quantity contains zero or negative values."
        logging.error(error_message)
        raise ValueError(error_message)
    
    # Check date columns are valid dates
    for date_col in ['order date (DateOrders)', 'shipping date (DateOrders)']:
        try:
            pd.to_datetime(df[date_col], errors='raise')
        except Exception as e:
            error_message = f"Invalid date format in column {date_col}: {e}"
            logging.error(error_message)
            raise ValueError(error_message)
    
    logging.info("Data validation successful")
    return True

def main():
    try:
        file_path = "C:/DataCoSupplyChainDataset.csv"  
        df = extract_data(file_path)
        validate_data(df)
        logging.info("Extraction and validation completed successfully.")
        return df
    
    except Exception as e:
        logging.error(f"ETL process failed: {e}")
        raise
    
if __name__ == "__main__":
    main()