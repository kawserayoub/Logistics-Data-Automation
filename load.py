import os
from dotenv import load_dotenv
import pandas as pd
import logging
from sqlalchemy import create_engine

load_dotenv()

db_host = os.getenv("DB_HOST")
db_name = os.getenv("DB_NAME")

logging.basicConfig(
    filename='etl.log', 
    level=logging.INFO, 
    format='%(asctime)s:%(levelname)s:%(message)s'
    )

def load_data(df, table_name):
    """
    Loading the transformed data into a SQL database.
    """
    try:
        engine = create_engine(f"mssql+pyodbc://{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes")
        connection=engine.connect()
        
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"Data loaded successfully into the {table_name} table in SupplyChainDB.")
    
    except Exception as e:
        logging.error(f"Error loading data into SQL: {e}")
        raise
    finally:
        connection.close()

if __name__ == "__main__":
    try:
        df_transformed = pd.read_csv("transformed_data.csv")
        table_name = 'supply_chain_data'
        load_data(df_transformed, table_name)
        
    except Exception as e:
        logging.error(f"Loading process failed: {e}")
        raise
