import pandas as pd
from extract import extract_data, validate_data
from transform import transform_data
from load import load_data

def get_sample_data():
    """Return sample data used for testing as a DataFrame."""
    sample_data = {
        'order date (DateOrders)': ['2023-01-01', '2023-02-01'],
        'shipping date (DateOrders)': ['2023-01-05', '2023-02-06'],
        'Product Price': [100, 150],
        'Order Item Quantity': [1, 2],
        'Order Item Total': [100, 300],
        'Customer Id': [1, 2],
        'Order Id': [101, 102],
        'Shipping Mode': ['Standard', 'Express'],
        'Order Region': ['East', 'West'],
        'Sales': [100, 300]
    }
    return pd.DataFrame(sample_data)

def test_extract_data():
    file_path = "C:/DataCoSupplyChainDataset.csv"
    df_extracted = extract_data(file_path)
    assert isinstance(df_extracted, pd.DataFrame), "Data extraction failed to return a DataFrame."

def test_validate_data():
    df = get_sample_data()
    validate_result = validate_data(df)
    assert validate_result == True, "Data validation failed."

def test_transform_data():
    df = get_sample_data()
    df_transformed, region_shipping_avg, monthly_sales = transform_data(df)
    assert 'Delivery Delay' in df_transformed.columns, "Delivery Delay column missing."
    assert 'Profit Margin' in df_transformed.columns, "Profit Margin column missing."
    assert len(region_shipping_avg) > 0, "No shipping data aggregated."
    assert len(monthly_sales) > 0, "No monthly sales data aggregated."

def test_load_data():
    df = get_sample_data()
    try:
        load_data(df, 'test_table')
        assert True, "Data loaded successfully."
    except Exception:
        assert False, "Loading data to SQL failed."