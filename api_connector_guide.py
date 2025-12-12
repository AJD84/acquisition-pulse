import requests
import pandas as pd
import json

# --- PHASE 7: SHOPIFY API CONNECTOR GUIDE ---

# 1. PLACEHOLDER CREDENTIALS (DO NOT USE REAL KEYS HERE)
# In a real app, these would be loaded securely from a .env file.
SHOPIFY_STORE_NAME = "your-store-name"  # e.g., 'acme-widgets-2025'
SHOPIFY_ACCESS_TOKEN = "shpat_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" # Placeholder Token

# 2. API ENDPOINT AND HEADERS
# We are requesting the 'orders' resource.
API_URL = f"https://{SHOPIFY_STORE_NAME}.myshopify.com/admin/api/2024-04/orders.json"

# Headers are required for secure authentication and specifying data format.
HEADERS = {
    "X-Shopify-Access-Token": SHOPIFY_ACCESS_TOKEN,
    "Content-Type": "application/json"
}

# Parameters to filter the data we want
# 'status=any' gets all orders (open, closed, cancelled)
# 'limit=50' is a typical page limit
# 'fields=' ensures we only download the required data, saving time and bandwidth
PARAMS = {
    "status": "any",
    "limit": 50,
    "fields": "id, created_at, total_price, customer, total_line_items_price, financial_status, source_name, source_identifier"
}

def fetch_shopify_orders():
    """
    Simulates fetching order data from the Shopify API.
    """
    print(f"Attempting to connect to: {API_URL}")
    print("-" * 40)
    
    try:
        # 3. MAKE THE SECURE API REQUEST
        response = requests.get(API_URL, headers=HEADERS, params=PARAMS)
        
        # Check if the request was successful (HTTP status 200)
        if response.status_code == 200:
            print("SUCCESS: Received data from Shopify API endpoint.")
            
            # 4. PARSE THE JSON RESPONSE
            data = response.json()
            orders = data.get('orders', [])
            
            if not orders:
                print("WARNING: API connection was successful, but no orders were found.")
                return pd.DataFrame() # Return empty DataFrame
            
            # 5. TRANSFORM JSON INTO DATAFRAME
            # This is where we would map the JSON fields to our required DataFrame columns:
            
            # In a real application, we would iterate and calculate COGS, 
            # and map source_identifier to our UTMS.
            
            # Example: Creating a DataFrame from the IDs of the first 5 orders
            order_ids = [order['id'] for order in orders[:5]]
            df_example = pd.DataFrame(order_ids, columns=['order_id'])
            print(f"Successfully loaded {len(orders)} orders into a list.")
            print("Example of the first 5 Order IDs loaded into a DataFrame:")
            print(df_example)
            
            return df_example
            
        else:
            print(f"ERROR: API Request failed with status code {response.status_code}")
            print("Check your API Token and Store Name placeholders.")
            print(f"Response text: {response.text[:200]}...")
            return pd.DataFrame()
            
    except requests.exceptions.RequestException as e:
        print(f"FATAL ERROR: Could not connect to the internet or API. {e}")
        return pd.DataFrame()

# Execute the function
if __name__ == "__main__":
    df_live_orders = fetch_shopify_orders()
    if not df_live_orders.empty:
        print("\nReady to process live data using the logic from our previous phases!")