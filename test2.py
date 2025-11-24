# import pandas as pd
# from sqlalchemy import create_engine, text
# import os
# from datetime import datetime

# # --- 1. SETUP ---
# engine = create_engine('mysql+pymysql://jabalazer:jabalazer@localhost/cleaning')

# # Define the directory where your Excel files are located
# file_directory = r'C:\BI FILES'

# # Define the mapping of inventory filenames to their corresponding Branch_ID
# branch_files = {
#     'branch1_inventory.xlsx': 1,
#     'branch2_inventory.xlsx': 2,
#     'branch3_inventory.xlsx': 3,
# }

# # --- 2. EXTRACTION ---
# all_inventory = []
# print("Starting inventory data extraction...")
# for file_name, branch_id in branch_files.items():
#     full_path = os.path.join(file_directory, file_name)
#     try:
#         # Read Excel file - only the columns we need
#         df = pd.read_excel(full_path, usecols=['Description', 'Cost', 'QtyOutStock'])
#         df['Branch_ID'] = branch_id
#         # Add current date as the inventory snapshot date
#         df['InventoryDate'] = datetime.now().date()
#         all_inventory.append(df)
#         print(f"Successfully loaded {file_name} for Branch ID {branch_id}.")
#     except FileNotFoundError:
#         print(f"WARNING: File not found at {full_path}. Skipping.")
#     except ValueError as e:
#         print(f"WARNING: Column error in {file_name}: {e}. Skipping.")

# if not all_inventory:
#     print("No inventory data was loaded. Exiting.")
# else:
#     inventory_df = pd.concat(all_inventory, ignore_index=True)
#     print(f"Inventory extraction complete. Total rows extracted: {len(inventory_df)}")

#     # --- 3. DATA CLEANING & TRANSFORMATION ---
#     print("Starting data cleaning and transformation...")
    
#     original_count = len(inventory_df)
    
#     # Clean data
#     inventory_df.dropna(how='all', inplace=True)
#     inventory_df.dropna(subset=['Description', 'Cost', 'QtyOutStock'], inplace=True)
    
#     # Standardize product names
#     inventory_df['Description'] = inventory_df['Description'].str.strip()
    
#     # Ensure correct data types
#     inventory_df['Cost'] = pd.to_numeric(inventory_df['Cost'], errors='coerce').fillna(0)
#     inventory_df['QtyOutStock'] = pd.to_numeric(inventory_df['QtyOutStock'], errors='coerce').fillna(0).astype(int)
    
#     # Filter out invalid data (products with no valid cost or negative stock)
#     inventory_df = inventory_df[inventory_df['Cost'] > 0]
#     inventory_df = inventory_df[inventory_df['QtyOutStock'] >= 0]
    
#     # Remove duplicate products within each branch
#     duplicate_check_columns = ['Description', 'Cost', 'Branch_ID']
#     before_dedup = len(inventory_df)
#     inventory_df.drop_duplicates(subset=duplicate_check_columns, inplace=True, keep='first')
#     duplicates_removed = before_dedup - len(inventory_df)
    
#     print(f"Data cleaning finished. Rows after cleaning: {len(inventory_df)}")
#     print(f"Removed {duplicates_removed} duplicate product(s)")

#     # Create Dim_Product (unique products)
#     dim_product = inventory_df[['Description', 'Cost']].drop_duplicates()
#     dim_product.rename(columns={'Description': 'Product_Name', 'Cost': 'Unit_Price'}, inplace=True)
#     dim_product.reset_index(drop=True, inplace=True)
#     dim_product['Product_ID'] = dim_product.index + 1

#     # Merge Product_ID back to inventory dataframe
#     inventory_df = inventory_df.merge(
#         dim_product,
#         left_on=['Description', 'Cost'],
#         right_on=['Product_Name', 'Unit_Price'],
#         how='left'
#     )

#     # Create Dim_Time for inventory dates
#     dim_time = pd.DataFrame({'Date_ID': inventory_df['InventoryDate'].unique()})
#     dim_time['Date_ID'] = pd.to_datetime(dim_time['Date_ID'])
#     dim_time['Year'] = dim_time['Date_ID'].dt.year
#     dim_time['Month'] = dim_time['Date_ID'].dt.month
#     dim_time['Day'] = dim_time['Date_ID'].dt.day
#     dim_time['Weekday'] = dim_time['Date_ID'].dt.day_name()

#     # Prepare Fact_Inventory
#     fact_inventory = inventory_df.copy()
#     fact_inventory = fact_inventory.rename(columns={
#         'InventoryDate': 'Date_ID',
#         'QtyOutStock': 'Stock_Level'
#     })
#     fact_inventory = fact_inventory[['Date_ID', 'Branch_ID', 'Product_ID', 'Stock_Level']]
    
#     print(f"Transformation complete. Prepared {len(dim_product)} products and {len(fact_inventory)} inventory records.")

#     # --- 4. LOADING ---
#     print("Starting data loading into the database...")
#     try:
#         with engine.connect() as connection:
#             with connection.begin():
#                 # Load Dim_Product safely
#                 dim_product.to_sql('temp_products', connection, if_exists='replace', index=False)
#                 connection.execute(text('''
#                     INSERT INTO Dim_Product (Product_ID, Product_Name, Unit_Price)
#                     SELECT t.Product_ID, t.Product_Name, t.Unit_Price
#                     FROM temp_products t
#                     LEFT JOIN Dim_Product d ON t.Product_Name = d.Product_Name AND t.Unit_Price = d.Unit_Price
#                     WHERE d.Product_ID IS NULL;
#                 '''))
#                 print(f"Loaded {len(dim_product)} unique products into Dim_Product.")

#                 # Load Dim_Time safely
#                 dim_time.to_sql('temp_dim_time', connection, if_exists='replace', index=False)
#                 connection.execute(text('''
#                     INSERT INTO Dim_Time (Date_ID, Year, Month, Day, Weekday)
#                     SELECT t.Date_ID, t.Year, t.Month, t.Day, t.Weekday
#                     FROM temp_dim_time t
#                     LEFT JOIN Dim_Time d ON t.Date_ID = d.Date_ID
#                     WHERE d.Date_ID IS NULL;
#                 '''))
#                 print(f"Loaded {len(dim_time)} unique date(s) into Dim_Time.")

#             # Load Fact_Inventory
#             fact_inventory.to_sql('Fact_Inventory', engine, if_exists='append', index=False)
#             print(f"Loaded {len(fact_inventory)} inventory records into Fact_Inventory.")
#             print("\nInventory ETL process completed successfully.")

#     except Exception as e:
#         print(f"\nAn error occurred during the database loading phase: {e}")


import pandas as pd
from sqlalchemy import create_engine, text
import io
import requests
from datetime import datetime

# --- 1. SETUP ---
engine = create_engine('mysql+pymysql://jabalazer:jabalazer@localhost/cleaning')

# FIXED file IDs - Set these once and update the files in Google Drive
# To get file ID: Right-click file in Google Drive > Share > "Anyone with link" > Copy link
# Extract ID from: https://drive.google.com/file/d/FILE_ID_HERE/view
branch_files = {
    '1xGa3wMJPtaXJsEZQdmxffflkV26s7hqM': 1,  # branch1_inventory.xlsx
    # 'YOUR_FILE_ID_2': 2,  # branch2_inventory.xlsx
    # 'YOUR_FILE_ID_3': 3,  # branch3_inventory.xlsx
}

def read_excel_from_gdrive(file_id, usecols=None):
    """
    Read Excel file directly from Google Drive without downloading
    """
    url = f'https://drive.google.com/uc?export=download&id={file_id}'
    response = requests.get(url)
    
    if response.status_code == 200:
        return pd.read_excel(io.BytesIO(response.content), usecols=usecols)
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

# --- 2. EXTRACTION ---
all_inventory = []
print(f"Starting inventory ETL - {datetime.now()}")
print("Reading inventory data from Google Drive...")

for file_id, branch_id in branch_files.items():
    try:
        print(f"Reading inventory for Branch ID {branch_id}...")
        # Only read the columns we need
        df = read_excel_from_gdrive(file_id, usecols=['Description', 'Cost', 'QtyOutStock'])
        df['Branch_ID'] = branch_id
        df['InventoryDate'] = datetime.now().date()
        all_inventory.append(df)
        print(f"✓ Successfully loaded {len(df)} products for Branch ID {branch_id}")
    except FileNotFoundError:
        print(f"✗ File not found for Branch {branch_id}. Skipping.")
    except ValueError as e:
        print(f"✗ Column error for Branch {branch_id}: {e}. Skipping.")
    except Exception as e:
        print(f"✗ Error loading Branch {branch_id}: {e}. Skipping.")

if not all_inventory:
    print("No inventory data was loaded. Exiting.")
else:
    inventory_df = pd.concat(all_inventory, ignore_index=True)
    print(f"\n✓ Inventory extraction complete. Total rows: {len(inventory_df)}")

    # --- 3. DATA CLEANING & TRANSFORMATION ---
    print("\nCleaning and transforming data...")
    
    original_count = len(inventory_df)
    
    # Clean data
    inventory_df.dropna(how='all', inplace=True)
    inventory_df.dropna(subset=['Description', 'Cost', 'QtyOutStock'], inplace=True)
    
    # Standardize product names
    inventory_df['Description'] = inventory_df['Description'].str.strip()
    
    # Ensure correct data types
    inventory_df['Cost'] = pd.to_numeric(inventory_df['Cost'], errors='coerce').fillna(0)
    inventory_df['QtyOutStock'] = pd.to_numeric(inventory_df['QtyOutStock'], errors='coerce').fillna(0).astype(int)
    
    # Filter out invalid data
    inventory_df = inventory_df[inventory_df['Cost'] > 0]
    inventory_df = inventory_df[inventory_df['QtyOutStock'] >= 0]
    
    # Remove duplicate products within each branch
    duplicate_check_columns = ['Description', 'Cost', 'Branch_ID']
    before_dedup = len(inventory_df)
    inventory_df.drop_duplicates(subset=duplicate_check_columns, inplace=True, keep='first')
    duplicates_removed = before_dedup - len(inventory_df)
    
    print(f"✓ Data cleaning finished. Rows after cleaning: {len(inventory_df)}")
    print(f"✓ Removed {duplicates_removed} duplicate product(s)")

    # Create Dim_Product (unique products)
    dim_product = inventory_df[['Description', 'Cost']].drop_duplicates()
    dim_product.rename(columns={'Description': 'Product_Name', 'Cost': 'Unit_Price'}, inplace=True)
    dim_product.reset_index(drop=True, inplace=True)
    dim_product['Product_ID'] = dim_product.index + 1

    # Merge Product_ID back to inventory dataframe
    inventory_df = inventory_df.merge(
        dim_product,
        left_on=['Description', 'Cost'],
        right_on=['Product_Name', 'Unit_Price'],
        how='left'
    )

    # Create Dim_Time for inventory dates
    dim_time = pd.DataFrame({'Date_ID': inventory_df['InventoryDate'].unique()})
    dim_time['Date_ID'] = pd.to_datetime(dim_time['Date_ID'])
    dim_time['Year'] = dim_time['Date_ID'].dt.year
    dim_time['Month'] = dim_time['Date_ID'].dt.month
    dim_time['Day'] = dim_time['Date_ID'].dt.day
    dim_time['Weekday'] = dim_time['Date_ID'].dt.day_name()

    # Prepare Fact_Inventory
    fact_inventory = inventory_df.copy()
    fact_inventory = fact_inventory.rename(columns={
        'InventoryDate': 'Date_ID',
        'QtyOutStock': 'Stock_Level'
    })
    fact_inventory = fact_inventory[['Date_ID', 'Branch_ID', 'Product_ID', 'Stock_Level']]
    
    print(f"✓ Transformation complete. Prepared {len(dim_product)} products and {len(fact_inventory)} inventory records")

    # --- 4. LOADING ---
    print("\nLoading data to database...")
    try:
        with engine.connect() as connection:
            with connection.begin():
                # Load Dim_Product safely
                dim_product.to_sql('temp_products', connection, if_exists='replace', index=False)
                connection.execute(text('''
                    INSERT INTO Dim_Product (Product_ID, Product_Name, Unit_Price)
                    SELECT t.Product_ID, t.Product_Name, t.Unit_Price
                    FROM temp_products t
                    LEFT JOIN Dim_Product d ON t.Product_Name = d.Product_Name AND t.Unit_Price = d.Unit_Price
                    WHERE d.Product_ID IS NULL;
                '''))
                print(f"✓ Loaded {len(dim_product)} unique products into Dim_Product")

                # Load Dim_Time safely
                dim_time.to_sql('temp_dim_time', connection, if_exists='replace', index=False)
                connection.execute(text('''
                    INSERT INTO Dim_Time (Date_ID, Year, Month, Day, Weekday)
                    SELECT t.Date_ID, t.Year, t.Month, t.Day, t.Weekday
                    FROM temp_dim_time t
                    LEFT JOIN Dim_Time d ON t.Date_ID = d.Date_ID
                    WHERE d.Date_ID IS NULL;
                '''))
                print(f"✓ Loaded {len(dim_time)} unique date(s) into Dim_Time")

            # Load Fact_Inventory
            fact_inventory.to_sql('Fact_Inventory', engine, if_exists='append', index=False)
            print(f"✓ Loaded {len(fact_inventory)} inventory records into Fact_Inventory")
            print(f"\n✅ Inventory ETL completed successfully - {datetime.now()}")

    except Exception as e:
        print(f"\n❌ Database error: {e}")

