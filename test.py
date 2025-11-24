import pandas as pd
from sqlalchemy import create_engine, text
import io
import requests

# --- 1. SETUP ---
engine = create_engine('mysql+pymysql://jabalazer:jabalazer@localhost/cleaning')

# Define the mapping of Google Drive file IDs to their corresponding Branch_ID
# To get file ID: Right-click file in Google Drive > Share > Copy link
# From: https://drive.google.com/file/d/1ABC123xyz/view?usp=sharing
# Extract: 1ABC123xyz
branch_files = {
    '1YrRfEsObH3lvedpuQRs5O1TWRbezZzhx': 1,  # branch1_expenses.xlsx
    # 'YOUR_FILE_ID_2': 2,  # branch2_expenses.xlsx
    # 'YOUR_FILE_ID_3': 3,  # branch3_expenses.xlsx
}

def read_excel_from_gdrive(file_id):
    """
    Read Excel file directly from Google Drive without downloading
    """
    # Construct the download URL
    url = f'https://drive.google.com/uc?export=download&id={file_id}'
    
    # Download the file content into memory
    response = requests.get(url)
    
    if response.status_code == 200:
        # Read Excel from bytes in memory
        return pd.read_excel(io.BytesIO(response.content))
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")

# --- 2. EXTRACTION ---
all_expenses = []
print("Starting data extraction from Google Drive...")

for file_id, branch_id in branch_files.items():
    try:
        print(f"Reading file from Google Drive for Branch ID {branch_id}...")
        df = read_excel_from_gdrive(file_id)
        df['Branch_ID'] = branch_id
        all_expenses.append(df)
        print(f"Successfully loaded data for Branch ID {branch_id}.")
    except Exception as e:
        print(f"WARNING: Error reading file for Branch {branch_id}: {e}. Skipping.")

if not all_expenses:
    print("No data was loaded. Exiting.")
else:
    expenses_df = pd.concat(all_expenses, ignore_index=True)
    print(f"Data extraction complete. Total rows extracted: {len(expenses_df)}")

    # --- 3. DATA CLEANING & TRANSFORMATION ---
    print("Starting data cleaning and transformation...")
    
    # Store original count for comparison
    original_count = len(expenses_df)
    
    # Clean data
    expenses_df.dropna(how='all', inplace=True)
    expenses_df.dropna(subset=['Type', 'TransactionDate', 'Payment'], inplace=True)
    expenses_df['Type'] = expenses_df['Type'].str.strip().str.title()
    if 'Particulars' in expenses_df.columns:
        expenses_df['Particulars'] = expenses_df['Particulars'].str.strip()
    if 'TransactedBy' in expenses_df.columns:
        expenses_df['TransactedBy'] = expenses_df['TransactedBy'].str.strip().str.title()
    expenses_df['TransactionDate'] = pd.to_datetime(expenses_df['TransactionDate'], errors='coerce')
    expenses_df['Payment'] = pd.to_numeric(expenses_df['Payment'], errors='coerce').fillna(0)
    expenses_df.dropna(subset=['TransactionDate'], inplace=True)
    expenses_df = expenses_df[expenses_df['Payment'] > 0]
    
    # Remove exact duplicate rows
    before_dedup = len(expenses_df)
    expenses_df.drop_duplicates(inplace=True)
    duplicates_removed = before_dedup - len(expenses_df)
    
    print(f"Data cleaning finished. Rows after cleaning: {len(expenses_df)}")
    print(f"Removed {duplicates_removed} duplicate row(s)")

    # Create Dim_Expense_Type (unique expense types only)
    dim_expense_type = pd.DataFrame({'Expense_Name': expenses_df['Type'].unique()})
    dim_expense_type.reset_index(inplace=True)
    dim_expense_type.rename(columns={'index': 'Expense_Type_ID'}, inplace=True)
    dim_expense_type['Expense_Type_ID'] += 1

    # Merge Expense_Type_ID back to main dataframe
    expenses_df = expenses_df.merge(dim_expense_type, left_on='Type', right_on='Expense_Name', how='left')

    # Create Dim_Time (unique dates only)
    unique_dates = expenses_df['TransactionDate'].dt.date.unique()
    dim_time = pd.DataFrame({'Date_ID': unique_dates})
    dim_time['Date_ID'] = pd.to_datetime(dim_time['Date_ID'])
    dim_time['Year'] = dim_time['Date_ID'].dt.year
    dim_time['Month'] = dim_time['Date_ID'].dt.month
    dim_time['Day'] = dim_time['Date_ID'].dt.day
    dim_time['Weekday'] = dim_time['Date_ID'].dt.day_name()

    # Prepare Fact_Expense
    fact_expense = expenses_df.copy()
    fact_expense['Date_ID'] = fact_expense['TransactionDate'].dt.date
    fact_expense = fact_expense.rename(columns={'Payment': 'Amount_Spent'})
    fact_expense = fact_expense[['Date_ID', 'Branch_ID', 'Expense_Type_ID', 'Amount_Spent']]
    
    print(f"Transformation complete. Prepared {len(fact_expense)} expense records for loading.")

    # --- 4. LOADING ---
    print("Starting data loading into the database...")
    try:
        with engine.connect() as connection:
            with connection.begin():
                # Load Dim_Expense_Type safely
                dim_expense_type.to_sql('temp_expense_types', connection, if_exists='replace', index=False)
                connection.execute(text('''
                    INSERT INTO Dim_Expense_Type (Expense_Type_ID, Expense_Name)
                    SELECT t.Expense_Type_ID, t.Expense_Name
                    FROM temp_expense_types t
                    LEFT JOIN Dim_Expense_Type d ON t.Expense_Name = d.Expense_Name
                    WHERE d.Expense_Type_ID IS NULL;
                '''))
                print(f"Loaded {len(dim_expense_type)} unique expense types into Dim_Expense_Type.")

                # Load Dim_Time safely
                dim_time.to_sql('temp_dim_time', connection, if_exists='replace', index=False)
                connection.execute(text('''
                    INSERT INTO Dim_Time (Date_ID, Year, Month, Day, Weekday)
                    SELECT t.Date_ID, t.Year, t.Month, t.Day, t.Weekday
                    FROM temp_dim_time t
                    LEFT JOIN Dim_Time d ON t.Date_ID = d.Date_ID
                    WHERE d.Date_ID IS NULL;
                '''))
                print(f"Loaded {len(dim_time)} unique dates into Dim_Time.")

            # Load Fact_Expense directly
            fact_expense.to_sql('Fact_Expense', engine, if_exists='append', index=False)
            print(f"Loaded {len(fact_expense)} expense records into Fact_Expense.")
            print("\nETL process completed successfully.")

    except Exception as e:
        print(f"\nAn error occurred during the database loading phase: {e}")
