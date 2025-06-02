import pandas as pd
import os
import glob
import argparse
from sqlalchemy import create_engine, text
from sqlalchemy.types import TEXT, INTEGER, DATE
from datetime import datetime

# Database connection parameters
DB_URL = os.getenv('DATABASE_URL', 'postgresql://username:password@localhost:5432/database')
TABLE_NAME = 'entity_metadata'

def process_csv_file(file_path):
    """Process a single CSV file and return a cleaned DataFrame."""
    print(f"\nProcessing file: {file_path}")
    df = pd.read_csv(file_path, encoding='latin1')
    print(f"Loaded {len(df)} rows from {os.path.basename(file_path)}")
    print("Original Data Types:\n", df.dtypes)
    print(f"Columns: {df.columns.tolist()}")
    
    # Convert numeric columns to appropriate integer types with error handling
    try:
        numeric_columns = ['customer_id', 'registration_number', 'tax_identification_no', 
                           'unit_number', 'postal_zip_code']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    except Exception as e:
        print(f"Warning: Error converting numeric columns: {e}")
    
    # Get current timestamp
    now = pd.to_datetime(datetime.now())
    
    # Convert date columns to datetime format
    try:
        date_columns = ['date_of_incorporation', 'create_date', 'last_update_date']
        for col in date_columns:
            if col in df.columns:
                # Try multiple date formats
                df[col] = pd.to_datetime(df[col], errors='coerce')
                
        # Fill missing dates with current timestamp
        if 'create_date' in df.columns:
            df['create_date'] = df['create_date'].fillna(now)
        if 'last_update_date' in df.columns:
            df['last_update_date'] = df['last_update_date'].fillna(now)
    except Exception as e:
        print(f"Warning: Error processing date columns: {e}")
    
    # Define text columns
    text_columns = [
        'incorporator_name', 'corporation_name', 'customer_type', 'country',
        'state_province', 'industry', 'address_line1', 'address_line2', 'city'
    ]
    
    # Convert text columns to string type if they exist in the dataframe
    for col in text_columns:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)
    
    # Convert all string values to uppercase
    df = df.applymap(lambda x: x.upper() if isinstance(x, str) else x)
    
    # Handle duplicates within this file by keeping the latest record based on last_update_date
    duplicate_mask = df.duplicated(subset=['customer_id'], keep=False)
    duplicate_count = duplicate_mask.sum()
    print(f"Found {duplicate_count} duplicate entries based on customer_id within this file")
    
    if duplicate_count > 0:
        # Display the duplicate records
        print("\nDuplicate Records in this CSV:")
        duplicates = df[duplicate_mask].sort_values('customer_id')
        print(duplicates[['customer_id', 'corporation_name', 'last_update_date']].head(5))
        if len(duplicates) > 5:
            print(f"... and {len(duplicates) - 5} more")
        
        # Sort by last_update_date and keep the latest version of each record
        df = df.sort_values('last_update_date').drop_duplicates(
            subset=['customer_id'], keep='last')
        print(f"After removing internal file duplicates, {len(df)} records remain")
    
    return df

def merge_multiple_csv_files(file_paths):
    """Merge multiple CSV files into a single DataFrame, handling duplicates across files."""
    all_dfs = []
    file_info = []
    
    for file_path in file_paths:
        try:
            df = process_csv_file(file_path)
            if not df.empty:
                all_dfs.append(df)
                file_info.append(f"{os.path.basename(file_path)}: {len(df)} records")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
    
    if not all_dfs:
        print("No valid CSV files found to process.")
        return None
    
    print("\n=== File Processing Summary ===")
    for info in file_info:
        print(info)
    
    # Combine all DataFrames
    combined_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nCombined {len(all_dfs)} files with a total of {len(combined_df)} records")
    
    # Check for duplicates across all files
    duplicate_mask = combined_df.duplicated(subset=['customer_id'], keep=False)
    duplicate_count = duplicate_mask.sum()
    print(f"Found {duplicate_count} duplicate customer_ids across all files")
    
    if duplicate_count > 0:
       # Show all of the duplicates
        duplicates = combined_df[duplicate_mask].sort_values(['customer_id', 'last_update_date'])
        print(f"\nAll cross-file duplicates ({duplicate_count} records):")
        print(duplicates)  # This will display all duplicate records
        
        # Group by customer_id and display sample differences
        sample_duplicates = duplicates['customer_id'].unique()[:3]  # Show up to 3 different customer_ids
        for cust_id in sample_duplicates:
            records = duplicates[duplicates['customer_id'] == cust_id]
            print(f"\nCustomer ID: {cust_id} has {len(records)} records:")
            for idx, row in records.iterrows():
                print(f"  - Update date: {row['last_update_date']}, Corp Name: {row['corporation_name']}")
        
        # Sort by last_update_date and keep the latest version of each record
        print("Resolving duplicates across files by keeping most recent record...")
        combined_df = combined_df.sort_values('last_update_date').drop_duplicates(
            subset=['customer_id'], keep='last')
        print(f"After removing cross-file duplicates, {len(combined_df)} records remain")
    
    return combined_df

def get_dtype_map():
    """Define PostgreSQL data type mapping."""
    return {
        'customer_id': INTEGER(),
        'incorporator_name': TEXT(),
        'corporation_name': TEXT(),
        'customer_type': TEXT(),
        'date_of_incorporation': DATE(),
        'country': TEXT(),
        'state_province': TEXT(),
        'registration_number': INTEGER(),
        'tax_identification_no': INTEGER(),
        'industry': TEXT(),
        'unit_number': INTEGER(),
        'address_line1': TEXT(),
        'address_line2': TEXT(),
        'city': TEXT(),
        'postal_zip_code': INTEGER(),
        'create_date': DATE(),
        'last_update_date': DATE()
    }

def update_database(df_modified, engine, table_name, dtype_map, force_update=False):
    """
    Update the database with the processed data.
    
    Args:
        df_modified: DataFrame with processed data
        engine: SQLAlchemy engine
        table_name: Target table name
        dtype_map: Data type mapping for SQL
        force_update: If True, will always update records even if timestamps are older
    """
    try:
        # Check if table exists, create it if it doesn't
        with engine.connect() as conn:
            result = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                );
            """))
            table_exists = result.fetchone()[0]
            
            if not table_exists:
                print(f"Table '{table_name}' does not exist. Creating it...")
                # Create the table with the proper schema but no data
                df_modified.head(0).to_sql(table_name, engine, if_exists='replace', index=False, dtype=dtype_map)
                # Insert all records
                df_modified.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_map)
                print(f"Created table and inserted {len(df_modified)} records.")
            else:
                # Table exists, use proper merge approach for upsert
                print(f"Table '{table_name}' exists. Performing controlled merge operation...")
                
                # Get existing data from the database
                try:
                    existing_data_query = f"SELECT * FROM {table_name}"
                    existing_df = pd.read_sql(existing_data_query, engine)
                    print(f"Retrieved {len(existing_df)} existing records from database.")
                except Exception as e:
                    print(f"Error retrieving existing data: {e}")
                    print("Proceeding with empty existing data set.")
                    existing_df = pd.DataFrame(columns=df_modified.columns)
                
                # Ensure date columns are properly formatted for comparison
                try:
                    if 'date_of_incorporation' in existing_df.columns:
                        existing_df['date_of_incorporation'] = pd.to_datetime(existing_df['date_of_incorporation'])
                    
                    existing_df['create_date'] = pd.to_datetime(existing_df['create_date'])
                    existing_df['last_update_date'] = pd.to_datetime(existing_df['last_update_date'])
                    
                    # Ensure datetime format for incoming data
                    df_modified['create_date'] = pd.to_datetime(df_modified['create_date'])
                    df_modified['last_update_date'] = pd.to_datetime(df_modified['last_update_date'])
                except Exception as e:
                    print(f"Warning: Error converting date columns: {e}")
                
                # Get a list of all customer_ids in both datasets
                try:
                    csv_customer_ids = set(df_modified['customer_id'].dropna().astype(int).tolist())
                    db_customer_ids = set(existing_df['customer_id'].dropna().astype(int).tolist()) if not existing_df.empty else set()
                except Exception as e:
                    print(f"Warning: Error processing customer IDs: {e}")
                    # Fallback to string comparison if numeric conversion fails
                    csv_customer_ids = set(df_modified['customer_id'].dropna().astype(str).tolist())
                    db_customer_ids = set(existing_df['customer_id'].dropna().astype(str).tolist()) if not existing_df.empty else set()
                
                # Find new records (in CSV but not in DB)
                new_customer_ids = csv_customer_ids - db_customer_ids
                new_records_df = df_modified[df_modified['customer_id'].isin(new_customer_ids)]
                print(f"Found {len(new_records_df)} new records to insert.")
                
                # Find common customer IDs (potential updates)
                common_customer_ids = csv_customer_ids.intersection(db_customer_ids)
                print(f"Found {len(common_customer_ids)} customer IDs that exist in both CSV and database.")
                
                # For common records, determine if an update is needed
                records_to_update = []
                if force_update:
                    # If force_update is True, update all common records
                    records_to_update = list(common_customer_ids)
                    print(f"Force update enabled. All {len(records_to_update)} common records will be updated.")
                else:
                    # Only update records with newer timestamps
                    for cid in common_customer_ids:
                        try:
                            csv_record = df_modified[df_modified['customer_id'] == cid].iloc[0]
                            db_record = existing_df[existing_df['customer_id'] == cid].iloc[0]
                            
                            if csv_record['last_update_date'] > db_record['last_update_date']:
                                records_to_update.append(cid)
                        except Exception as e:
                            print(f"Warning: Error comparing records for customer_id {cid}: {e}")
                    
                    print(f"Found {len(records_to_update)} records that need updating based on timestamp comparison.")
                
                # Connect and perform the inserts and updates
                with engine.connect() as conn:
                    conn.execution_options(autocommit=False)
                    transaction = conn.begin()
                    try:
                        # Insert new records
                        if not new_records_df.empty:
                            new_records_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_map)
                            print(f"Successfully inserted {len(new_records_df)} new records.")
                        
                        # Update records with newer timestamps
                        if records_to_update:
                            updates_df = df_modified[df_modified['customer_id'].isin(records_to_update)]
                            print(f"Updating {len(updates_df)} records...")
                            
                            # Process in batches
                            batch_size = 1000
                            for i in range(0, len(records_to_update), batch_size):
                                batch = list(records_to_update)[i:i+batch_size]
                                # Using parameterized query with executemany for safety
                                delete_query = text(f"DELETE FROM {table_name} WHERE customer_id = :customer_id")
                                conn.execute(delete_query, [{"customer_id": cid} for cid in batch])
                                print(f"Deleted batch {i//batch_size + 1} ({len(batch)} records) for update.")
                            
                            # Insert the updated records
                            updates_df.to_sql(table_name, engine, if_exists='append', index=False, dtype=dtype_map)
                            print(f"Inserted {len(updates_df)} updated records.")
                        
                        transaction.commit()
                        print("Transaction committed successfully.")
                    except Exception as e:
                        transaction.rollback()
                        print(f"Error occurred during update: {e}")
                        print("Transaction rolled back.")
                        raise
        
        # Count records in the table
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            count = result.fetchone()[0]
            print(f"Total records in table '{table_name}': {count}")

    except Exception as e:
        print(f"An error occurred during database update: {e}")
        raise

def main():
    """Main function to process entity data files."""
    parser = argparse.ArgumentParser(description='Process entity data CSV files and update the database.')
    parser.add_argument('--input-dir', default=r"C:\Users\anany\OneDrive\Desktop\Entity_Data_load\Entity_input",
                        help='Directory containing CSV files (default: %(default)s)')
    parser.add_argument('--pattern', default="*.CSV", help='Pattern to match CSV files (default: %(default)s)')
    parser.add_argument('--force-update', action='store_true', 
                        help='Force update all records even if timestamps are older')
    parser.add_argument('--file', help='Process a specific file instead of all files')
    args = parser.parse_args()
    
    input_directory = args.input_dir
    
    # Get all CSV files in the directory
    if args.file:
        # Process a specific file
        file_path = os.path.join(input_directory, args.file)
        if os.path.exists(file_path):
            csv_files = [file_path]
        else:
            print(f"Error: File {file_path} does not exist.")
            return
    else:
        # Get all CSV files matching the pattern
        csv_files = glob.glob(os.path.join(input_directory, args.pattern))
        # Add case-insensitive match if needed
        if args.pattern.upper() != args.pattern.lower():
            csv_files.extend(glob.glob(os.path.join(input_directory, args.pattern.upper())))
            csv_files.extend(glob.glob(os.path.join(input_directory, args.pattern.lower())))
    
    # Remove duplicates from the file list
    csv_files = list(set(csv_files))
    
    if not csv_files:
        print(f"No CSV files found in {input_directory} matching pattern {args.pattern}")
        return
    
    print(f"\n===== Entity Data Processing =====")
    print(f"Found {len(csv_files)} CSV files to process:")
    for file in csv_files:
        print(f"  - {os.path.basename(file)}")
    
    # Process and merge all CSV files
    merged_df = merge_multiple_csv_files(csv_files)
    
    if merged_df is None or merged_df.empty:
        print("No valid data to process. Exiting.")
        return
    
    print(f"\n===== Database Update =====")
    print(f"Force update mode: {'ENABLED' if args.force_update else 'DISABLED'}")
    
    # Setup database connection
    engine = create_engine(DB_URL)
    dtype_map = get_dtype_map()
    
    # Update the database with the merged data
    update_database(merged_df, engine, TABLE_NAME, dtype_map, force_update=args.force_update)
    print("\n===== Processing Complete =====")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Critical error: {e}")
        import traceback
        traceback.print_exc()