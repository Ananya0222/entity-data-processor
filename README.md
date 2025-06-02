A robust Python application for processing entity/corporation CSV data files and synchronizing them with a PostgreSQL database. The tool handles data validation, deduplication, type conversion, and intelligent upsert operations.
Features

Multi-file CSV Processing: Process single files or batch process multiple CSV files
Data Validation & Cleaning: Automatic data type conversion, date parsing, and text normalization
Intelligent Deduplication: Handles duplicates within files and across multiple files
Smart Database Sync: Compares timestamps to determine if records need updating
Force Update Mode: Option to override timestamp-based updates
Error Handling: Comprehensive error handling with detailed logging
Flexible Configuration: Command-line arguments for different processing scenarios

Prerequisites

Python 3.7+
PostgreSQL database
Required Python packages (see requirements.txt)# entity-data-processor.
How It Works

File Discovery: Scans the input directory for CSV files matching the specified pattern
Data Processing: For each file:

Loads CSV data with proper encoding
Converts data types (numeric, date, text)
Normalizes text to uppercase
Handles missing values

Deduplication:

Removes duplicates within each file (keeps latest by last_update_date)
Resolves duplicates across multiple files

Database Sync:

Creates table if it doesn't exist
Identifies new records vs. existing records
Updates only records with newer timestamps (unless force-update is enabled)
Uses transactions for data integrity

Data Processing Features

Automatic Type Conversion: Numeric columns are converted to appropriate integer types
Date Handling: Multiple date formats are supported with automatic parsing
Text Normalization: All text is converted to uppercase for consistency
Missing Data: Automatically fills missing dates with current timestamp
Encoding Support: Handles various CSV encodings (default: latin1)

Error Handling
The script includes comprehensive error handling for:

File reading errors
Data type conversion issues
Database connection problems
Transaction failures with automatic rollback
Invalid data formats

Logging
The script provides detailed console output including:

File processing status
Record counts at each stage
Duplicate detection results
Database update summaries
Error messages with context
