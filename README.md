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
