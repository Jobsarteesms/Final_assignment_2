# Final_assignment_2
Optimizing Data Management with a Data Migration and Transformation Solution for HIVE Data Warehouses
# JSON Data Pipeline

This project downloads a zip file, extracts JSON files, parses the data, stores it in HDFS, and allows querying through HIVE.

# Setup

1. Install dependencies: pip install -r requirements.txt

2. Run the main script:python main.py

3. Optional: Setup HDFS directories (if not already done):sh hdfs_setup.sh

4. Query data in HIVE: hive -f query_hive.sql

# Replace your-hdfs-hostname, your-hdfs-username, and your_hive_table_name with your actual HDFS and Hive configuration.
