**ETL Pipeline – SQL Server | MySQL | MongoDB | AWS S3 | DynamoDB | Python**

This project is a complete End-to-End ETL (Extract–Transform–Load) Pipeline built using Python.
It integrates multiple data sources — SQL Server, MySQL, and MongoDB — performs data cleaning and transformations using Pandas, and loads the prepared datasets back into target systems such as SQL Server, MySQL, and AWS services like S3 and DynamoDB.

The goal of this project is to demonstrate a production-style ETL workflow with modular code, configuration-driven connections, and scalable handling of heterogeneous data.

🚀 Features

🔹 Extraction

Extracts data from:

    SQL Server
    
    MySQL
    
    MongoDB

Config-driven database connections.

Reusable extraction functions using .ini file.

🔹 Transformation

    Data cleaning & standardization:
    
    Deduplication
    
    Type corrections
    
    Date parsing
    
    Numeric conversions (e.g., order_amount)
    
    Handling missing or inconsistent values
    
    Dataset merging & unification into a single dataframe.

🔹 Loading

Loads processed data into:

    SQL Server (with auto table creation)
    
    MySQL
    
    AWS DynamoDB
    
    AWS S3 (raw file storage)

Supports:

Prepared statements

Automatic schema creation

Insert batching

🔹 Additional Capabilities

Modular pipeline functions (Extract → Transform → Load)

Error handling with clear messages

Configurable file paths and database settings

Reusable utility functions

📁 Project Structure

    ETL-Pipeline/
    │── config/
    │   └── config.ini              # Database & AWS credentials
    │
    │── src/
    │   ├── config_loader.py        # Reads config.ini
    │   ├── extract.py              # Extract functions (SQL Server, MySQL, MongoDB)
    │   ├── transform.py            # Cleaning & transformation logic
    │   ├── load.py                 # Load into SQL Server, MySQL, DynamoDB
    │   ├── util.py                 # DB utilities, common helpers
    │   ├── main.py                 # Orchestrates full ETL pipeline
    │   ├── test.py                 # Testing individual modules
    │   └── sql_unified_data.csv    # (generated) unified cleaned dataset
    │
    └── README.md

⚙️ Tech Stack

    Layer	          Technology
    Programming	  Python (Pandas, mysql-connector, pyodbc, boto3, pymongo)
    Databases	  SQL Server, MySQL, MongoDB
    Cloud Services 	  AWS S3, AWS DynamoDB
    Configuration	  INI file-based config loader
    Tooling	          VS Code / PyCharm

**🧠 How the Pipeline Works**

1️⃣ Extract

Pulls customer & order data from:

MySQL tables

SQL Server tables

MongoDB collections

All extraction functions return Pandas DataFrames.

2️⃣ Transform

Transformations include:

Converting dates to datetime

Cleaning numeric fields (order_amount)

Removing special characters and noise

Handling nulls, duplicates

Merging extracted datasets into sql_unified_df

3️⃣ Load

The final cleaned DataFrame is loaded into:

SQL Server (create table if not exists + insert)

MySQL (create table if not exists + insert)

DynamoDB (batch_write_item)

S3 (raw + processed files)

▶️ Running the Pipeline

Step 1 — Add your config

Update config/config.ini with:

    [mysql]
    host=
    user=
    password=
    database=
    
    [sqlserver]
    server=
    user=
    password=
    database=
    
    [mongodb]
    uri=
    database=
    
    [aws]
    access_key=
    secret_key=
    bucket_name=
    region=

Step 2 — Run the pipeline

    python src/main.py

🧪 Testing Individual Components

You can test each stage independently:

    python src/test.py


The test file includes:

Connection checks

Extract tests

Transform tests

Load tests

📌 Future Enhancements

Add logging with Python logging module

Airflow orchestration

Incremental loading (CDC)

Complete unit test coverage

Docker containerization

📝 License

This project is open-source under the MIT License.