# db_connect.py

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# 1. Load environment variables from .env file
# This will make POSTGRES_USER, POSTGRES_PASSWORD, etc. available via os.environ
load_dotenv()

# 2. Get connection details
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")

# 3. Construct the SQLAlchemy connection URL
# Format: postgresql+psycopg2://user:password@host:port/database
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print(f"Attempting to connect to: {DB_HOST}:{DB_PORT} as user {DB_USER} to database {DB_NAME}")

# 4. Create the SQLAlchemy Engine
try:
    engine = create_engine(DATABASE_URL)
    
    # 5. Test the connection
    with engine.connect() as connection:
        # Execute a simple query to confirm connection
        result = connection.execute(text("SELECT version();"))
        
        print("\n--- Connection Successful! ---")
        for row in result:
            print(f"PostgreSQL Version: {row[0]}")
        
        # Optional: Example of creating and querying a simple table
        
        # Create a table
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                message VARCHAR(255) NOT NULL,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            );
        """))
        print("\n'test_table' ensured to exist.")
        
        # Insert data
        connection.execute(text("INSERT INTO test_table (message) VALUES (:msg)"), {"msg": "Hello from SQLAlchemy"})
        print("Data inserted.")
        
        # Select data
        select_result = connection.execute(text("SELECT id, message, created_at FROM test_table ORDER BY id DESC LIMIT 1;"))
        
        print("\n--- Latest Record ---")
        for row in select_result:
            print(f"ID: {row[0]}, Message: {row[1]}, Created At: {row[2]}")
            
        connection.commit() # Commit all changes

except Exception as e:
    print(f"\n--- ERROR: Could not connect to the database. ---")
    print(f"Details: {e}")
