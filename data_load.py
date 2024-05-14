import snowflake.connector
import pandas as pd
import logging

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        logging.info("Creating or replacing the 'CLAIMS' table...")
        # Connect to Snowflake
        conn = snowflake.connector.connect(
            user ="DCEE118",
            password ="Ichigo-21",
            account ="CSNKJOE-IO72426",
            warehouse ="COMPUTE_WH",
            database ="HASTINGS_DIRECT",
            schema="PUBLIC"
        )
        # CREATE TABLE statement to create or replace the 'CLAIMS' table
        cursor = conn.cursor()
        cursor.execute("""
            CREATE OR REPLACE TABLE CLAIMS (
                NAME VARCHAR(16777216),
                CLAIM_NUMBER VARCHAR(16777216),
                DATE DATE,
                AMOUNT NUMBER(38,2),
                ID NUMBER(38,0)
            )
        """)
        conn.commit()
        logging.info("Data table 'CLAIMS' has been created or replaced successfully.")
        logging.info("Reading the CSV file...")
        
        # Read the CSV file
        df = pd.read_csv('data-load.csv')
        logging.info("Cleaning the data...")
       
        # Clean the data
        df.columns = df.columns.str.strip()  # Remove whitespace from column 
        df['Name'] = df['Name'].str.replace(r'[^\w\s]', '', regex=True).str.strip()  # Remove symbols and whitespace from the Name column
        df['Date'] = pd.to_datetime(df['Date'], dayfirst=True).dt.strftime('%Y-%m-%d')  # Consistent date format
        df['ClaimNumber'] = df['ClaimNumber'].astype(str).str.replace(r'⅘', '', regex=True) # Remove non-numeric characters from ClaimNumber
        df['ID'] = df['ID'].astype(str).str.replace(r'\D', '', regex=True)  # Remove non-numeric characters from ID
        df['Amount'] = df['Amount'].astype(float).round(2)  # Convert Amount to float and round to two decimal place
        df = df.dropna(axis=1, how='all')
        
        #insert data 
        logging.info("Inserting data into the table...")
        for index, row in df.iterrows():
            cursor.execute("""
                INSERT INTO CLAIMS (NAME, CLAIM_NUMBER, DATE, AMOUNT, ID)
                VALUES (%s, %s, %s, %s, %s)
            """, (row['Name'], row['ClaimNumber'], row['Date'], row['Amount'], row['ID']))
      
        conn.commit()
        logging.info("Closing the cursor and connection...")

        cursor.close()
        conn.close()
        
        logging.info("Data has been loaded into the table 'CLAIMS' on Snowflake successfully.")
    
    except Exception as e:
        logging.error("An error occurred: %s", e)
    
main()