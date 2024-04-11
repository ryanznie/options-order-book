import azure.functions as func
import logging
import pyodbc
import os
import requests 

# Function to call API and store data in Azure SQL Database
def call_api_and_store_data():
    # Replace 'https://api.example.com/data' with the actual API endpoint
    api_url = 'https://api.example.com/data'

    try:
        # Make a GET request to the API
        response = requests.get(api_url)
        response.raise_for_status()  # Raise an exception for HTTP errors

        # Extract data from the API response (assuming it returns JSON data)
        data = response.json()

        # Connect to Azure SQL Database
        connection_string = os.environ["AzureSQLConnectionString"]
        conn = pyodbc.connect(connection_string)

        # Insert data into the database
        cursor = conn.cursor()

        for item in data:
            # Assuming your table has 'name' and 'value' columns
            cursor.execute("INSERT INTO YourTableName (name, value) VALUES (?, ?)", item['name'], item['value'])
        
        conn.commit()
        conn.close()

        return True, "Data inserted successfully into Azure SQL Database."
    
    except Exception as e:
        return False, f"Error: {str(e)}"


# HTTP trigger function
def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    # Call the function to fetch data from API and store in Azure SQL Database
    success, message = call_api_and_store_data()

    if success:
        return func.HttpResponse(message, status_code=200)
    else:
        return func.HttpResponse(message, status_code=500)
