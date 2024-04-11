import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import openbb
from datetime import datetime
import io
import csv

app = func.FunctionApp()

@app.schedule(schedule="*/2 * * * *", arg_name="myTimer", run_on_startup=True,
              use_monitor=False)  #0 30 9,11,13,15 * * MON-FRI
def timer_trigger(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info('The timer is past due!')


    logging.info('Python timer trigger function executed.')

    msft_price = "101"#openbb.stocks.load("MSFT")

    # Create a BlobServiceClient using the connection string
    blob_service_client = BlobServiceClient.from_connection_string()
    
    # Specify the container and blob names
    container_name = "0dte-options"
    blob_name = "msft_price.csv"

    # Get a reference to the container
    container_client = blob_service_client.get_container_client(container_name)

    # Create the container if it doesn't exist
    if not container_client.exists():
        container_client.create_container()

    blob_client = container_client.get_blob_client(blob_name)

    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Create a CSV writer and StringIO buffer
    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)

    # Write the stock price data to the CSV buffer
    csv_writer.writerow([current_time, msft_price])

    # Upload the CSV buffer to the blob
    blob_client.upload_blob(csv_buffer.getvalue(), overwrite=True)

    logging.info(f"Microsoft stock price stored: {msft_price}")

    return