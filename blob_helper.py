from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import config
import encoding_decoding
import io
import pandas as pd
import mysql.connector

#blob code
connection_string = encoding_decoding.decrypt(config.enc_connection_string)
container_name = config.container_name
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
csv_file_name = config.master_file_name

#mysql config
host = config.host
user = config.user
password = config.password
database = config.database

def create_container(container_name):
    container_client = blob_service_client.get_container_client(container_name)
    if not container_client.exists():
        container_client.create_container()
    return "container created"

def upload_file(container_name,local_path, file_to_upload):
    blob_client = blob_service_client.get_blob_client(container_name, file_to_upload)
    try:
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)    

        return "file uploaded successfully"    
    except:
        return "Error uploading file"
    
def list_container_name():
    containers = blob_service_client.list_containers()
    for c in containers:
        print(c['name'])
    return

def list_blob_in_container(container_name):
    container_client = blob_service_client.get_container_client(container_name)
    blobs = container_client.list_blobs()
    for blob in blobs:
        print(blob.name)

def read_csv_from_blob(container_name , csv_file_name):
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(csv_file_name)
    # Download the CSV file content
    csv_data = blob_client.download_blob()
    csv_text = csv_data.readall()
    # Convert the CSV data to a Pandas DataFrame
    df = pd.read_csv(io.StringIO(csv_text.decode('utf-8')))

    return df

def get_mysql_connection():
    connection =  mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        database=database
    )
    return connection



