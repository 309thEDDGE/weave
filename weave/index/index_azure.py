from azure.storage.blob import BlobClient, BlobServiceClient
import os


conn_str = os.environ["AZURE_CONNECTION_STRING"]
service_client = BlobServiceClient.from_connection_string(conn_str)

# List containers
for container in service_client.list_containers():
    print(container['name'])

con_client = service_client.get_container_client("index")
# container_client = service_client.create_container("index")
con_client.delete_container()