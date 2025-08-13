from azure.storage.blob import BlobClient, BlobServiceClient
import os
from dotenv import load_dotenv

load_dotenv()

acc_url = os.getenv("AZURE_ACCOUNT_URL")
acc_key = os.getenv("AZURE_ACCOUNT_KEY")
service_client = BlobServiceClient(account_url=acc_url, credential=acc_key)

# List containers
for container in service_client.list_containers():
    print(container['name'])

# con_client = service_client.get_container_client("index")
container_client = service_client.create_container("index")

container_client.delete_container()