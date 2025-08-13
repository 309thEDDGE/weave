from azure.storage.blob import BlobClient, BlobServiceClient


conn_str = "DefaultEndpointsProtocol=https;AccountName=weavetest;AccountKey=ZAdtwybngBHU10LK30m9WSZKLacA0VbOHe64af6FoD1Be8a7AmwSltRVv/OTOaXG/nDZ71R8Pq7Z+AStn5tQew==;EndpointSuffix=core.windows.net"
service_client = BlobServiceClient.from_connection_string(conn_str)

# List containers
for container in service_client.list_containers():
    print(container['name'])

con_client = service_client.get_container_client("index")
# container_client = service_client.create_container("index")
con_client.delete_container()