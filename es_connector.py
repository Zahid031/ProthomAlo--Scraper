# In file: es_connector.py

from elasticsearch import Elasticsearch

# --- Configuration with your Credentials ---
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "JvQhvZYl"

# --- Client Connection Function ---
def get_es_client():
    """
    Creates and returns a client connection to a secure Elasticsearch instance.
    """
    print("Attempting to connect to Elasticsearch...")
    try:
        # Create the client instance with authentication and SSL settings
        client = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASSWORD),
            # For local development with a self-signed cert from Docker,
            # we disable SSL certificate verification.
            # In a production environment, you would provide the CA certificate instead.
            verify_certs=False
        )

        # Check if the connection is successful by pinging the server
        if client.ping():
            print("Successfully connected to Elasticsearch!")
            return client
        else:
            print("Could not connect to Elasticsearch. The ping failed.")
            return None
    except Exception as e:
        print(f"An error occurred while connecting to Elasticsearch: {e}")
        return None

# --- Main block to test the connection ---
if __name__ == "__main__":
    # Get the client connection
    es = get_es_client()

    # If the connection was successful, 'es' will be a client object
    if es:
        print("\nConnection test complete. You are ready to interact with your database.")
        # You can get more info to be sure
        print("\nCluster Information:")
        print(es.info())
    else:
        print("\nConnection test failed. Please check your Docker container and credentials.")