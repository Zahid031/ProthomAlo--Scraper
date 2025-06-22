# In file: create_es_index.py

from elasticsearch import Elasticsearch

# --- Configuration with your Credentials ---
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "JvQhvZYl"
INDEX_NAME = "prothomalo_politics"

def create_index_with_mapping():
    """Connects to ES and creates an index with a specific mapping."""
    try:
        # Connect securely
        print("Connecting to Elasticsearch to create index...")
        es_client = Elasticsearch(
            hosts=[ES_HOST],
            basic_auth=(ES_USER, ES_PASSWORD),
            verify_certs=False  # For local Docker self-signed cert
        )

        # Check if the index already exists
        if es_client.indices.exists(index=INDEX_NAME):
            print(f"Index '{INDEX_NAME}' already exists. No action needed.")
            return

        print(f"Creating index '{INDEX_NAME}' with custom mapping...")
        
        # Define the structure for our articles
        index_body = {
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "url": {"type": "keyword"},
                    "headline": {"type": "text", "analyzer": "standard"},
                    "author": {"type": "text"},
                    "location": {"type": "keyword"},
                    "publication_date": {"type": "date", "format": "yyyy-MM-dd"},
                    "content": {"type": "text", "analyzer": "standard"}
                }
            }
        }
        
        # Create the index
        es_client.indices.create(index=INDEX_NAME, body=index_body)
        print("Index created successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    create_index_with_mapping()