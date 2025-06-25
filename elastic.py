from elasticsearch import Elasticsearch

def get_es_client():
    return Elasticsearch(
        "http://localhost:9200",
        basic_auth=("elastic", "JvQhvZYl"),  # use your actual credentials
        verify_certs=False
    )
