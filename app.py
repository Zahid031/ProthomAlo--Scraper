from flask import Flask, jsonify, render_template
from elasticsearch import Elasticsearch
from flask_cors import CORS

app = Flask(__name__)
CORS(app)  # Enable CORS if serving frontend separately

# --- Elasticsearch Configuration ---
ES_HOST = "http://localhost:9200"
ES_USER = "elastic"
ES_PASSWORD = "JvQhvZYl"
ES_INDEX = "prothomalo_politics"

# --- Connect to Elasticsearch ---
es = Elasticsearch(
    hosts=[ES_HOST],
    basic_auth=(ES_USER, ES_PASSWORD),
    verify_certs=False
)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/api/news", methods=["GET"])
def get_news():
    query = {
        "size": 20,
        "sort": [{"published_at": "desc"}],
        "query": {
            "match_all": {}
        }
    }
    try:
        res = es.search(index=ES_INDEX, body=query)
        hits = res.get("hits", {}).get("hits", [])
        news = [hit["_source"] for hit in hits]
        return jsonify(news)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
