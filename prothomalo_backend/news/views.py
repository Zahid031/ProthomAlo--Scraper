from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from elasticsearch import Elasticsearch
from django.conf import settings

class NewsListAPIView(APIView):
    def get(self, request):
        es = Elasticsearch(
            hosts=[settings.ES_HOST],
            basic_auth=(settings.ES_USER, settings.ES_PASSWORD),
            verify_certs=False,
        )
        try:
            res = es.search(
                index="prothomalo_politics",
                body={
                    "size": 20,
                    "sort": [{"published_at": {"order": "desc"}}],
                    "query": {"match_all": {}}
                }
            )
            hits = res['hits']['hits']
            articles = [hit['_source'] for hit in hits]
            return Response(articles)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
