#!/usr/bin/env python3
"""
Elasticsearch Data Explorer for Prothom Alo Articles

This script provides various ways to query and explore your scraped news data
directly from Elasticsearch, plus setup instructions for Kibana visualization.
"""

import json
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from typing import Dict, List, Any, Optional
import pandas as pd

class ElasticsearchDataExplorer:
    """Class to explore and query scraped Prothom Alo articles."""
    
    def __init__(self):
        self.es_host = "http://localhost:9200"
        self.es_user = "elastic"
        self.es_password = "JvQhvZYl"
        self.index_name = "prothomalo_politics"
        self.es_client = None
        
    def connect(self):
        """Connect to Elasticsearch."""
        try:
            self.es_client = Elasticsearch(
                hosts=[self.es_host],
                basic_auth=(self.es_user, self.es_password),
                verify_certs=False
            )
            
            if self.es_client.ping():
                print("✅ Connected to Elasticsearch successfully!")
                return True
            else:
                print("❌ Failed to ping Elasticsearch")
                return False
        except Exception as e:
            print(f"❌ Connection error: {e}")
            return False
    
    def get_index_info(self):
        """Get basic information about the index."""
        try:
            # Check if index exists
            if not self.es_client.indices.exists(index=self.index_name):
                print(f"❌ Index '{self.index_name}' does not exist!")
                return
            
            # Get index stats
            stats = self.es_client.indices.stats(index=self.index_name)
            doc_count = stats['_all']['total']['docs']['count']
            size_in_bytes = stats['_all']['total']['store']['size_in_bytes']
            
            print(f"\n📊 INDEX INFORMATION")
            print(f"=" * 40)
            print(f"Index Name: {self.index_name}")
            print(f"Total Documents: {doc_count:,}")
            print(f"Index Size: {size_in_bytes / (1024*1024):.2f} MB")
            
            # Get mapping info
            mapping = self.es_client.indices.get_mapping(index=self.index_name)
            fields = mapping[self.index_name]['mappings']['properties'].keys()
            print(f"Fields: {', '.join(fields)}")
            
        except Exception as e:
            print(f"❌ Error getting index info: {e}")
    
    def search_all_articles(self, size=10):
        """Get all articles with basic information."""
        try:
            query = {
                "query": {"match_all": {}},
                "size": size,
                "sort": [{"publication_date": {"order": "desc"}}]
            }
            
            result = self.es_client.search(index=self.index_name, body=query)
            
            print(f"\n📰 RECENT ARTICLES (Top {size})")
            print("=" * 60)
            
            for i, hit in enumerate(result['hits']['hits'], 1):
                source = hit['_source']
                print(f"\n{i}. {source.get('headline', 'No headline')}")
                print(f"   Author: {source.get('author', 'Unknown')}")
                print(f"   Date: {source.get('publication_date', 'Unknown')}")
                print(f"   Location: {source.get('location', 'Unknown')}")
                print(f"   Word Count: {source.get('word_count', 0)}")
                print(f"   URL: {source.get('url', '')}")
            
            return result['hits']['hits']
            
        except Exception as e:
            print(f"❌ Error searching articles: {e}")
            return []
    
    def search_by_keyword(self, keyword, size=5):
        """Search articles by keyword in headline or content."""
        try:
            query = {
                "query": {
                    "multi_match": {
                        "query": keyword,
                        "fields": ["headline^2", "content"],  # Boost headline matches
                        "type": "best_fields"
                    }
                },
                "size": size,
                "highlight": {
                    "fields": {
                        "headline": {},
                        "content": {"fragment_size": 150, "number_of_fragments": 2}
                    }
                }
            }
            
            result = self.es_client.search(index=self.index_name, body=query)
            
            print(f"\n🔍 SEARCH RESULTS for '{keyword}' (Top {size})")
            print("=" * 60)
            
            for i, hit in enumerate(result['hits']['hits'], 1):
                source = hit['_source']
                print(f"\n{i}. {source.get('headline', 'No headline')}")
                print(f"   Score: {hit['_score']:.2f}")
                print(f"   Date: {source.get('publication_date', 'Unknown')}")
                
                # Show highlighted snippets
                if 'highlight' in hit:
                    if 'headline' in hit['highlight']:
                        print(f"   📍 Highlighted Title: {hit['highlight']['headline'][0]}")
                    if 'content' in hit['highlight']:
                        for snippet in hit['highlight']['content']:
                            print(f"   📍 Snippet: {snippet}")
            
            return result['hits']['hits']
            
        except Exception as e:
            print(f"❌ Error searching by keyword: {e}")
            return []
    
    def get_articles_by_author(self, author_name, size=10):
        """Get articles by specific author."""
        try:
            query = {
                "query": {
                    "match": {
                        "author": author_name
                    }
                },
                "size": size,
                "sort": [{"publication_date": {"order": "desc"}}]
            }
            
            result = self.es_client.search(index=self.index_name, body=query)
            
            print(f"\n👤 ARTICLES BY '{author_name}' (Top {size})")
            print("=" * 60)
            
            for i, hit in enumerate(result['hits']['hits'], 1):
                source = hit['_source']
                print(f"{i}. {source.get('headline', 'No headline')}")
                print(f"   Date: {source.get('publication_date', 'Unknown')}")
                print(f"   Words: {source.get('word_count', 0)}")
            
            return result['hits']['hits']
            
        except Exception as e:
            print(f"❌ Error searching by author: {e}")
            return []
    
    def get_articles_by_date_range(self, start_date, end_date, size=10):
        """Get articles within a date range."""
        try:
            query = {
                "query": {
                    "range": {
                        "publication_date": {
                            "gte": start_date,
                            "lte": end_date
                        }
                    }
                },
                "size": size,
                "sort": [{"publication_date": {"order": "desc"}}]
            }
            
            result = self.es_client.search(index=self.index_name, body=query)
            
            print(f"\n📅 ARTICLES FROM {start_date} TO {end_date} (Top {size})")
            print("=" * 60)
            
            for i, hit in enumerate(result['hits']['hits'], 1):
                source = hit['_source']
                print(f"{i}. {source.get('headline', 'No headline')}")
                print(f"   Date: {source.get('publication_date', 'Unknown')}")
                print(f"   Author: {source.get('author', 'Unknown')}")
            
            return result['hits']['hits']
            
        except Exception as e:
            print(f"❌ Error searching by date range: {e}")
            return []
    
    def get_analytics(self):
        """Get analytics and statistics about the data."""
        try:
            # Total articles
            total_query = {"query": {"match_all": {}}}
            total_result = self.es_client.search(index=self.index_name, body=total_query, size=0)
            total_articles = total_result['hits']['total']['value']
            
            # Top authors
            authors_agg = {
                "size": 0,
                "aggs": {
                    "top_authors": {
                        "terms": {
                            "field": "author.keyword",
                            "size": 10
                        }
                    }
                }
            }
            authors_result = self.es_client.search(index=self.index_name, body=authors_agg)
            
            # Articles by date
            date_agg = {
                "size": 0,
                "aggs": {
                    "articles_by_date": {
                        "date_histogram": {
                            "field": "publication_date",
                            "calendar_interval": "day"
                        }
                    }
                }
            }
            date_result = self.es_client.search(index=self.index_name, body=date_agg)
            
            # Word count statistics
            stats_agg = {
                "size": 0,
                "aggs": {
                    "word_count_stats": {
                        "stats": {
                            "field": "word_count"
                        }
                    }
                }
            }
            stats_result = self.es_client.search(index=self.index_name, body=stats_agg)
            
            print(f"\n📈 DATA ANALYTICS")
            print("=" * 40)
            print(f"Total Articles: {total_articles:,}")
            
            # Word count stats
            word_stats = stats_result['aggregations']['word_count_stats']
            print(f"\nWord Count Statistics:")
            print(f"  Average: {word_stats['avg']:.0f} words")
            print(f"  Min: {word_stats['min']:.0f} words")
            print(f"  Max: {word_stats['max']:.0f} words")
            
            # Top authors
            print(f"\nTop Authors:")
            for i, bucket in enumerate(authors_result['aggregations']['top_authors']['buckets'][:5], 1):
                print(f"  {i}. {bucket['key']}: {bucket['doc_count']} articles")
            
            # Recent activity
            print(f"\nRecent Publishing Activity:")
            for bucket in date_result['aggregations']['articles_by_date']['buckets'][-7:]:
                date_str = bucket['key_as_string'][:10]  # Just the date part
                print(f"  {date_str}: {bucket['doc_count']} articles")
            
        except Exception as e:
            print(f"❌ Error getting analytics: {e}")
    
    def export_to_csv(self, filename="prothomalo_articles.csv", max_docs=1000):
        """Export articles to CSV for external analysis."""
        try:
            query = {
                "query": {"match_all": {}},
                "size": max_docs,
                "sort": [{"publication_date": {"order": "desc"}}]
            }
            
            result = self.es_client.search(index=self.index_name, body=query)
            
            # Convert to list of dictionaries
            articles = []
            for hit in result['hits']['hits']:
                article = hit['_source']
                articles.append({
                    'headline': article.get('headline', ''),
                    'author': article.get('author', ''),
                    'location': article.get('location', ''),
                    'publication_date': article.get('publication_date', ''),
                    'word_count': article.get('word_count', 0),
                    'url': article.get('url', ''),
                    'content_preview': article.get('content', '')[:200] + '...' if article.get('content') else ''
                })
            
            # Create DataFrame and save
            df = pd.DataFrame(articles)
            df.to_csv(filename, index=False, encoding='utf-8')
            
            print(f"✅ Exported {len(articles)} articles to {filename}")
            return filename
            
        except Exception as e:
            print(f"❌ Error exporting to CSV: {e}")
            return None

def main():
    """Main function to demonstrate data exploration."""
    explorer = ElasticsearchDataExplorer()
    
    # Connect to Elasticsearch
    if not explorer.connect():
        return
    
    print("🚀 PROTHOM ALO DATA EXPLORER")
    print("=" * 50)
    
    # Get basic index information
    explorer.get_index_info()
    
    # Show recent articles
    explorer.search_all_articles(size=5)
    
    # Get analytics
    explorer.get_analytics()
    
    # Example searches
    print("\n" + "="*60)
    print("EXAMPLE SEARCHES")
    print("="*60)
    
    # Search by keyword (you can change this)
    explorer.search_by_keyword("নির্বাচন", size=3)  # "Election" in Bengali
    
    # Date range search (last 7 days)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    explorer.get_articles_by_date_range(start_date, end_date, size=3)
    
    # Export to CSV
    print("\n" + "="*60)
    print("EXPORTING DATA")
    print("="*60)
    explorer.export_to_csv("prothomalo_export.csv", max_docs=100)

if __name__ == "__main__":
    main()

# ===================================================================
# KIBANA SETUP INSTRUCTIONS
# ===================================================================

"""
📊 KIBANA SETUP GUIDE

1. INSTALL KIBANA WITH DOCKER:
   ```bash
   docker run -d --name kibana --link elasticsearch:elasticsearch -p 5601:5601 kibana:7.17.0
   ```

2. ACCESS KIBANA:
   - Open browser: http://localhost:5601
   - Username: elastic
   - Password: JvQhvZYl

3. CREATE INDEX PATTERN:
   - Go to Stack Management → Index Patterns
   - Click "Create index pattern"
   - Pattern: prothomalo_politics
   - Time field: publication_date
   - Click "Create index pattern"

4. EXPLORE DATA:
   - Go to Discover tab
   - Select your index pattern
   - You can now search, filter, and explore your articles

5. CREATE VISUALIZATIONS:
   
   A) ARTICLES BY DATE (Line Chart):
   - Go to Visualize → Create visualization → Line
   - X-axis: Date Histogram on publication_date (Daily)
   - Y-axis: Count
   
   B) TOP AUTHORS (Pie Chart):
   - Go to Visualize → Create visualization → Pie
   - Split slices: Terms on author.keyword (Top 10)
   
   C) WORD COUNT DISTRIBUTION (Histogram):
   - Go to Visualize → Create visualization → Histogram
   - X-axis: Histogram on word_count (Interval: 100)
   - Y-axis: Count
   
   D) ARTICLES BY LOCATION (Bar Chart):
   - Go to Visualize → Create visualization → Vertical Bar
   - X-axis: Terms on location.keyword
   - Y-axis: Count

6. CREATE DASHBOARD:
   - Go to Dashboard → Create new dashboard
   - Add all your visualizations
   - Save the dashboard

7. USEFUL KIBANA QUERIES:
   - Search for specific terms: headline:"নির্বাচন"
   - Filter by author: author:"Author Name"
   - Date range: publication_date:[2025-06-01 TO 2025-06-30]
   - Word count filter: word_count:[500 TO 1000]

8. ADVANCED FEATURES:
   - Set up alerts for new articles
   - Create saved searches
   - Use Kibana's machine learning features
   - Set up dashboards for different time periods
"""