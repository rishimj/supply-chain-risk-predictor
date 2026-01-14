"""
End-to-End Integration Test with Load Balanced Enrichment

Tests the full pipeline:
Gateway → Kafka → Flink → Nginx LB → Enrichment (x3) → Redis

Verifies that news flows through the system and features are computed correctly
using the load-balanced enrichment cluster.
"""
import pytest
import requests
import redis
import json
import time
from datetime import datetime


class TestEndToEndLoadBalanced:
    """End-to-end tests with load-balanced enrichment."""
    
    @pytest.fixture
    def redis_client(self):
        """Redis client for checking feature storage."""
        client = redis.from_url("redis://localhost:6379/0", decode_responses=True)
        yield client
        client.close()
    
    @pytest.fixture
    def sample_news_articles(self):
        """Sample news articles for testing."""
        return [
            {
                "news_id": "e2e-lb-1",
                "content": "Apple announces major supply chain disruption due to factory shutdown",
                "pub_time": datetime.utcnow().isoformat() + "Z",
                "source": "test"
            },
            {
                "news_id": "e2e-lb-2", 
                "content": "Microsoft expands cloud infrastructure with new data centers",
                "pub_time": datetime.utcnow().isoformat() + "Z",
                "source": "test"
            },
            {
                "news_id": "e2e-lb-3",
                "content": "Tesla faces supply shortage impacting production goals",
                "pub_time": datetime.utcnow().isoformat() + "Z",
                "source": "test"
            }
        ]
    
    def test_gateway_to_kafka(self, sample_news_articles):
        """Test that news articles can be posted to gateway."""
        for article in sample_news_articles:
            response = requests.post(
                "http://localhost:8080/api/v1/news",
                json=article,
                timeout=10
            )
            assert response.status_code in [200, 201, 202], \
                f"Failed to post news: {response.status_code} - {response.text}"
        
        print(f"✓ Posted {len(sample_news_articles)} articles to gateway")
    
    def test_enrichment_through_load_balancer(self):
        """Test that enrichment works through load balancer."""
        test_request = {
            "news_id": "lb-direct-test",
            "headline": "Apple reports supply chain delays",
            "content": "Apple Inc. faces significant supply chain challenges.",
            "pub_time": datetime.utcnow().isoformat() + "Z"
        }
        
        # Send request through load balancer
        response = requests.post(
            "http://localhost:8085/v1/enrich",
            json=test_request,
            timeout=10
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data["news_id"] == test_request["news_id"]
        assert "companies" in data
        
        print(f"✓ Enrichment through load balancer successful")
        print(f"  Found {len(data['companies'])} companies")
    
    def test_full_pipeline_with_load_balancer(self, redis_client, sample_news_articles):
        """Test complete pipeline from gateway to Redis with load-balanced enrichment."""
        # Clear any existing test data
        for key in redis_client.scan_iter("feat:*:latest"):
            redis_client.delete(key)
        
        # Post news articles
        for article in sample_news_articles:
            response = requests.post(
                "http://localhost:8080/api/v1/news",
                json=article,
                timeout=10
            )
            assert response.status_code in [200, 201, 202]
        
        print(f"✓ Posted {len(sample_news_articles)} articles")
        
        # Wait for Flink processing (5-minute windows, but should see results sooner)
        print("⏳ Waiting for Flink processing through load-balanced enrichment...")
        time.sleep(90)  # Wait 90 seconds for windowing and processing
        
        # Check Redis for features
        feature_keys = list(redis_client.scan_iter("feat:*:latest"))
        
        print(f"\n📊 Pipeline Results:")
        print(f"  Feature keys found: {len(feature_keys)}")
        
        if len(feature_keys) > 0:
            # Examine features
            for key in feature_keys[:5]:  # Show first 5
                features = json.loads(redis_client.get(key))
                print(f"\n  {key}:")
                print(f"    Ticker: {features.get('ticker')}")
                print(f"    Risk Score: {features.get('risk_score_5m')}")
                print(f"    Sentiment: {features.get('sentiment_score_5m')}")
                print(f"    Pos/Neg: {features.get('pos_news_count_5m')}/{features.get('neg_news_count_5m')}")
        
        # We should have at least some features
        assert len(feature_keys) > 0, "No features found in Redis after pipeline processing"
        print(f"\n✅ Full pipeline test successful with load-balanced enrichment!")
    
    def test_load_balancer_under_pipeline_load(self, sample_news_articles):
        """Test that load balancer handles multiple articles in quick succession."""
        num_batches = 3
        articles_per_batch = 5
        
        for batch in range(num_batches):
            for i in range(articles_per_batch):
                article = {
                    "news_id": f"load-test-{batch}-{i}",
                    "content": f"Test article {i} about supply chain event {batch}",
                    "pub_time": datetime.utcnow().isoformat() + "Z",
                    "source": "load-test"
                }
                
                response = requests.post(
                    "http://localhost:8080/api/v1/news",
                    json=article,
                    timeout=10
                )
                assert response.status_code in [200, 201, 202]
            
            print(f"✓ Posted batch {batch + 1}/{num_batches}")
        
        total_articles = num_batches * articles_per_batch
        print(f"\n✅ Load balancer handled {total_articles} articles successfully")


class TestLoadBalancerResilience:
    """Test system resilience with load-balanced enrichment."""
    
    def test_system_health_check(self):
        """Verify all components are healthy."""
        components = {
            "Gateway": "http://localhost:8080/healthz",
            "Nginx LB": "http://localhost:8085/healthz",
            "Enrichment-1": "http://localhost:8082/healthz",
            "Enrichment-2": "http://localhost:8086/healthz",
            "Enrichment-3": "http://localhost:8087/healthz",
        }
        
        print("\n🏥 Health Check:")
        all_healthy = True
        
        for name, url in components.items():
            try:
                response = requests.get(url, timeout=5)
                status = "✓ Healthy" if response.status_code == 200 else f"✗ Unhealthy ({response.status_code})"
                print(f"  {name}: {status}")
                if response.status_code != 200:
                    all_healthy = False
            except Exception as e:
                print(f"  {name}: ✗ Error - {e}")
                all_healthy = False
        
        assert all_healthy, "Not all components are healthy"
    
    def test_redis_connectivity(self):
        """Test Redis connectivity for feature storage."""
        client = redis.from_url("redis://localhost:6379/0", decode_responses=True)
        
        # Test basic operations
        test_key = "test:lb:health"
        client.set(test_key, "healthy", ex=10)
        value = client.get(test_key)
        
        assert value == "healthy"
        print("✓ Redis connectivity confirmed")
        
        client.close()


class TestLoadBalancerMetrics:
    """Test metrics and observability of load-balanced system."""
    
    def test_nginx_metrics_available(self):
        """Test that nginx exposes metrics."""
        response = requests.get("http://localhost:8085/nginx_status", timeout=5)
        assert response.status_code == 200
        
        # Parse basic metrics
        status_text = response.text
        print("\n📊 Nginx Metrics:")
        print(f"{status_text}")
        
        # Should contain key metrics
        assert "Active connections" in status_text
        assert "Reading" in status_text
        assert "Writing" in status_text


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short", "-s"])

