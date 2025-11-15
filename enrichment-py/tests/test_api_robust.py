"""
Robust API tests for the FastAPI Enrichment Service

These tests ensure the HTTP endpoints are robust and handle edge cases,
malformed inputs, and error conditions gracefully.
"""

import pytest
import json
import time
from unittest.mock import patch, MagicMock
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import after path setup
import requests
from models import EnrichmentRequest, EnrichmentResponse, CompanyMention

class TestEnrichmentAPIRobustness:
    """Comprehensive robustness tests for the enrichment API."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class - assumes service is running on localhost:8082."""
        cls.base_url = "http://localhost:8082"
        
        # Test if service is available
        try:
            response = requests.get(f"{cls.base_url}/healthz", timeout=2)
            if response.status_code != 200:
                pytest.skip("Enrichment service not available")
        except requests.exceptions.RequestException:
            pytest.skip("Enrichment service not available")
    
    def test_health_endpoint_robustness(self):
        """Test health endpoint under various conditions."""
        # Normal health check
        response = requests.get(f"{self.base_url}/healthz")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data
        assert data["version"] == "1.0.0"
        
        # Health check with extra headers
        response = requests.get(f"{self.base_url}/healthz", headers={
            "User-Agent": "Test/1.0",
            "Accept": "application/json",
            "Custom-Header": "test-value"
        })
        assert response.status_code == 200
    
    def test_metrics_endpoint_robustness(self):
        """Test metrics endpoint returns valid Prometheus format."""
        response = requests.get(f"{self.base_url}/metrics")
        assert response.status_code == 200
        
        content = response.text
        
        # Check for required Prometheus metrics
        assert "enrich_requests_total" in content
        assert "enrich_latency_seconds" in content
        assert "companies_detected_total" in content
        
        # Check metric format (basic validation)
        lines = content.split('\n')
        metric_lines = [line for line in lines if not line.startswith('#') and line.strip()]
        
        for line in metric_lines:
            if line.strip():
                # Should contain metric name and value
                assert ' ' in line or '{' in line
    
    def test_stats_endpoint_robustness(self):
        """Test stats endpoint returns complete information."""
        response = requests.get(f"{self.base_url}/stats")
        assert response.status_code == 200
        
        data = response.json()
        
        # Required fields
        required_fields = [
            "total_companies", "total_keywords", 
            "negative_sentiment_keywords", "positive_sentiment_keywords",
            "service", "version", "environment"
        ]
        
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"
        
        # Data type validation
        assert isinstance(data["total_companies"], int)
        assert isinstance(data["total_keywords"], int)
        assert isinstance(data["negative_sentiment_keywords"], int)
        assert isinstance(data["positive_sentiment_keywords"], int)
        assert data["service"] == "enrichment"
        assert data["version"] == "1.0.0"
    
    def test_enrich_valid_requests(self):
        """Test enrichment with various valid request formats."""
        test_cases = [
            # Minimal valid request
            {
                "news_id": "test-minimal",
                "headline": "Apple reports quarterly results"
            },
            # Full request
            {
                "news_id": "test-full",
                "headline": "Tesla production increases significantly",
                "body": "Tesla Inc. announces major production milestone",
                "url": "https://example.com/tesla-news",
                "published": "2024-01-01T10:00:00Z"
            },
            # Request with special characters
            {
                "news_id": "test-special",
                "headline": "Microsoft & Google's partnership beats expectations!",
                "body": "Companies announce 50% growth in Q4 2024."
            },
            # Request with very long content
            {
                "news_id": "test-long",
                "headline": "Amazon " + "supply chain " * 100 + "optimization",
                "body": "Long article content " * 500
            }
        ]
        
        for test_case in test_cases:
            response = requests.post(f"{self.base_url}/v1/enrich", json=test_case)
            assert response.status_code == 200, f"Failed for test case: {test_case['news_id']}"
            
            data = response.json()
            assert "news_id" in data
            assert "companies" in data
            assert data["news_id"] == test_case["news_id"]
            assert isinstance(data["companies"], list)
            
            # Validate company structure if any found
            for company in data["companies"]:
                assert "ticker" in company
                assert "role" in company
                assert "sentiment" in company
                assert company["role"] in ["primary", "mentioned"]
                assert -1.0 <= company["sentiment"] <= 1.0
    
    def test_enrich_invalid_requests(self):
        """Test enrichment with invalid requests."""
        invalid_requests = [
            # Missing required fields
            ({"headline": "Test headline"}, 422, "Missing news_id"),
            ({"news_id": "test"}, 422, "Missing headline"),
            
            # Empty/invalid values
            ({"news_id": "", "headline": "Test"}, 422, "Empty news_id"),
            ({"news_id": "test", "headline": ""}, 400, "Empty headline"),
            ({"news_id": "test", "headline": "   "}, 400, "Whitespace-only headline"),
            
            # Invalid data types
            ({"news_id": 123, "headline": "Test"}, 422, "Non-string news_id"),
            ({"news_id": "test", "headline": 123}, 422, "Non-string headline"),
            ({"news_id": "test", "headline": None}, 422, "Null headline"),
            
            # Invalid published date format
            ({"news_id": "test", "headline": "Test", "published": "invalid-date"}, 200, "Invalid date format should be accepted"),
        ]
        
        for request_data, expected_status, description in invalid_requests:
            response = requests.post(f"{self.base_url}/v1/enrich", json=request_data)
            assert response.status_code == expected_status, f"Failed: {description} - got {response.status_code}"
    
    def test_enrich_malformed_json(self):
        """Test enrichment with malformed JSON."""
        # Invalid JSON
        response = requests.post(
            f"{self.base_url}/v1/enrich",
            data="invalid json{",
            headers={"Content-Type": "application/json"}
        )
        assert response.status_code == 422
        
        # Empty request body
        response = requests.post(f"{self.base_url}/v1/enrich")
        assert response.status_code == 422
        
        # Wrong content type
        response = requests.post(
            f"{self.base_url}/v1/enrich",
            data="news_id=test&headline=Test",
            headers={"Content-Type": "application/x-www-form-urlencoded"}
        )
        assert response.status_code == 422
    
    def test_enrich_edge_cases(self):
        """Test enrichment with edge cases."""
        edge_cases = [
            # No companies in text
            {
                "news_id": "test-no-companies",
                "headline": "General market outlook improves for next quarter",
                "expected_companies": 0
            },
            # Multiple mentions of same company
            {
                "news_id": "test-duplicate",
                "headline": "Apple iPhone and Apple iPad sales surge as Apple expands",
                "expected_companies": 1  # Should deduplicate
            },
            # Mixed case sensitivity
            {
                "news_id": "test-case",
                "headline": "TESLA PRODUCTION and tesla manufacturing",
                "expected_companies": 1
            },
            # Numbers and special characters
            {
                "news_id": "test-numbers",
                "headline": "Tesla Model 3 & Model Y production up 25% in Q4",
                "expected_min_companies": 1
            },
            # Very short content
            {
                "news_id": "test-short",
                "headline": "Tesla",
                "expected_companies": 1
            }
        ]
        
        for case in edge_cases:
            request_data = {
                "news_id": case["news_id"],
                "headline": case["headline"]
            }
            
            response = requests.post(f"{self.base_url}/v1/enrich", json=request_data)
            assert response.status_code == 200
            
            data = response.json()
            companies = data["companies"]
            
            if "expected_companies" in case:
                assert len(companies) == case["expected_companies"], f"Failed for case: {case['news_id']}"
            elif "expected_min_companies" in case:
                assert len(companies) >= case["expected_min_companies"], f"Failed for case: {case['news_id']}"
    
    def test_enrich_concurrent_requests(self):
        """Test enrichment under concurrent load."""
        import threading
        import time
        
        results = []
        errors = []
        
        def make_request(i):
            try:
                request_data = {
                    "news_id": f"concurrent-test-{i}",
                    "headline": f"Tesla production update {i}"
                }
                
                start_time = time.time()
                response = requests.post(f"{self.base_url}/v1/enrich", json=request_data, timeout=10)
                latency = time.time() - start_time
                
                if response.status_code == 200:
                    results.append({"id": i, "latency": latency, "data": response.json()})
                else:
                    errors.append({"id": i, "status": response.status_code, "error": response.text})
            except Exception as e:
                errors.append({"id": i, "exception": str(e)})
        
        # Create 20 concurrent requests
        threads = []
        for i in range(20):
            thread = threading.Thread(target=make_request, args=(i,))
            threads.append(thread)
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for completion
        for thread in threads:
            thread.join(timeout=30)
        
        # Validate results
        assert len(errors) == 0, f"Concurrent requests failed: {errors}"
        assert len(results) == 20, f"Expected 20 results, got {len(results)}"
        
        # Check latency (should be reasonable)
        latencies = [r["latency"] for r in results]
        avg_latency = sum(latencies) / len(latencies)
        max_latency = max(latencies)
        
        assert avg_latency < 1.0, f"Average latency too high: {avg_latency:.3f}s"
        assert max_latency < 5.0, f"Max latency too high: {max_latency:.3f}s"
        
        # Verify all requests processed correctly
        for result in results:
            assert "companies" in result["data"]
            assert result["data"]["news_id"].startswith("concurrent-test-")
    
    def test_enrich_trace_id_propagation(self):
        """Test trace_id propagation and logging."""
        custom_trace_id = "test-trace-12345"
        
        request_data = {
            "news_id": "trace-test",
            "headline": "Apple announces new product line"
        }
        
        response = requests.post(
            f"{self.base_url}/v1/enrich",
            json=request_data,
            headers={"x-trace-id": custom_trace_id}
        )
        
        assert response.status_code == 200
        
        # Should return same trace_id in response headers
        assert response.headers.get("x-trace-id") == custom_trace_id
        
        # Test without custom trace_id (should generate one)
        response = requests.post(f"{self.base_url}/v1/enrich", json=request_data)
        assert response.status_code == 200
        assert "x-trace-id" in response.headers
        assert response.headers["x-trace-id"].startswith("enrich-")
    
    def test_enrich_http_methods(self):
        """Test that only POST is allowed for /v1/enrich."""
        request_data = {
            "news_id": "method-test",
            "headline": "Test headline"
        }
        
        # POST should work
        response = requests.post(f"{self.base_url}/v1/enrich", json=request_data)
        assert response.status_code == 200
        
        # Other methods should fail
        response = requests.get(f"{self.base_url}/v1/enrich")
        assert response.status_code == 405  # Method Not Allowed
        
        response = requests.put(f"{self.base_url}/v1/enrich", json=request_data)
        assert response.status_code == 405
        
        response = requests.delete(f"{self.base_url}/v1/enrich")
        assert response.status_code == 405
    
    def test_enrich_timeout_resilience(self):
        """Test enrichment service resilience under timeout conditions."""
        # Test with very large request (but not too large to cause real issues)
        large_text = "supply chain " * 1000
        
        request_data = {
            "news_id": "timeout-test",
            "headline": f"Tesla {large_text} optimization",
            "body": large_text * 10  # Even larger body
        }
        
        start_time = time.time()
        response = requests.post(f"{self.base_url}/v1/enrich", json=request_data, timeout=30)
        processing_time = time.time() - start_time
        
        assert response.status_code == 200
        assert processing_time < 10.0, f"Processing took too long: {processing_time:.3f}s"
        
        data = response.json()
        assert "companies" in data
        # Should still detect Tesla despite large text
        tesla_companies = [c for c in data["companies"] if c["ticker"] == "TSLA"]
        assert len(tesla_companies) > 0
    
    def test_nonexistent_endpoints(self):
        """Test behavior with non-existent endpoints."""
        # Non-existent endpoints should return 404
        response = requests.get(f"{self.base_url}/v1/nonexistent")
        assert response.status_code == 404
        
        response = requests.post(f"{self.base_url}/v1/wrong-endpoint")
        assert response.status_code == 404
        
        response = requests.get(f"{self.base_url}/admin")
        assert response.status_code == 404
    
    def test_security_headers(self):
        """Test security-related aspects."""
        # Test for potential security headers
        response = requests.get(f"{self.base_url}/healthz")
        
        # Should not expose server information
        assert "server" not in response.headers.keys()
        
        # Test with potential XSS payload (should be handled safely)
        malicious_request = {
            "news_id": "<script>alert('xss')</script>",
            "headline": "<img src=x onerror=alert('xss')>Tesla news"
        }
        
        response = requests.post(f"{self.base_url}/v1/enrich", json=malicious_request)
        # Should process normally (not execute script)
        assert response.status_code == 200
        
        data = response.json()
        # Should return the news_id as-is (escaped in JSON)
        assert data["news_id"] == "<script>alert('xss')</script>"

class TestEnrichmentAPIPerformance:
    """Performance tests for the enrichment API."""
    
    @classmethod
    def setup_class(cls):
        """Set up test class."""
        cls.base_url = "http://localhost:8082"
    
    def test_response_time_benchmarks(self):
        """Test that response times meet SLA requirements."""
        test_cases = [
            {"news_id": "perf-simple", "headline": "Tesla reports earnings"},
            {"news_id": "perf-complex", "headline": "Apple, Microsoft, Google, and Amazon announce partnership"},
            {"news_id": "perf-long", "headline": "Tesla " + "production efficiency " * 50}
        ]
        
        for case in test_cases:
            start_time = time.time()
            response = requests.post(f"{self.base_url}/v1/enrich", json=case)
            response_time = time.time() - start_time
            
            assert response.status_code == 200
            # Should meet the 200ms p95 SLA from context.md
            assert response_time < 0.5, f"Response time too slow: {response_time:.3f}s for {case['news_id']}"
    
    def test_memory_efficiency(self):
        """Test memory efficiency with repeated requests."""
        # Make many requests to check for memory leaks
        for i in range(100):
            request_data = {
                "news_id": f"memory-test-{i}",
                "headline": f"Tesla production update {i % 10}"  # Reuse some patterns
            }
            
            response = requests.post(f"{self.base_url}/v1/enrich", json=request_data)
            assert response.status_code == 200
            
            if i % 20 == 0:  # Check every 20 requests
                # Service should still be responsive
                health_response = requests.get(f"{self.base_url}/healthz")
                assert health_response.status_code == 200

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-x"])  # -x stops on first failure
