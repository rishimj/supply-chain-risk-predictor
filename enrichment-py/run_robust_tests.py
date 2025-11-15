#!/usr/bin/env python3
"""
Comprehensive test runner for enrichment service robustness tests

This script runs both unit tests and integration tests to ensure
the enrichment service is robust and production-ready.
"""

import sys
import os
import time
import subprocess
import requests
from contextlib import contextmanager

def check_service_health(base_url="http://localhost:8082", timeout=5):
    """Check if the enrichment service is running and healthy."""
    try:
        response = requests.get(f"{base_url}/healthz", timeout=timeout)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def run_unit_tests():
    """Run unit tests that don't require the HTTP server."""
    print("🧪 RUNNING UNIT TESTS (No HTTP server required)")
    print("=" * 60)
    
    # Run the service logic tests
    result = subprocess.run([
        sys.executable, "-m", "pytest", 
        "tests/test_enrichment.py",
        "tests/test_service_robust.py", 
        "-v", "--tb=short"
    ], cwd=os.path.dirname(os.path.abspath(__file__)))
    
    return result.returncode == 0

def run_integration_tests():
    """Run integration tests that require the HTTP server."""
    print("\n🌐 RUNNING INTEGRATION TESTS (Requires HTTP server)")
    print("=" * 60)
    
    # Check if service is running
    if not check_service_health():
        print("❌ Enrichment service not running on http://localhost:8082")
        print("   Please start the service first:")
        print("   1. docker-compose up enrichment -d")
        print("   2. Or: cd enrichment-py && python app.py")
        return False
    
    print("✅ Enrichment service is running")
    
    # Run integration tests
    result = subprocess.run([
        sys.executable, "-m", "pytest",
        "tests/test_api_robust.py",
        "-v", "--tb=short", "-x"  # Stop on first failure for integration tests
    ], cwd=os.path.dirname(os.path.abspath(__file__)))
    
    return result.returncode == 0

def run_performance_tests():
    """Run basic performance tests."""
    print("\n⚡ RUNNING PERFORMANCE TESTS")
    print("=" * 60)
    
    if not check_service_health():
        print("❌ Service not available for performance tests")
        return False
    
    # Test response time
    start_time = time.time()
    try:
        response = requests.post("http://localhost:8082/v1/enrich", json={
            "news_id": "perf-test",
            "headline": "Tesla production increases with supply chain optimization"
        }, timeout=5)
        
        response_time = time.time() - start_time
        
        if response.status_code == 200:
            print(f"✅ Basic response time: {response_time:.3f}s")
            
            if response_time < 0.2:
                print("   🚀 Excellent response time (< 200ms)")
            elif response_time < 0.5:
                print("   ✅ Good response time (< 500ms)")
            else:
                print("   ⚠️  Slow response time (> 500ms)")
        else:
            print(f"❌ Performance test failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Performance test error: {e}")
        return False
    
    # Test concurrent requests
    print("\n🔄 Testing concurrent requests...")
    import threading
    
    results = []
    errors = []
    
    def make_request(i):
        try:
            start = time.time()
            resp = requests.post("http://localhost:8082/v1/enrich", json={
                "news_id": f"concurrent-{i}",
                "headline": f"Apple production update {i}"
            }, timeout=10)
            
            if resp.status_code == 200:
                results.append(time.time() - start)
            else:
                errors.append(f"Request {i}: {resp.status_code}")
        except Exception as e:
            errors.append(f"Request {i}: {e}")
    
    # Run 10 concurrent requests
    threads = []
    for i in range(10):
        thread = threading.Thread(target=make_request, args=(i,))
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    if errors:
        print(f"❌ Concurrent test errors: {errors}")
        return False
    
    if results:
        avg_time = sum(results) / len(results)
        max_time = max(results)
        print(f"✅ Concurrent requests: avg {avg_time:.3f}s, max {max_time:.3f}s")
        
        if max_time < 1.0:
            print("   🚀 Excellent concurrent performance")
        else:
            print("   ⚠️  High latency under concurrent load")
    
    return True

def show_service_stats():
    """Show current service statistics."""
    print("\n📊 SERVICE STATISTICS")
    print("=" * 60)
    
    try:
        # Get stats
        stats_response = requests.get("http://localhost:8082/stats", timeout=5)
        if stats_response.status_code == 200:
            stats = stats_response.json()
            print(f"🏢 Total Companies: {stats['total_companies']}")
            print(f"🔍 Total Keywords: {stats['total_keywords']}")
            print(f"😞 Negative Keywords: {stats['negative_sentiment_keywords']}")
            print(f"😊 Positive Keywords: {stats['positive_sentiment_keywords']}")
            print(f"🔧 Service Version: {stats['version']}")
            print(f"🌍 Environment: {stats['environment']}")
        
        # Get recent metrics
        metrics_response = requests.get("http://localhost:8082/metrics", timeout=5)
        if metrics_response.status_code == 200:
            metrics = metrics_response.text
            
            # Extract key metrics
            lines = metrics.split('\n')
            for line in lines:
                if 'enrich_requests_total{outcome="success"}' in line:
                    print(f"✅ Successful Requests: {line.split()[-1]}")
                elif 'enrich_latency_seconds_sum' in line:
                    total_time = float(line.split()[-1])
                    print(f"⏱️  Total Processing Time: {total_time:.3f}s")
                elif 'enrich_latency_seconds_count' in line:
                    count = float(line.split()[-1])
                    if count > 0 and 'total_time' in locals():
                        avg_latency = total_time / count
                        print(f"📈 Average Latency: {avg_latency:.3f}s")
        
    except Exception as e:
        print(f"⚠️  Could not get service stats: {e}")

def main():
    """Run comprehensive robustness tests."""
    print("🛡️  ENRICHMENT SERVICE ROBUSTNESS TESTS")
    print("=" * 60)
    print("Testing the FastAPI enrichment service for production readiness")
    print()
    
    # Track test results
    results = {}
    
    # Run unit tests first (don't require server)
    results['unit_tests'] = run_unit_tests()
    
    # Run integration tests (require server)
    results['integration_tests'] = run_integration_tests()
    
    # Run performance tests
    results['performance_tests'] = run_performance_tests()
    
    # Show service statistics
    if check_service_health():
        show_service_stats()
    
    # Summary
    print("\n" + "=" * 60)
    print("📋 TEST SUMMARY")
    print("=" * 60)
    
    total_tests = len(results)
    passed_tests = sum(1 for result in results.values() if result)
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{test_name.replace('_', ' ').title()}: {status}")
    
    print(f"\nOverall: {passed_tests}/{total_tests} test suites passed")
    
    if all(results.values()):
        print("\n🎉 ALL TESTS PASSED - SERVICE IS PRODUCTION READY! 🎉")
        print("\n🔒 Your enrichment service is robust and ready for:")
        print("   • Production deployment")
        print("   • High-traffic workloads")  
        print("   • Integration with Flink processor")
        print("   • Real-time supply chain monitoring")
        return 0
    else:
        print("\n⚠️  SOME TESTS FAILED - REVIEW BEFORE PRODUCTION")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
