#!/usr/bin/env python3
"""
Test script to verify ARM64 setup is working correctly.
"""
import requests
import redis
import json
import time

def test_gateway():
    """Test the Go gateway service."""
    print("🧪 Testing Gateway Service...")
    
    try:
        # Test health endpoint
        health_resp = requests.get("http://localhost:8080/healthz", timeout=5)
        print(f"   ✅ Health check: {health_resp.status_code} - {health_resp.json()}")
        
        # Test news endpoint
        news_data = {
            "headline": "ARM64 Test: Tesla factory expansion in Texas",
            "url": "https://test.com/tesla-arm64",
            "published": "2024-01-01T10:00:00Z",
            "full_text": "Tesla announces major expansion of ARM64-powered manufacturing systems"
        }
        
        news_resp = requests.post(
            "http://localhost:8080/v1/news", 
            json=news_data,
            timeout=5
        )
        print(f"   ✅ News submission: {news_resp.status_code} - {news_resp.json()}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Gateway test failed: {e}")
        return False

def test_redis():
    """Test Redis connection."""
    print("🧪 Testing Redis...")
    
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
        # Test basic operations
        r.set('arm64_test', 'success')
        value = r.get('arm64_test')
        print(f"   ✅ Redis read/write: {value}")
        
        # Test feature store format
        feature_key = "feat:TSLA:1704110400"
        feature_data = {
            "ticker": "TSLA",
            "neg_news_count_24h": 1,
            "pos_news_count_24h": 2,
            "sentiment_ewm_7d": 0.3,
            "_ver": "v1",
            "_platform": "arm64"
        }
        
        r.setex(feature_key, 3600, json.dumps(feature_data))
        stored = json.loads(r.get(feature_key))
        print(f"   ✅ Feature store format: {stored['ticker']} platform={stored['_platform']}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Redis test failed: {e}")
        return False

def test_kafka_ui():
    """Test Kafka UI accessibility."""
    print("🧪 Testing Kafka UI...")
    
    try:
        resp = requests.get("http://localhost:8090", timeout=5)
        if "kafka" in resp.text.lower():
            print(f"   ✅ Kafka UI accessible: {resp.status_code}")
            return True
        else:
            print(f"   ❌ Kafka UI not responding properly")
            return False
            
    except Exception as e:
        print(f"   ❌ Kafka UI test failed: {e}")
        return False

def test_redis_ui():
    """Test Redis UI accessibility."""
    print("🧪 Testing Redis UI...")
    
    try:
        resp = requests.get("http://localhost:8081", timeout=5)
        if resp.status_code == 200:
            print(f"   ✅ Redis UI accessible: {resp.status_code}")
            return True
        else:
            print(f"   ❌ Redis UI returned: {resp.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Redis UI test failed: {e}")
        return False

def main():
    """Run all tests."""
    print("🚀 Testing ARM64 macOS Setup for Supply Chain Risk Predictor")
    print("=" * 60)
    
    results = []
    
    # Wait a moment for services to be ready
    print("⏱️  Waiting 3 seconds for services to be ready...")
    time.sleep(3)
    
    # Run tests
    results.append(("Gateway", test_gateway()))
    results.append(("Redis", test_redis()))
    results.append(("Kafka UI", test_kafka_ui()))
    results.append(("Redis UI", test_redis_ui()))
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 Test Results:")
    
    passed = 0
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {name:12} {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Summary: {passed}/{len(results)} tests passed")
    
    if passed == len(results):
        print("🎉 All tests passed! Your ARM64 macOS setup is working perfectly!")
        print("\n🔗 Service URLs:")
        print("   • Gateway API: http://localhost:8080")
        print("   • Kafka UI: http://localhost:8090")
        print("   • Redis UI: http://localhost:8081 (admin/admin)")
        print("   • Metrics: http://localhost:9100/metrics")
    else:
        print("⚠️  Some tests failed. Check the service logs with 'make logs'")

if __name__ == "__main__":
    main()
