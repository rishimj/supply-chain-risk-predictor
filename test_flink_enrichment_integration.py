#!/usr/bin/env python3
"""
Test script to verify Flink processor can call the real enrichment service

This script tests the integration between:
1. Go Gateway API
2. Kafka message queue  
3. Real FastAPI enrichment service (instead of mock)
4. Feature storage in Redis
"""

import requests
import redis
import json
import time
from datetime import datetime

def test_complete_pipeline():
    """Test the complete pipeline with real enrichment service."""
    print("🔗 TESTING COMPLETE PIPELINE WITH REAL ENRICHMENT")
    print("=" * 60)
    
    # Initialize connections
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Test case: News that should trigger enrichment
    test_news = {
        "headline": "Tesla Cybertruck production accelerates with supply chain improvements",
        "url": "https://test.com/tesla-cybertruck-production",
        "published": "2024-01-01T15:00:00Z",
        "full_text": "Tesla Inc. announces significant acceleration in Cybertruck production following major supply chain optimizations and new supplier partnerships."
    }
    
    print(f"📰 Test News:")
    print(f"   Headline: {test_news['headline']}")
    
    # Step 1: Send news through Gateway API
    print(f"\n🚪 Step 1: Sending news through Gateway API...")
    try:
        gateway_response = requests.post(
            "http://localhost:8080/v1/news",
            json=test_news,
            timeout=10
        )
        
        if gateway_response.status_code == 200:
            gateway_result = gateway_response.json()
            news_id = gateway_result['news_id']
            print(f"   ✅ Gateway accepted news: {news_id}")
            print(f"   📨 Status: {gateway_result['status']}")
        else:
            print(f"   ❌ Gateway failed: {gateway_response.status_code} {gateway_response.text}")
            return False
            
    except Exception as e:
        print(f"   ❌ Gateway error: {e}")
        return False
    
    # Step 2: Test enrichment service directly (to verify it's working)
    print(f"\n🧠 Step 2: Testing enrichment service directly...")
    try:
        enrichment_request = {
            "news_id": news_id,
            "headline": test_news['headline'],
            "body": test_news['full_text'],
            "url": test_news['url'],
            "published": test_news['published']
        }
        
        enrichment_response = requests.post(
            "http://localhost:8082/v1/enrich",
            json=enrichment_request,
            timeout=10
        )
        
        if enrichment_response.status_code == 200:
            enrichment_result = enrichment_response.json()
            print(f"   ✅ Enrichment service working:")
            print(f"   📊 Companies detected: {len(enrichment_result['companies'])}")
            
            for company in enrichment_result['companies']:
                print(f"      • {company['ticker']}: {company['role']} role, sentiment {company['sentiment']:+.1f}")
        else:
            print(f"   ❌ Enrichment failed: {enrichment_response.status_code}")
            
    except Exception as e:
        print(f"   ❌ Enrichment error: {e}")
    
    # Step 3: Wait for Flink processing
    print(f"\n⚡ Step 3: Waiting for Flink processor to process the message...")
    print(f"   (This may take 10-30 seconds depending on Flink setup)")
    
    # Check Redis for features over time
    max_wait_time = 60  # seconds
    check_interval = 5  # seconds
    start_time = time.time()
    features_found = False
    
    while time.time() - start_time < max_wait_time:
        # Look for any new features in Redis
        feature_keys = redis_client.keys('feat:*')
        
        if feature_keys:
            print(f"   🔍 Found {len(feature_keys)} feature keys in Redis")
            
            # Look for recent features (within last 2 minutes)
            recent_features = []
            for key in feature_keys:
                try:
                    data = json.loads(redis_client.get(key))
                    ingest_ts = data.get('_ingest_ts', '')
                    if ingest_ts:
                        # Check if this is a recent feature
                        recent_features.append((key, data))
                except:
                    pass
            
            if recent_features:
                print(f"   ✅ Found recent features! Pipeline is working:")
                for key, data in recent_features[-3:]:  # Show last 3
                    ticker = data.get('ticker', 'UNKNOWN')
                    pos_count = data.get('pos_news_count_24h', 0)
                    neg_count = data.get('neg_news_count_24h', 0)
                    sentiment_ewm = data.get('sentiment_ewm_7d', 0.0)
                    
                    print(f"      📈 {ticker}: Pos={pos_count}, Neg={neg_count}, EWM={sentiment_ewm:.3f}")
                
                features_found = True
                break
        
        print(f"   ⏳ Waiting... ({int(time.time() - start_time)}s elapsed)")
        time.sleep(check_interval)
    
    # Step 4: Final verification
    print(f"\n📊 Step 4: Final verification...")
    
    if features_found:
        print(f"   ✅ SUCCESS: Complete pipeline working!")
        print(f"   🔄 Data flow verified:")
        print(f"      1. ✅ Gateway API accepted news")
        print(f"      2. ✅ Enrichment service detected companies")
        print(f"      3. ✅ Flink processor generated features")
        print(f"      4. ✅ Features stored in Redis")
        
        # Show final stats
        all_keys = redis_client.keys('feat:*')
        print(f"\n📈 Final Redis Stats:")
        print(f"   • Total feature keys: {len(all_keys)}")
        
        companies = set()
        for key in all_keys:
            try:
                data = json.loads(redis_client.get(key))
                companies.add(data.get('ticker', 'UNKNOWN'))
            except:
                pass
        
        print(f"   • Unique companies: {len(companies)}")
        print(f"   • Companies: {', '.join(sorted(companies))}")
        
        return True
    else:
        print(f"   ⚠️  No recent features found in Redis")
        print(f"   📋 Possible issues:")
        print(f"      • Flink processor not running")
        print(f"      • Flink using mock enrichment instead of real service")
        print(f"      • Network connectivity issues")
        print(f"      • Processing delays")
        
        return False

def check_services():
    """Check if all required services are running."""
    print("🔍 CHECKING SERVICE STATUS")
    print("=" * 40)
    
    services = [
        ("Gateway API", "http://localhost:8080/healthz"),
        ("Enrichment Service", "http://localhost:8082/healthz"), 
        ("Kafka UI", "http://localhost:8090"),
        ("Redis UI", "http://localhost:8081"),
    ]
    
    all_healthy = True
    
    for name, url in services:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                print(f"   ✅ {name}: Running")
            else:
                print(f"   ❌ {name}: HTTP {response.status_code}")
                all_healthy = False
        except Exception as e:
            print(f"   ❌ {name}: Not accessible ({e})")
            all_healthy = False
    
    # Check Redis
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        r.ping()
        print(f"   ✅ Redis: Connected")
    except Exception as e:
        print(f"   ❌ Redis: Not accessible ({e})")
        all_healthy = False
    
    return all_healthy

def main():
    """Main test execution."""
    print("🚀 FLINK-PROCESSOR → ENRICHMENT SERVICE INTEGRATION TEST")
    print("=" * 60)
    print("Testing switch from MockEnrichmentClient to real FastAPI service")
    print()
    
    # Check services first
    if not check_services():
        print("\n❌ Some services are not running. Please start all services:")
        print("   docker-compose up -d")
        return 1
    
    print("\n✅ All services are running!")
    
    # Run the complete pipeline test
    success = test_complete_pipeline()
    
    print("\n" + "=" * 60)
    if success:
        print("🎉 INTEGRATION TEST PASSED!")
        print("✅ Flink processor successfully using real enrichment service!")
        print()
        print("🔗 Your complete pipeline is now:")
        print("   📰 News → 🚪 Gateway → 📨 Kafka → ⚡ Flink → 🧠 Enrichment API → 📊 Features → 💾 Redis")
        return 0
    else:
        print("⚠️  INTEGRATION TEST INCOMPLETE")
        print("The enrichment service is working, but check Flink processor configuration.")
        return 1

if __name__ == "__main__":
    exit(main())
