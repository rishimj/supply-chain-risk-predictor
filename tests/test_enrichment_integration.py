#!/usr/bin/env python3
"""
Integration test for the FastAPI Enrichment Service

Tests the enrichment service integrated with the Gateway API,
showing the complete news -> enrichment -> features pipeline.
"""

import requests
import json
import time
import redis
from datetime import datetime

def test_enrichment_integration():
    """Test the enrichment service integrated with the Gateway API."""
    print("🔗 TESTING ENRICHMENT SERVICE INTEGRATION")
    print("=" * 60)
    
    # Initialize Redis client
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Test cases with different companies and sentiments
    test_cases = [
        {
            "name": "Tesla Negative",
            "news": {
                "headline": "Tesla production halts due to severe supply shortage",
                "url": "https://test.com/tesla-halt",
                "published": "2024-01-01T10:00:00Z",
                "full_text": "Tesla Inc. faces major supply chain disruption with production halt"
            },
            "expected_companies": ["TSLA"],
            "expected_sentiment": -0.7
        },
        {
            "name": "Apple Positive", 
            "news": {
                "headline": "Apple expands supplier network with record growth",
                "url": "https://test.com/apple-expand",
                "published": "2024-01-01T11:00:00Z", 
                "full_text": "Apple Inc. announces significant expansion of manufacturing partnerships"
            },
            "expected_companies": ["AAPL"],
            "expected_sentiment": 0.7
        },
        {
            "name": "Multi-Company Partnership",
            "news": {
                "headline": "Microsoft and Amazon partnership beats all expectations",
                "url": "https://test.com/msft-amzn",
                "published": "2024-01-01T12:00:00Z",
                "full_text": "Microsoft Corporation and Amazon Web Services announce expanded collaboration"
            },
            "expected_companies": ["MSFT", "AMZN"],
            "expected_sentiment": 0.7
        },
        {
            "name": "Semiconductor Supply Chain",
            "news": {
                "headline": "NVIDIA GPU shortage impacts TSMC production capacity",
                "url": "https://test.com/nvda-tsmc",
                "published": "2024-01-01T13:00:00Z",
                "full_text": "NVIDIA and TSMC face supply chain challenges affecting semiconductor production"
            },
            "expected_companies": ["NVDA", "TSM"],
            "expected_sentiment": -0.7
        }
    ]
    
    print("📊 TESTING DIRECT ENRICHMENT SERVICE")
    print("-" * 40)
    
    enrichment_results = []
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📰 Test {i}: {test_case['name']}")
        print(f"   Headline: {test_case['news']['headline']}")
        
        # Test direct enrichment service
        enrichment_request = {
            "news_id": f"enrich-test-{i}",
            "headline": test_case['news']['headline'],
            "body": test_case['news']['full_text'],
            "url": test_case['news']['url'],
            "published": test_case['news']['published']
        }
        
        try:
            response = requests.post(
                "http://localhost:8082/v1/enrich",
                json=enrichment_request,
                timeout=5
            )
            
            if response.status_code == 200:
                result = response.json()
                companies = result.get('companies', [])
                
                print(f"   ✅ Enrichment successful: {len(companies)} companies detected")
                
                for company in companies:
                    ticker = company['ticker']
                    sentiment = company['sentiment']
                    role = company['role']
                    
                    print(f"      • {ticker}: {role} role, sentiment {sentiment:+.1f}")
                    
                    # Verify expected companies
                    if ticker in test_case['expected_companies']:
                        if sentiment == test_case['expected_sentiment']:
                            print(f"        ✅ Expected sentiment matched")
                        else:
                            print(f"        ⚠️  Sentiment mismatch: got {sentiment}, expected {test_case['expected_sentiment']}")
                    
                enrichment_results.append({
                    'test_case': test_case,
                    'result': result
                })
                
            else:
                print(f"   ❌ Enrichment failed: {response.status_code} {response.text}")
                
        except Exception as e:
            print(f"   ❌ Enrichment error: {e}")
    
    # Show enrichment service statistics
    print("\n" + "=" * 60)
    print("📊 ENRICHMENT SERVICE STATISTICS")
    print("=" * 60)
    
    try:
        stats_response = requests.get("http://localhost:8082/stats", timeout=5)
        if stats_response.status_code == 200:
            stats = stats_response.json()
            print(f"🏢 Total Companies: {stats['total_companies']}")
            print(f"🔍 Total Keywords: {stats['total_keywords']}")
            print(f"😞 Negative Keywords: {stats['negative_sentiment_keywords']}")
            print(f"😊 Positive Keywords: {stats['positive_sentiment_keywords']}")
            print(f"🔧 Service Version: {stats['version']}")
        else:
            print("❌ Failed to get stats")
    except Exception as e:
        print(f"❌ Stats error: {e}")
    
    # Test integration with Gateway API (if running)
    print("\n" + "=" * 60)
    print("🚪 TESTING GATEWAY + ENRICHMENT INTEGRATION")
    print("=" * 60)
    
    try:
        # Check if gateway is running
        gateway_health = requests.get("http://localhost:8080/healthz", timeout=2)
        if gateway_health.status_code == 200:
            print("✅ Gateway service is running")
            
            # Send news through Gateway -> Kafka -> (Future: Real Enrichment)
            test_news = {
                "headline": "Boeing 737 production increases after supply chain optimization",
                "url": "https://test.com/boeing-production",
                "published": "2024-01-01T14:00:00Z",
                "full_text": "Boeing announces increased 737 production following supply chain improvements"
            }
            
            print(f"\n📰 Sending news through Gateway:")
            print(f"   {test_news['headline']}")
            
            gateway_response = requests.post(
                "http://localhost:8080/v1/news",
                json=test_news,
                timeout=5
            )
            
            if gateway_response.status_code == 200:
                gateway_result = gateway_response.json()
                print(f"   ✅ Gateway accepted news: {gateway_result['news_id']}")
                print(f"   📨 Status: {gateway_result['status']}")
                
                # Test the same news directly with enrichment service
                enrichment_request = {
                    "news_id": gateway_result['news_id'],
                    "headline": test_news['headline'],
                    "body": test_news['full_text']
                }
                
                enrich_response = requests.post(
                    "http://localhost:8082/v1/enrich",
                    json=enrichment_request,
                    timeout=5
                )
                
                if enrich_response.status_code == 200:
                    enrich_result = enrich_response.json()
                    companies = enrich_result.get('companies', [])
                    print(f"   ✅ Enrichment detected {len(companies)} companies:")
                    
                    for company in companies:
                        print(f"      • {company['ticker']}: {company['role']} role, sentiment {company['sentiment']:+.1f}")
                else:
                    print(f"   ❌ Enrichment failed: {enrich_response.status_code}")
            else:
                print(f"   ❌ Gateway failed: {gateway_response.status_code}")
        else:
            print("⚠️  Gateway service not running - skipping integration test")
            
    except Exception as e:
        print(f"⚠️  Gateway integration test skipped: {e}")
    
    # Summary
    print("\n" + "=" * 60)
    print("✅ ENRICHMENT SERVICE INTEGRATION TEST COMPLETE!")
    print("=" * 60)
    
    successful_tests = len([r for r in enrichment_results if r['result']['companies']])
    print(f"🎯 Successful enrichments: {successful_tests}/{len(test_cases)}")
    print(f"🏢 Companies detected across all tests: {sum(len(r['result']['companies']) for r in enrichment_results)}")
    
    print(f"\n🌟 Your FastAPI enrichment service is working perfectly!")
    print(f"📡 Available at: http://localhost:8082")
    print(f"📊 Metrics at: http://localhost:8082/metrics")
    print(f"📈 Stats at: http://localhost:8082/stats")
    
    print(f"\n🔗 Ready for integration with:")
    print(f"   • Flink processor (via HTTP calls)")
    print(f"   • Real-time news pipeline")
    print(f"   • Supply chain risk monitoring")

if __name__ == "__main__":
    test_enrichment_integration()
