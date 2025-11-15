#!/usr/bin/env python3
"""
Test the expanded company database with various Fortune 500 companies
"""
import asyncio
import sys
import redis
import json
from datetime import datetime

# Add the flink-processor src to path
sys.path.append('flink-processor/src')

from models import NewsMessage, CompanyFeatures
from enrichment_client import MockEnrichmentClient
from company_database import COMPANY_DATABASE, get_company_keywords

async def test_expanded_companies():
    """Test various Fortune 500 companies with the expanded database."""
    print("🏢 TESTING EXPANDED COMPANY DATABASE")
    print("=" * 60)
    
    # Show database stats
    keyword_map = get_company_keywords()
    print(f"📊 Database Stats:")
    print(f"   • Total companies: {len(COMPANY_DATABASE)}")
    print(f"   • Total keywords: {len(keyword_map)}")
    
    sectors = {}
    suppliers = 0
    for company in COMPANY_DATABASE.values():
        sectors[company.sector] = sectors.get(company.sector, 0) + 1
        if company.is_supplier:
            suppliers += 1
    
    print(f"   • Sectors: {len(sectors)}")
    print(f"   • Supply chain companies: {suppliers}")
    
    for sector, count in sorted(sectors.items()):
        print(f"     - {sector}: {count}")
    
    print("\n" + "=" * 60)
    print("🧪 TESTING COMPANY DETECTION")
    print("=" * 60)
    
    # Initialize clients
    enrichment_client = MockEnrichmentClient()
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Test cases covering different sectors and companies
    test_cases = [
        # Technology Giants
        {
            "headline": "Apple iPhone supply chain faces delays in China",
            "expected": "AAPL",
            "sector": "Technology"
        },
        {
            "headline": "Microsoft Azure cloud services beat quarterly expectations",
            "expected": "MSFT", 
            "sector": "Technology"
        },
        {
            "headline": "NVIDIA GPU shortage impacts gaming industry",
            "expected": "NVDA",
            "sector": "Semiconductors"
        },
        # Automotive
        {
            "headline": "Tesla Cybertruck production ramps up at Austin facility",
            "expected": "TSLA",
            "sector": "Automotive"
        },
        {
            "headline": "Ford F-150 Lightning electric truck sales surge",
            "expected": "F",
            "sector": "Automotive"
        },
        {
            "headline": "Toyota hybrid vehicle technology expands globally",
            "expected": "TM",
            "sector": "Automotive"
        },
        # Supply Chain Critical
        {
            "headline": "TSMC semiconductor fab construction delayed in Arizona",
            "expected": "TSM",
            "sector": "Semiconductors"
        },
        {
            "headline": "Foxconn manufacturing operations halt due to COVID",
            "expected": "AAPL",  # Maps to Apple as major customer
            "sector": "Electronics Manufacturing"
        },
        # Retail
        {
            "headline": "Walmart supply chain optimization reduces costs",
            "expected": "WMT",
            "sector": "Retail"
        },
        {
            "headline": "Amazon Prime delivery network expands to rural areas",
            "expected": "AMZN",
            "sector": "Technology"
        },
        # Industrial/Aerospace
        {
            "headline": "Boeing 737 MAX production increases after safety review",
            "expected": "BA",
            "sector": "Aerospace"
        },
        {
            "headline": "Caterpillar heavy machinery demand surge in construction",
            "expected": "CAT",
            "sector": "Industrial"
        },
        # Financial
        {
            "headline": "JPMorgan Chase reports strong quarterly earnings",
            "expected": "JPM",
            "sector": "Financial"
        },
        # Healthcare
        {
            "headline": "Johnson & Johnson vaccine production ramps up",
            "expected": "JNJ",
            "sector": "Healthcare"
        },
        # Energy
        {
            "headline": "Exxon Mobil oil production cuts due to refinery issues",
            "expected": "XOM",
            "sector": "Energy"
        },
        # International
        {
            "headline": "Alibaba e-commerce platform faces regulatory scrutiny",
            "expected": "BABA",
            "sector": "Technology"
        }
    ]
    
    # Process each test case
    successful_detections = 0
    company_features = {}
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📰 Test {i:2d}: {test_case['headline']}")
        
        news = NewsMessage(
            news_id=f"test-{i}",
            headline=test_case['headline'],
            url=f"https://test.com/news-{i}",
            published="2024-01-01T10:00:00Z",
            full_text=test_case['headline']
        )
        
        # Call enrichment
        result = await enrichment_client.enrich_news(news)
        
        if result.companies:
            company = result.companies[0]  # Take first match
            if company.ticker == test_case['expected']:
                print(f"   ✅ Detected: {company.ticker} (sentiment: {company.sentiment:+.1f})")
                successful_detections += 1
                
                # Track for features
                ticker = company.ticker
                if ticker not in company_features:
                    company_features[ticker] = {
                        'pos_count': 0,
                        'neg_count': 0,
                        'total_count': 0,
                        'sentiment_sum': 0.0,
                        'sector': COMPANY_DATABASE[ticker].sector
                    }
                
                features = company_features[ticker]
                features['total_count'] += 1
                features['sentiment_sum'] += company.sentiment
                
                if company.sentiment > 0:
                    features['pos_count'] += 1
                elif company.sentiment < 0:
                    features['neg_count'] += 1
                    
            else:
                print(f"   ❌ Wrong ticker: got {company.ticker}, expected {test_case['expected']}")
        else:
            print(f"   ❌ No companies detected (expected {test_case['expected']})")
    
    # Show results summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    print(f"🎯 Detection Success Rate: {successful_detections}/{len(test_cases)} ({successful_detections/len(test_cases)*100:.1f}%)")
    print(f"🏢 Unique Companies Detected: {len(company_features)}")
    
    print("\n📈 COMPANY FEATURES GENERATED:")
    print("-" * 40)
    
    for ticker in sorted(company_features.keys()):
        features = company_features[ticker]
        avg_sentiment = features['sentiment_sum'] / features['total_count'] if features['total_count'] > 0 else 0.0
        
        # Calculate risk score
        risk_score = 0.5 + (features['neg_count'] * 0.1) - (features['pos_count'] * 0.05) - (avg_sentiment * 0.2)
        risk_score = max(0.0, min(1.0, risk_score))
        risk_level = 'HIGH' if risk_score > 0.7 else 'MEDIUM' if risk_score > 0.4 else 'LOW'
        
        company_info = COMPANY_DATABASE[ticker]
        print(f"{ticker:5} ({company_info.sector:12}) | News:{features['total_count']} Pos:{features['pos_count']} Neg:{features['neg_count']} | Risk:{risk_score:.3f} ({risk_level})")
    
    # Store a sample in Redis
    print("\n💾 STORING SAMPLE FEATURES IN REDIS...")
    for ticker, features in list(company_features.items())[:5]:  # Store first 5
        avg_sentiment = features['sentiment_sum'] / features['total_count']
        
        company_features_obj = CompanyFeatures(
            ticker=ticker,
            window_end=datetime.now().isoformat() + "Z",
            neg_news_count_24h=features['neg_count'],
            pos_news_count_24h=features['pos_count'],
            sentiment_ewm_7d=round(avg_sentiment, 3)
        )
        
        key = f"feat:{ticker}:expanded_test"
        value = company_features_obj.to_redis_value()
        redis_client.setex(key, 3600, value)
        print(f"   ✅ Stored {key}")
    
    print("\n" + "=" * 60)
    print("✅ EXPANDED COMPANY DATABASE TEST COMPLETE!")
    print(f"🌟 Now supporting {len(COMPANY_DATABASE)} Fortune 500 companies")
    print(f"🔍 {len(keyword_map)} total keywords for detection")
    print("🔗 Check Redis UI at http://localhost:8081 to see stored features")

if __name__ == "__main__":
    asyncio.run(test_expanded_companies())
