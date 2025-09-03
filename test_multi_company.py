#!/usr/bin/env python3
"""
Test multiple companies with the mock enrichment client
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

async def test_multiple_companies():
    """Test different companies with the mock enrichment client."""
    print("🧪 TESTING MULTIPLE COMPANIES")
    print("=" * 50)
    
    # Initialize clients
    enrichment_client = MockEnrichmentClient()
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
    
    # Test cases for different companies
    test_cases = [
        {
            "company": "Apple",
            "news": NewsMessage(
                news_id="test-apple-1",
                headline="Apple supplier network expands with growth partnerships",
                url="https://test.com/apple-expand",
                published="2024-01-01T10:00:00Z",
                full_text="Apple announces expansion of supplier partnerships"
            ),
            "expected_ticker": "AAPL",
            "expected_sentiment": 0.7  # positive due to "expand" keyword
        },
        {
            "company": "Amazon", 
            "news": NewsMessage(
                news_id="test-amazon-1",
                headline="Amazon warehouse operations halt due to system issues",
                url="https://test.com/amazon-halt",
                published="2024-01-01T11:00:00Z",
                full_text="Amazon temporarily halts warehouse operations"
            ),
            "expected_ticker": "AMZN",
            "expected_sentiment": -0.7  # negative due to "halt" keyword
        },
        {
            "company": "Google",
            "news": NewsMessage(
                news_id="test-google-1", 
                headline="Google cloud infrastructure beat analyst expectations",
                url="https://test.com/google-beat",
                published="2024-01-01T12:00:00Z",
                full_text="Google parent Alphabet reports cloud services beat expectations"
            ),
            "expected_ticker": "GOOGL",
            "expected_sentiment": 0.7  # positive due to "beat" keyword
        },
        {
            "company": "Microsoft",
            "news": NewsMessage(
                news_id="test-microsoft-1",
                headline="Microsoft reports quarterly supply chain performance", 
                url="https://test.com/microsoft-quarterly",
                published="2024-01-01T13:00:00Z",
                full_text="Microsoft Corporation released quarterly results"
            ),
            "expected_ticker": "MSFT",
            "expected_sentiment": 0.0  # neutral - no sentiment keywords
        }
    ]
    
    # Process each test case
    company_features = {}
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📰 Test {i}: {test_case['company']}")
        print(f"   Headline: {test_case['news'].headline}")
        
        # Call enrichment
        result = await enrichment_client.enrich_news(test_case['news'])
        
        if result.companies:
            company = result.companies[0]  # Take first match
            print(f"   ✅ Detected: {company.ticker} (sentiment: {company.sentiment})")
            
            # Verify expectations
            if company.ticker == test_case['expected_ticker']:
                print(f"   ✅ Ticker match: {company.ticker}")
            else:
                print(f"   ❌ Ticker mismatch: got {company.ticker}, expected {test_case['expected_ticker']}")
                
            if company.sentiment == test_case['expected_sentiment']:
                print(f"   ✅ Sentiment match: {company.sentiment}")
            else:
                print(f"   ❌ Sentiment mismatch: got {company.sentiment}, expected {test_case['expected_sentiment']}")
            
            # Create features for this company
            ticker = company.ticker
            if ticker not in company_features:
                company_features[ticker] = {
                    'pos_count': 0,
                    'neg_count': 0,
                    'sentiment_ewm': 0.0,
                    'sentiment_count': 0
                }
            
            # Update features
            features = company_features[ticker]
            if company.sentiment > 0:
                features['pos_count'] += 1
            elif company.sentiment < 0:
                features['neg_count'] += 1
                
            # Update EWM
            alpha = 0.1
            if features['sentiment_count'] == 0:
                features['sentiment_ewm'] = company.sentiment
            else:
                features['sentiment_ewm'] = alpha * company.sentiment + (1 - alpha) * features['sentiment_ewm']
            features['sentiment_count'] += 1
            
        else:
            print(f"   ❌ No companies detected")
    
    # Store features in Redis and display results
    print("\n" + "=" * 50)
    print("💾 STORING FEATURES IN REDIS")
    print("=" * 50)
    
    for ticker, features in company_features.items():
        # Create CompanyFeatures object
        company_features_obj = CompanyFeatures(
            ticker=ticker,
            window_end=datetime.utcnow().isoformat() + "Z",
            neg_news_count_24h=features['neg_count'],
            pos_news_count_24h=features['pos_count'],
            sentiment_ewm_7d=round(features['sentiment_ewm'], 3)
        )
        
        # Store in Redis
        key = f"feat:{ticker}:multi_test"
        value = company_features_obj.to_redis_value()
        redis_client.setex(key, 3600, value)  # 1 hour TTL
        
        print(f"\n🏢 {ticker}:")
        print(f"   📊 Positive news: {features['pos_count']}")
        print(f"   📊 Negative news: {features['neg_count']}")  
        print(f"   📊 Sentiment EWM: {features['sentiment_ewm']:.3f}")
        
        # Calculate risk score
        risk_score = 0.5 + (features['neg_count'] * 0.1) - (features['pos_count'] * 0.05) - (features['sentiment_ewm'] * 0.2)
        risk_score = max(0.0, min(1.0, risk_score))
        
        risk_level = 'HIGH' if risk_score > 0.7 else 'MEDIUM' if risk_score > 0.4 else 'LOW'
        print(f"   🎯 Risk Score: {risk_score:.3f} ({risk_level})")
        print(f"   💾 Stored: {key}")
    
    print("\n" + "=" * 50)
    print("✅ MULTI-COMPANY TEST COMPLETE!")
    print(f"📊 Processed {len(company_features)} companies")
    print("🔗 Check Redis UI at http://localhost:8081 to see the features")

if __name__ == "__main__":
    asyncio.run(test_multiple_companies())
