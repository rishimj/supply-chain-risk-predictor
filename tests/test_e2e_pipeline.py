#!/usr/bin/env python3
"""
End-to-End Pipeline Test
Tests the complete flow: Gateway → Kafka → Processing → Redis
"""
import requests
import redis
import json
import time
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class E2EPipelineTest:
    """End-to-end pipeline testing."""
    
    def __init__(self):
        self.gateway_url = "http://localhost:8080"
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        
    def test_step_1_gateway_api(self):
        """Step 1: Send news through Gateway API."""
        logger.info("🚀 STEP 1: Testing Gateway API")
        
        # Test news with different companies and sentiments
        test_cases = [
            {
                "name": "Tesla Negative News",
                "data": {
                    "headline": "Tesla factory halts production due to supply chain issues",
                    "url": "https://test.com/tesla-halt",
                    "published": datetime.utcnow().isoformat() + "Z",
                    "full_text": "Tesla's Gigafactory in Texas has temporarily halted production due to semiconductor shortage affecting the automotive supply chain."
                },
                "expected_company": "TSLA",
                "expected_sentiment": "negative"
            },
            {
                "name": "Apple Positive News", 
                "data": {
                    "headline": "Apple expands supplier network with new partnerships",
                    "url": "https://test.com/apple-expand",
                    "published": datetime.utcnow().isoformat() + "Z",
                    "full_text": "Apple announces expansion of its supplier base with strategic partnerships to strengthen supply chain resilience."
                },
                "expected_company": "AAPL",
                "expected_sentiment": "positive"
            },
            {
                "name": "Microsoft Neutral News",
                "data": {
                    "headline": "Microsoft reports quarterly earnings",
                    "url": "https://test.com/msft-earnings", 
                    "published": datetime.utcnow().isoformat() + "Z",
                    "full_text": "Microsoft Corporation released its quarterly financial results showing stable performance across all divisions."
                },
                "expected_company": "MSFT",
                "expected_sentiment": "neutral"
            }
        ]
        
        results = []
        
        for test_case in test_cases:
            try:
                logger.info(f"   📰 Sending: {test_case['name']}")
                
                response = requests.post(
                    f"{self.gateway_url}/v1/news",
                    json=test_case["data"],
                    timeout=10
                )
                
                if response.status_code == 200:
                    result = response.json()
                    logger.info(f"   ✅ Success: {result['news_id']} -> {result['status']}")
                    results.append({
                        "test_case": test_case,
                        "news_id": result["news_id"],
                        "success": True
                    })
                else:
                    logger.error(f"   ❌ Failed: {response.status_code} - {response.text}")
                    results.append({
                        "test_case": test_case,
                        "success": False,
                        "error": f"HTTP {response.status_code}"
                    })
                    
            except Exception as e:
                logger.error(f"   ❌ Exception: {e}")
                results.append({
                    "test_case": test_case,
                    "success": False,
                    "error": str(e)
                })
        
        return results
    
    def test_step_2_kafka_flow(self):
        """Step 2: Wait for Kafka processing."""
        logger.info("🔄 STEP 2: Waiting for Kafka Processing")
        logger.info("   ⏳ Giving processor time to consume messages...")
        
        # Wait for processing (in real system, you'd check Kafka consumer lag)
        for i in range(10):
            time.sleep(1)
            logger.info(f"   ⏱️  Waiting... {i+1}/10 seconds")
        
        logger.info("   ✅ Processing window complete")
        return True
    
    def test_step_3_redis_features(self):
        """Step 3: Check Redis for computed features."""
        logger.info("💾 STEP 3: Checking Redis Feature Store")
        
        try:
            # Get all feature keys
            feature_keys = self.redis_client.keys("feat:*")
            logger.info(f"   📊 Found {len(feature_keys)} feature keys in Redis")
            
            results = []
            
            for key in feature_keys:
                try:
                    value_json = self.redis_client.get(key)
                    if value_json:
                        feature_data = json.loads(value_json)
                        ticker = feature_data.get('ticker', 'UNKNOWN')
                        
                        logger.info(f"   🏢 {ticker}:")
                        logger.info(f"      • Negative news: {feature_data.get('neg_news_count_24h', 0)}")
                        logger.info(f"      • Positive news: {feature_data.get('pos_news_count_24h', 0)}")
                        logger.info(f"      • Sentiment EWM: {feature_data.get('sentiment_ewm_7d', 0.0)}")
                        logger.info(f"      • Version: {feature_data.get('_ver', 'unknown')}")
                        logger.info(f"      • Ingested: {feature_data.get('_ingest_ts', 'unknown')}")
                        
                        results.append({
                            "key": key,
                            "ticker": ticker,
                            "features": feature_data,
                            "success": True
                        })
                    
                except json.JSONDecodeError as e:
                    logger.error(f"   ❌ Invalid JSON in key {key}: {e}")
                    results.append({
                        "key": key,
                        "success": False,
                        "error": "Invalid JSON"
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"   ❌ Redis error: {e}")
            return []
    
    def test_step_4_feature_retrieval(self):
        """Step 4: Test feature retrieval (simulating scoring service)."""
        logger.info("🎯 STEP 4: Testing Feature Retrieval for Scoring")
        
        # Test companies we expect to have features
        test_tickers = ['TSLA', 'AAPL', 'MSFT']
        results = []
        
        for ticker in test_tickers:
            try:
                # Get latest feature for this ticker (simplified - in real system you'd use time-based lookup)
                pattern = f"feat:{ticker}:*"
                keys = self.redis_client.keys(pattern)
                
                if keys:
                    # Get the most recent (simplified)
                    latest_key = sorted(keys)[-1]
                    feature_json = self.redis_client.get(latest_key)
                    
                    if feature_json:
                        features = json.loads(feature_json)
                        logger.info(f"   📈 {ticker} Features Ready for Scoring:")
                        logger.info(f"      • Key: {latest_key}")
                        logger.info(f"      • Neg Count: {features.get('neg_news_count_24h', 0)}")
                        logger.info(f"      • Pos Count: {features.get('pos_news_count_24h', 0)}")
                        logger.info(f"      • EWM Sentiment: {features.get('sentiment_ewm_7d', 0.0)}")
                        
                        # Simulate risk score calculation
                        neg_count = features.get('neg_news_count_24h', 0)
                        pos_count = features.get('pos_news_count_24h', 0)
                        sentiment = features.get('sentiment_ewm_7d', 0.0)
                        
                        # Simple risk score (higher = more risky)
                        risk_score = 0.5 + (neg_count * 0.1) - (pos_count * 0.05) - (sentiment * 0.2)
                        risk_score = max(0.0, min(1.0, risk_score))  # Clamp to [0,1]
                        
                        logger.info(f"      🎲 Simulated Risk Score: {risk_score:.3f}")
                        
                        results.append({
                            "ticker": ticker,
                            "features": features,
                            "risk_score": risk_score,
                            "success": True
                        })
                    else:
                        logger.warning(f"   ⚠️  {ticker}: Key exists but no data")
                        results.append({
                            "ticker": ticker,
                            "success": False,
                            "error": "No data in key"
                        })
                else:
                    logger.warning(f"   ⚠️  {ticker}: No features found")
                    results.append({
                        "ticker": ticker,
                        "success": False,
                        "error": "No features found"
                    })
                    
            except Exception as e:
                logger.error(f"   ❌ {ticker}: Error retrieving features: {e}")
                results.append({
                    "ticker": ticker,
                    "success": False,
                    "error": str(e)
                })
        
        return results
    
    def run_full_test(self):
        """Run complete end-to-end test."""
        logger.info("🧪 STARTING END-TO-END PIPELINE TEST")
        logger.info("=" * 80)
        
        # Step 1: Send news through Gateway
        gateway_results = self.test_step_1_gateway_api()
        
        # Step 2: Wait for processing
        self.test_step_2_kafka_flow()
        
        # Step 3: Check Redis features
        redis_results = self.test_step_3_redis_features()
        
        # Step 4: Test feature retrieval
        scoring_results = self.test_step_4_feature_retrieval()
        
        # Summary
        logger.info("=" * 80)
        logger.info("📊 END-TO-END TEST SUMMARY")
        
        logger.info(f"🚪 Gateway API: {sum(1 for r in gateway_results if r['success'])}/{len(gateway_results)} successful")
        logger.info(f"💾 Redis Features: {len(redis_results)} feature sets stored")
        logger.info(f"🎯 Feature Retrieval: {sum(1 for r in scoring_results if r['success'])}/{len(scoring_results)} successful")
        
        # Overall success
        gateway_success = all(r['success'] for r in gateway_results)
        redis_success = len(redis_results) > 0
        scoring_success = any(r['success'] for r in scoring_results)
        
        if gateway_success and redis_success and scoring_success:
            logger.info("🎉 END-TO-END TEST PASSED!")
            logger.info("✅ Complete pipeline working: Gateway → Kafka → Processing → Redis → Scoring")
        else:
            logger.warning("⚠️  Some components need attention:")
            if not gateway_success:
                logger.warning("   - Gateway API issues")
            if not redis_success:
                logger.warning("   - No features stored in Redis")
            if not scoring_success:
                logger.warning("   - Feature retrieval issues")
        
        return {
            "gateway_results": gateway_results,
            "redis_results": redis_results,
            "scoring_results": scoring_results,
            "overall_success": gateway_success and redis_success and scoring_success
        }

def main():
    """Run the end-to-end test."""
    tester = E2EPipelineTest()
    
    # Test Redis connection first
    try:
        tester.redis_client.ping()
        logger.info("✅ Redis connection verified")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        return
    
    # Run the full test
    results = tester.run_full_test()
    
    # Exit code based on success
    exit_code = 0 if results["overall_success"] else 1
    exit(exit_code)

if __name__ == "__main__":
    main()
