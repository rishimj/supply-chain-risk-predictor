#!/usr/bin/env python3
"""
Test script for shock detection mode.

Tests:
1. Pre-filter performance (keyword matching)
2. Shock detection (highly negative articles)
3. Async enrichment throughput
4. Redis shock feature storage

Usage:
    # Make sure shock mode is running
    docker-compose up -d flink-shock-detector
    
    # Run tests
    python test_shock_detection.py
"""

import asyncio
import aiohttp
import time
import json
import redis
from datetime import datetime, timezone
from typing import List, Dict


# Test configuration
GATEWAY_URL = "http://localhost:8080"
REDIS_URL = "redis://localhost:6379/0"


class ShockDetectionTester:
    """Test suite for shock detection mode."""
    
    def __init__(self):
        self.gateway_url = GATEWAY_URL
        self.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        
        # Test articles with shock keywords
        self.shock_articles = [
            {
                "ticker": "TSLA",
                "headline": "Tesla factory closure due to critical supply chain disruption",
                "body": "Tesla announces emergency shutdown of its Texas Gigafactory amid severe semiconductor shortage affecting global production."
            },
            {
                "ticker": "AAPL", 
                "headline": "Apple supply chain collapse threatens iPhone production",
                "body": "Apple faces unprecedented supply chain crisis as multiple suppliers report raw material shortages and manufacturing halts."
            },
            {
                "ticker": "MSFT",
                "headline": "Microsoft data centers hit by power outage and infrastructure failure",
                "body": "Microsoft Azure experiences massive outage due to critical infrastructure failure affecting cloud supply chain operations."
            },
            {
                "ticker": "AMZN",
                "headline": "Amazon warehouse network paralyzed by severe logistics crisis",
                "body": "Amazon reports critical logistics failure as port congestion and shipping disruptions bring delivery network to standstill."
            },
            {
                "ticker": "GOOGL",
                "headline": "Google chip shortage forces production shutdown",
                "body": "Google's hardware division faces critical component shortage leading to manufacturing halt across multiple product lines."
            }
        ]
        
        # Non-shock articles (should be filtered)
        self.non_shock_articles = [
            {
                "ticker": "NFLX",
                "headline": "Netflix announces new content library expansion",
                "body": "Netflix reports strong subscriber growth and plans to expand content offerings in new markets."
            },
            {
                "ticker": "META",
                "headline": "Meta stock rises on positive quarterly earnings",
                "body": "Meta Platforms beats earnings expectations with strong ad revenue growth and user engagement metrics."
            }
        ]
    
    def create_test_article(self, ticker: str, headline: str, body: str) -> Dict:
        """Create a test article payload."""
        return {
            "headline": headline,
            "url": f"https://test.com/{ticker.lower()}-{int(time.time())}",
            "published": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "full_text": body
        }
    
    async def test_shock_detection(self):
        """Test 1: Send shock articles and verify detection."""
        print("\n" + "="*80)
        print("TEST 1: Shock Detection")
        print("="*80)
        
        async with aiohttp.ClientSession() as session:
            shock_count = 0
            
            for article in self.shock_articles:
                payload = self.create_test_article(
                    article["ticker"],
                    article["headline"],
                    article["body"]
                )
                
                print(f"\n📤 Sending shock article: {article['ticker']}")
                print(f"   {article['headline'][:60]}...")
                
                try:
                    async with session.post(
                        f"{self.gateway_url}/v1/news",
                        json=payload,
                        timeout=10
                    ) as response:
                        if response.status == 200:
                            shock_count += 1
                            print(f"   ✅ Accepted by gateway")
                        else:
                            print(f"   ❌ Gateway error: {response.status}")
                
                except Exception as e:
                    print(f"   ❌ Failed: {e}")
                
                # Small delay between articles
                await asyncio.sleep(0.5)
            
            print(f"\n📊 Sent {shock_count}/{len(self.shock_articles)} shock articles")
            
            # Wait for processing
            print(f"\n⏳ Waiting 15 seconds for shock detection and windowing...")
            await asyncio.sleep(15)
            
            # Check Redis for shocks
            print(f"\n🔍 Checking Redis for shock features...")
            found_shocks = []
            
            for article in self.shock_articles:
                ticker = article["ticker"]
                key = f"shock:{ticker}:latest"
                
                if self.redis_client.exists(key):
                    data = json.loads(self.redis_client.get(key))
                    found_shocks.append({
                        "ticker": ticker,
                        "shock_count": data.get("shock_count_5m", 0),
                        "worst_sentiment": data.get("worst_sentiment_5m", 0),
                        "risk_score": data.get("risk_score_5m", 0)
                    })
                    print(f"   ✅ {ticker}: {data['shock_count_5m']} shocks, "
                          f"risk={data['risk_score_5m']:.3f}, "
                          f"sentiment={data['worst_sentiment_5m']:.3f}")
            
            if found_shocks:
                print(f"\n✅ Successfully detected {len(found_shocks)} companies with shocks!")
            else:
                print(f"\n⚠️  No shocks found in Redis yet (may need more time)")
            
            return len(found_shocks) > 0
    
    async def test_pre_filter(self):
        """Test 2: Verify non-shock articles are filtered."""
        print("\n" + "="*80)
        print("TEST 2: Pre-Filter (Non-Shock Articles)")
        print("="*80)
        
        async with aiohttp.ClientSession() as session:
            non_shock_count = 0
            
            for article in self.non_shock_articles:
                payload = self.create_test_article(
                    article["ticker"],
                    article["headline"],
                    article["body"]
                )
                
                print(f"\n📤 Sending non-shock article: {article['ticker']}")
                print(f"   {article['headline'][:60]}...")
                
                try:
                    async with session.post(
                        f"{self.gateway_url}/v1/news",
                        json=payload,
                        timeout=10
                    ) as response:
                        if response.status == 200:
                            non_shock_count += 1
                            print(f"   ✅ Accepted by gateway (should be pre-filtered)")
                
                except Exception as e:
                    print(f"   ❌ Failed: {e}")
            
            print(f"\n📊 Sent {non_shock_count} non-shock articles")
            
            # Wait a bit
            await asyncio.sleep(10)
            
            # These should NOT appear in shock features
            print(f"\n🔍 Verifying these are NOT in shock features...")
            filtered_count = 0
            
            for article in self.non_shock_articles:
                ticker = article["ticker"]
                key = f"shock:{ticker}:latest"
                
                if not self.redis_client.exists(key):
                    filtered_count += 1
                    print(f"   ✅ {ticker}: Not in shock features (correctly filtered)")
                else:
                    print(f"   ⚠️  {ticker}: Found in shock features (unexpected)")
            
            if filtered_count == len(self.non_shock_articles):
                print(f"\n✅ Pre-filter working correctly! "
                      f"{filtered_count}/{len(self.non_shock_articles)} filtered")
            else:
                print(f"\n⚠️  Pre-filter may not be working as expected")
            
            return filtered_count == len(self.non_shock_articles)
    
    async def test_throughput(self):
        """Test 3: Test high-volume shock detection."""
        print("\n" + "="*80)
        print("TEST 3: High-Volume Throughput")
        print("="*80)
        
        num_articles = 50
        print(f"\n📤 Sending {num_articles} shock articles rapidly...")
        
        async with aiohttp.ClientSession() as session:
            start_time = time.time()
            
            tasks = []
            for i in range(num_articles):
                # Rotate through shock articles
                article = self.shock_articles[i % len(self.shock_articles)]
                payload = self.create_test_article(
                    article["ticker"],
                    article["headline"] + f" ({i})",
                    article["body"]
                )
                
                task = session.post(
                    f"{self.gateway_url}/v1/news",
                    json=payload,
                    timeout=10
                )
                tasks.append(task)
            
            # Send all at once
            responses = await asyncio.gather(*tasks, return_exceptions=True)
            
            elapsed = time.time() - start_time
            success_count = sum(1 for r in responses if not isinstance(r, Exception))
            
            throughput = num_articles / elapsed
            
            print(f"\n✅ Sent {num_articles} articles in {elapsed:.2f}s")
            print(f"   Throughput: {throughput:.1f} articles/sec")
            print(f"   Success: {success_count}/{num_articles}")
            
            if throughput > 100:
                print(f"   🚀 Excellent! System handling high load")
            else:
                print(f"   ⚠️  Lower than expected throughput")
            
            return throughput
    
    async def check_shock_features(self):
        """Test 4: Examine shock feature format."""
        print("\n" + "="*80)
        print("TEST 4: Shock Feature Format")
        print("="*80)
        
        print(f"\n🔍 Checking all shock features in Redis...")
        
        # Find all shock keys
        shock_keys = self.redis_client.keys("shock:*:latest")
        
        if not shock_keys:
            print(f"   ⚠️  No shock features found")
            return False
        
        print(f"\n   Found {len(shock_keys)} companies with shocks:\n")
        
        for key in shock_keys:
            data = json.loads(self.redis_client.get(key))
            print(f"   📊 {data['ticker']}:")
            print(f"      Shock count: {data['shock_count_5m']}")
            print(f"      Worst sentiment: {data['worst_sentiment_5m']:.3f}")
            print(f"      Risk score: {data['risk_score_5m']:.3f}")
            print(f"      Headlines: {len(data.get('shock_headlines', []))}")
            if data.get('shock_headlines'):
                for headline in data['shock_headlines'][:2]:
                    print(f"        - {headline[:60]}...")
            print()
        
        return True


async def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("🚨 SHOCK DETECTION MODE TEST SUITE")
    print("="*80)
    print("\nThis tests the optimized shock-only detection pipeline")
    print("Make sure flink-shock-detector is running!\n")
    
    tester = ShockDetectionTester()
    
    try:
        # Test 1: Shock detection
        test1_pass = await tester.test_shock_detection()
        
        # Test 2: Pre-filter
        test2_pass = await tester.test_pre_filter()
        
        # Test 3: Throughput
        throughput = await tester.test_throughput()
        
        # Wait for processing
        print(f"\n⏳ Waiting 20 seconds for all processing to complete...")
        await asyncio.sleep(20)
        
        # Test 4: Check features
        test4_pass = await tester.check_shock_features()
        
        # Summary
        print("\n" + "="*80)
        print("📊 TEST SUMMARY")
        print("="*80)
        print(f"✅ Shock Detection: {'PASS' if test1_pass else 'FAIL'}")
        print(f"✅ Pre-Filter: {'PASS' if test2_pass else 'FAIL'}")
        print(f"✅ Throughput: {throughput:.1f} articles/sec")
        print(f"✅ Feature Format: {'PASS' if test4_pass else 'FAIL'}")
        
        all_pass = test1_pass and test2_pass and test4_pass and throughput > 50
        
        if all_pass:
            print(f"\n🎉 All tests PASSED! Shock detection working correctly!")
        else:
            print(f"\n⚠️  Some tests failed. Check logs above.")
        
    except Exception as e:
        print(f"\n❌ Test suite failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
