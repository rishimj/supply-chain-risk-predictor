#!/usr/bin/env python3
"""
Test script for tiered sentiment analysis and batch processing.

This script tests:
1. Batch enrichment endpoint
2. Tiered sentiment analysis (fast vs slow path)
3. Performance improvements

Usage:
    python test_tiered_batch.py
"""

import asyncio
import aiohttp
import time
import json
from datetime import datetime, timezone
from typing import List, Dict

# Test configuration
ENRICHMENT_URL = "http://localhost:8082"
BATCH_SIZE = 10
NUM_BATCHES = 10


async def test_single_enrichment():
    """Test single article enrichment (baseline)."""
    print("\n" + "="*80)
    print("TEST 1: Single Article Enrichment (Baseline)")
    print("="*80)
    
    article = {
        "news_id": "test_single",
        "headline": "Tesla reports supply chain disruption amid semiconductor shortage",
        "body": "Tesla's production has been impacted by ongoing semiconductor shortages.",
        "url": "https://test.com/single",
        "published": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }
    
    async with aiohttp.ClientSession() as session:
        start = time.time()
        async with session.post(f"{ENRICHMENT_URL}/v1/enrich", json=article) as response:
            result = await response.json()
            latency = (time.time() - start) * 1000
        
        print(f"✅ Single enrichment completed in {latency:.1f}ms")
        print(f"   Companies found: {len(result['companies'])}")
        if result['companies']:
            for company in result['companies']:
                print(f"   - {company['ticker']}: {company['sentiment']:.3f}")
        
        return latency


async def test_batch_enrichment():
    """Test batch enrichment endpoint."""
    print("\n" + "="*80)
    print(f"TEST 2: Batch Enrichment ({BATCH_SIZE} articles)")
    print("="*80)
    
    # Create test articles with mix of critical and routine news
    articles = []
    
    # Critical articles (should use DistilBERT)
    critical_headlines = [
        "Apple announces major acquisition of semiconductor supplier",
        "Microsoft reports quarterly earnings beat expectations",
        "Amazon warehouse network faces critical logistics crisis",
    ]
    
    # Routine articles (should use VADER)
    routine_headlines = [
        "Tesla stock price update",
        "Google announces minor product update",
        "Facebook reports user growth",
        "Netflix content library expansion",
    ]
    
    for i in range(BATCH_SIZE):
        if i < 3:
            headline = critical_headlines[i % len(critical_headlines)]
        else:
            headline = routine_headlines[i % len(routine_headlines)]
        
        articles.append({
            "news_id": f"test_batch_{i}",
            "headline": headline,
            "body": f"{headline}. Additional details about the story.",
            "url": f"https://test.com/batch/{i}",
            "published": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        })
    
    async with aiohttp.ClientSession() as session:
        start = time.time()
        async with session.post(
            f"{ENRICHMENT_URL}/v1/enrich/batch",
            json={"articles": articles}
        ) as response:
            result = await response.json()
            latency = (time.time() - start) * 1000
        
        total_companies = sum(len(r['companies']) for r in result['results'])
        avg_per_article = result['processing_time_ms'] / result['total_articles']
        
        print(f"✅ Batch enrichment completed in {latency:.1f}ms")
        print(f"   Total articles: {result['total_articles']}")
        print(f"   Total companies: {total_companies}")
        print(f"   Processing time: {result['processing_time_ms']:.1f}ms")
        print(f"   Avg per article: {avg_per_article:.1f}ms")
        print(f"   Speedup vs single: {(latency / BATCH_SIZE):.1f}ms per article")
        
        return result


async def test_sustained_throughput():
    """Test sustained throughput with multiple batches."""
    print("\n" + "="*80)
    print(f"TEST 3: Sustained Throughput ({NUM_BATCHES} batches of {BATCH_SIZE} articles)")
    print("="*80)
    
    async def send_batch(session, batch_id):
        articles = [
            {
                "news_id": f"throughput_{batch_id}_{i}",
                "headline": f"Test article {batch_id}-{i}",
                "body": "Test content for throughput testing.",
                "url": f"https://test.com/throughput/{batch_id}/{i}",
                "published": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
            }
            for i in range(BATCH_SIZE)
        ]
        
        start = time.time()
        async with session.post(
            f"{ENRICHMENT_URL}/v1/enrich/batch",
            json={"articles": articles}
        ) as response:
            result = await response.json()
            latency = (time.time() - start) * 1000
            return result, latency
    
    async with aiohttp.ClientSession() as session:
        start = time.time()
        
        # Send all batches in parallel
        tasks = [send_batch(session, i) for i in range(NUM_BATCHES)]
        results = await asyncio.gather(*tasks)
        
        total_time = time.time() - start
        
        # Calculate statistics
        total_articles = sum(r[0]['total_articles'] for r in results)
        total_companies = sum(sum(len(res['companies']) for res in r[0]['results']) for r in results)
        latencies = [r[1] for r in results]
        
        avg_latency = sum(latencies) / len(latencies)
        min_latency = min(latencies)
        max_latency = max(latencies)
        throughput = total_articles / total_time
        
        print(f"✅ Sustained throughput test completed")
        print(f"   Total articles: {total_articles}")
        print(f"   Total companies: {total_companies}")
        print(f"   Total time: {total_time:.2f}s")
        print(f"   Throughput: {throughput:.1f} articles/sec")
        print(f"   Avg batch latency: {avg_latency:.1f}ms")
        print(f"   Min/Max latency: {min_latency:.1f}ms / {max_latency:.1f}ms")
        
        return throughput


async def check_stats():
    """Check enrichment service statistics."""
    print("\n" + "="*80)
    print("TEST 4: Check Tiered Sentiment Statistics")
    print("="*80)
    
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{ENRICHMENT_URL}/stats") as response:
            stats = await response.json()
        
        print(f"📊 Enrichment Service Statistics:")
        print(f"   Sentiment Model: {stats.get('sentiment_model', 'N/A')}")
        print(f"   Total Requests: {stats.get('total_requests', 0)}")
        print(f"   Batch Requests: {stats.get('batch_requests', 0)}")
        print(f"   Fast Path (VADER): {stats.get('fast_path_count', 0)}")
        print(f"   Slow Path (DistilBERT): {stats.get('slow_path_count', 0)}")
        print(f"   Fast Path %: {stats.get('fast_path_percentage', 'N/A')}")
        print(f"   Total Companies Detected: {stats.get('total_companies_detected', 0)}")
        
        return stats


async def main():
    """Run all tests."""
    print("\n" + "="*80)
    print("🧪 TIERED + BATCH PROCESSING TEST SUITE")
    print("="*80)
    print(f"\nTesting enrichment service at: {ENRICHMENT_URL}")
    print(f"Batch size: {BATCH_SIZE}")
    print(f"Number of batches: {NUM_BATCHES}")
    
    try:
        # Test 1: Single enrichment (baseline)
        single_latency = await test_single_enrichment()
        await asyncio.sleep(1)
        
        # Test 2: Batch enrichment
        batch_result = await test_batch_enrichment()
        await asyncio.sleep(1)
        
        # Test 3: Sustained throughput
        throughput = await test_sustained_throughput()
        await asyncio.sleep(1)
        
        # Test 4: Check stats
        stats = await check_stats()
        
        # Summary
        print("\n" + "="*80)
        print("📊 TEST SUMMARY")
        print("="*80)
        print(f"✅ Single article latency: {single_latency:.1f}ms")
        print(f"✅ Batch processing: {BATCH_SIZE} articles in {batch_result['processing_time_ms']:.1f}ms")
        print(f"✅ Sustained throughput: {throughput:.1f} articles/sec")
        print(f"✅ Fast path percentage: {stats.get('fast_path_percentage', 'N/A')}")
        
        # Calculate improvement
        expected_batch_time = single_latency * BATCH_SIZE
        actual_batch_time = batch_result['processing_time_ms']
        speedup = expected_batch_time / actual_batch_time
        
        print(f"\n💡 Performance Improvement:")
        print(f"   Expected (no batching): {expected_batch_time:.1f}ms for {BATCH_SIZE} articles")
        print(f"   Actual (with batching): {actual_batch_time:.1f}ms for {BATCH_SIZE} articles")
        print(f"   Speedup: {speedup:.1f}x faster")
        
        # Estimate capacity
        articles_per_sec_per_replica = 1000 / (actual_batch_time / BATCH_SIZE)
        replicas_for_1000_per_sec = 1000 / articles_per_sec_per_replica
        
        print(f"\n🎯 Capacity Estimation:")
        print(f"   Articles/sec per replica: {articles_per_sec_per_replica:.1f}")
        print(f"   Replicas needed for 1000/sec: {replicas_for_1000_per_sec:.1f}")
        
        print("\n✅ All tests completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
