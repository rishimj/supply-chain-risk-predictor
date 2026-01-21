#!/usr/bin/env python3
"""
🚀 PERFORMANCE BENCHMARK SUITE
Measures end-to-end latency and maximum throughput of the supply chain risk predictor.

Usage:
    python benchmark_performance.py --mode latency    # Test E2E latency
    python benchmark_performance.py --mode throughput # Test max throughput
    python benchmark_performance.py --mode stress     # Stress test system
"""

import argparse
import asyncio
import json
import logging
import time
import statistics
import uuid
from datetime import datetime, timezone
from typing import List, Dict, Tuple
from dataclasses import dataclass, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import redis
import numpy as np

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('benchmark_results.log')
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class LatencyMetrics:
    """End-to-end latency measurement."""
    news_id: str
    submit_time: float
    gateway_response_time: float
    redis_found_time: float
    total_latency_ms: float
    gateway_latency_ms: float
    processing_latency_ms: float
    success: bool
    error: str = ""

@dataclass
class ThroughputMetrics:
    """Throughput measurement results."""
    total_requests: int
    successful_requests: int
    failed_requests: int
    duration_seconds: float
    requests_per_second: float
    avg_response_time_ms: float
    p95_response_time_ms: float
    p99_response_time_ms: float

class PerformanceBenchmark:
    """High-performance benchmark suite for supply chain risk predictor."""
    
    def __init__(self):
        self.gateway_url = "http://localhost:8080"
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.test_companies = [
            ("TSLA", "Tesla factory halts production due to supply chain issues"),
            ("AAPL", "Apple expands supplier network with new partnerships"), 
            ("MSFT", "Microsoft azure supply chain optimization success"),
            ("AMZN", "Amazon warehouse delays impact shipping network"),
            ("GOOGL", "Google supply chain AI improves vendor relations"),
            ("META", "Meta data centers face component shortage"),
            ("NVDA", "NVIDIA chip supply shortage affects automotive sector"),
            ("NFLX", "Netflix content delivery network expansion"),
            ("CRM", "Salesforce supply chain management platform launch"),
            ("ORCL", "Oracle database performance for supply chain analytics")
        ]
        
    def generate_test_article(self, ticker: str, headline_template: str) -> Dict:
        """Generate a test news article."""
        return {
            "headline": headline_template,
            "url": f"https://test.com/{ticker.lower()}-news-{uuid.uuid4().hex[:8]}",
            "published": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            "full_text": f"Detailed analysis of {headline_template} and its impact on supply chain operations."
        }

    async def measure_e2e_latency(self, num_samples: int = 10) -> List[LatencyMetrics]:
        """Measure end-to-end latency from gateway to Redis."""
        logger.info(f"🕐 Measuring E2E latency with {num_samples} samples")
        results = []
        
        for i in range(num_samples):
            ticker, headline = self.test_companies[i % len(self.test_companies)]
            article = self.generate_test_article(ticker, headline)
            
            # Start timing
            submit_time = time.time()
            
            try:
                # Submit to gateway
                response = requests.post(
                    f"{self.gateway_url}/v1/news",
                    json=article,
                    timeout=5.0
                )
                gateway_response_time = time.time()
                
                if response.status_code != 200:
                    results.append(LatencyMetrics(
                        news_id="", submit_time=submit_time, gateway_response_time=gateway_response_time,
                        redis_found_time=0, total_latency_ms=0, gateway_latency_ms=0,
                        processing_latency_ms=0, success=False, error=f"Gateway error: {response.status_code}"
                    ))
                    continue
                
                news_id = response.json().get('news_id', '')
                gateway_latency_ms = (gateway_response_time - submit_time) * 1000
                
                # Wait for processing and check Redis
                redis_found_time = None
                max_wait = 30  # 30 second timeout
                
                for attempt in range(max_wait * 10):  # Check every 100ms
                    await asyncio.sleep(0.1)
                    
                    # Check if features appeared in Redis
                    redis_keys = self.redis_client.keys(f"feat:{ticker}:*")
                    if redis_keys:
                        # Check if this is recent data (within last minute)
                        latest_key = max(redis_keys)
                        feature_data = self.redis_client.get(latest_key)
                        if feature_data:
                            feature_json = json.loads(feature_data)
                            ingest_time = datetime.fromisoformat(feature_json['_ingest_ts'].replace('Z', '+00:00'))
                            current_time = datetime.now(timezone.utc)
                            
                            if (current_time - ingest_time).total_seconds() < 60:
                                redis_found_time = time.time()
                                break
                
                if redis_found_time:
                    total_latency_ms = (redis_found_time - submit_time) * 1000
                    processing_latency_ms = (redis_found_time - gateway_response_time) * 1000
                    
                    results.append(LatencyMetrics(
                        news_id=news_id, submit_time=submit_time, 
                        gateway_response_time=gateway_response_time,
                        redis_found_time=redis_found_time, total_latency_ms=total_latency_ms,
                        gateway_latency_ms=gateway_latency_ms, processing_latency_ms=processing_latency_ms,
                        success=True
                    ))
                    
                    logger.info(f"✅ Sample {i+1}: {total_latency_ms:.1f}ms total (gateway: {gateway_latency_ms:.1f}ms, processing: {processing_latency_ms:.1f}ms)")
                else:
                    results.append(LatencyMetrics(
                        news_id=news_id, submit_time=submit_time,
                        gateway_response_time=gateway_response_time, redis_found_time=0,
                        total_latency_ms=0, gateway_latency_ms=gateway_latency_ms,
                        processing_latency_ms=0, success=False, error="Timeout waiting for Redis features"
                    ))
                    logger.warning(f"⚠️ Sample {i+1}: Timeout waiting for processing")
                    
            except Exception as e:
                logger.error(f"❌ Sample {i+1}: Error - {e}")
                results.append(LatencyMetrics(
                    news_id="", submit_time=submit_time, gateway_response_time=0,
                    redis_found_time=0, total_latency_ms=0, gateway_latency_ms=0,
                    processing_latency_ms=0, success=False, error=str(e)
                ))
                
        return results

    def measure_throughput(self, duration_seconds: int = 60, max_workers: int = 50) -> ThroughputMetrics:
        """Measure maximum throughput by flooding the system."""
        logger.info(f"🚀 Measuring throughput for {duration_seconds}s with {max_workers} workers")
        
        start_time = time.time()
        end_time = start_time + duration_seconds
        response_times = []
        successful_requests = 0
        failed_requests = 0
        
        def send_request() -> Tuple[bool, float]:
            """Send a single request and return (success, response_time_ms)."""
            request_start = time.time()
            try:
                ticker, headline = self.test_companies[np.random.randint(0, len(self.test_companies))]
                article = self.generate_test_article(ticker, headline)
                
                response = requests.post(
                    f"{self.gateway_url}/v1/news",
                    json=article,
                    timeout=2.0
                )
                
                response_time = (time.time() - request_start) * 1000
                return (response.status_code == 200, response_time)
                
            except Exception:
                response_time = (time.time() - request_start) * 1000
                return (False, response_time)

        # Use ThreadPoolExecutor for concurrent requests
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            
            while time.time() < end_time:
                # Submit new requests to maintain load
                if len(futures) < max_workers:
                    for _ in range(min(10, max_workers - len(futures))):
                        if time.time() >= end_time:
                            break
                        futures.append(executor.submit(send_request))
                
                # Collect completed futures
                completed = []
                for future in futures:
                    if future.done():
                        completed.append(future)
                        try:
                            success, response_time = future.result()
                            if success:
                                successful_requests += 1
                            else:
                                failed_requests += 1
                            response_times.append(response_time)
                        except Exception:
                            failed_requests += 1
                            response_times.append(2000)  # Timeout response time
                
                # Remove completed futures
                for future in completed:
                    futures.remove(future)
                    
                time.sleep(0.01)  # Small delay to prevent tight loop
            
            # Wait for remaining futures to complete
            for future in as_completed(futures, timeout=5):
                try:
                    success, response_time = future.result()
                    if success:
                        successful_requests += 1
                    else:
                        failed_requests += 1
                    response_times.append(response_time)
                except Exception:
                    failed_requests += 1
                    response_times.append(5000)
        
        actual_duration = time.time() - start_time
        total_requests = successful_requests + failed_requests
        
        return ThroughputMetrics(
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            duration_seconds=actual_duration,
            requests_per_second=total_requests / actual_duration if actual_duration > 0 else 0,
            avg_response_time_ms=statistics.mean(response_times) if response_times else 0,
            p95_response_time_ms=np.percentile(response_times, 95) if response_times else 0,
            p99_response_time_ms=np.percentile(response_times, 99) if response_times else 0
        )

    def stress_test(self, ramp_up_time: int = 60, peak_time: int = 120, max_rps: int = 100) -> Dict:
        """Perform stress test with ramping load."""
        logger.info(f"🔥 Stress test: ramp to {max_rps} RPS over {ramp_up_time}s, hold for {peak_time}s")
        
        results = {
            "ramp_up_phase": [],
            "peak_phase": None,
            "system_breakdown": None
        }
        
        # Ramp up phase
        for rps in range(1, max_rps + 1, max(1, max_rps // 10)):
            logger.info(f"Testing {rps} RPS...")
            metrics = self.measure_throughput(duration_seconds=10, max_workers=rps)
            results["ramp_up_phase"].append({
                "target_rps": rps,
                "actual_rps": metrics.requests_per_second,
                "success_rate": metrics.successful_requests / metrics.total_requests if metrics.total_requests > 0 else 0,
                "p95_latency": metrics.p95_response_time_ms
            })
            
            # Check if system is breaking down
            success_rate = metrics.successful_requests / metrics.total_requests if metrics.total_requests > 0 else 0
            if success_rate < 0.9 or metrics.p95_response_time_ms > 5000:
                results["system_breakdown"] = {
                    "breakdown_rps": rps,
                    "success_rate": success_rate,
                    "p95_latency": metrics.p95_response_time_ms
                }
                logger.warning(f"🚨 System breakdown detected at {rps} RPS!")
                break
        
        # Peak phase
        if not results["system_breakdown"]:
            logger.info(f"Peak phase: {max_rps} RPS for {peak_time}s")
            results["peak_phase"] = self.measure_throughput(duration_seconds=peak_time, max_workers=max_rps)
        
        return results

    def print_latency_report(self, results: List[LatencyMetrics]):
        """Print detailed latency analysis."""
        successful = [r for r in results if r.success]
        
        if not successful:
            logger.error("❌ No successful latency measurements!")
            return
        
        total_latencies = [r.total_latency_ms for r in successful]
        gateway_latencies = [r.gateway_latency_ms for r in successful]
        processing_latencies = [r.processing_latency_ms for r in successful]
        
        print("\n" + "="*60)
        print("📊 END-TO-END LATENCY REPORT")
        print("="*60)
        print(f"Successful measurements: {len(successful)}/{len(results)}")
        print(f"Success rate: {len(successful)/len(results)*100:.1f}%")
        print()
        
        print("🕐 Total E2E Latency (Gateway → Redis):")
        print(f"  Mean:    {statistics.mean(total_latencies):.1f} ms")
        print(f"  Median:  {statistics.median(total_latencies):.1f} ms") 
        print(f"  P95:     {np.percentile(total_latencies, 95):.1f} ms")
        print(f"  P99:     {np.percentile(total_latencies, 99):.1f} ms")
        print(f"  Min:     {min(total_latencies):.1f} ms")
        print(f"  Max:     {max(total_latencies):.1f} ms")
        print()
        
        print("🌐 Gateway Latency:")
        print(f"  Mean:    {statistics.mean(gateway_latencies):.1f} ms")
        print(f"  P95:     {np.percentile(gateway_latencies, 95):.1f} ms")
        print()
        
        print("⚙️  Processing Latency (Kafka → Stream → Redis):")
        print(f"  Mean:    {statistics.mean(processing_latencies):.1f} ms")
        print(f"  P95:     {np.percentile(processing_latencies, 95):.1f} ms")
        
        # Failed requests analysis
        failed = [r for r in results if not r.success]
        if failed:
            print("\n❌ Failed Requests:")
            error_counts = {}
            for r in failed:
                error_counts[r.error] = error_counts.get(r.error, 0) + 1
            
            for error, count in error_counts.items():
                print(f"  {error}: {count}")

    def print_throughput_report(self, metrics: ThroughputMetrics):
        """Print detailed throughput analysis."""
        print("\n" + "="*60)
        print("🚀 THROUGHPUT REPORT") 
        print("="*60)
        print(f"Duration: {metrics.duration_seconds:.1f} seconds")
        print(f"Total requests: {metrics.total_requests}")
        print(f"Successful: {metrics.successful_requests}")
        print(f"Failed: {metrics.failed_requests}")
        print(f"Success rate: {metrics.successful_requests/metrics.total_requests*100:.1f}%")
        print()
        print(f"🎯 Throughput: {metrics.requests_per_second:.1f} requests/second")
        print()
        print("⏱️  Response Times:")
        print(f"  Mean:    {metrics.avg_response_time_ms:.1f} ms")
        print(f"  P95:     {metrics.p95_response_time_ms:.1f} ms") 
        print(f"  P99:     {metrics.p99_response_time_ms:.1f} ms")

async def main():
    parser = argparse.ArgumentParser(description='Performance benchmark for supply chain risk predictor')
    parser.add_argument('--mode', choices=['latency', 'throughput', 'stress'], required=True,
                       help='Benchmark mode to run')
    parser.add_argument('--samples', type=int, default=20, 
                       help='Number of latency samples (default: 20)')
    parser.add_argument('--duration', type=int, default=60,
                       help='Throughput test duration in seconds (default: 60)')
    parser.add_argument('--workers', type=int, default=50,
                       help='Number of concurrent workers (default: 50)')
    parser.add_argument('--max-rps', type=int, default=100,
                       help='Maximum RPS for stress test (default: 100)')
    
    args = parser.parse_args()
    
    benchmark = PerformanceBenchmark()
    
    # Test connectivity
    try:
        response = requests.get(f"{benchmark.gateway_url}/healthz", timeout=5)
        if response.status_code != 200:
            logger.error("❌ Gateway not healthy!")
            return
        
        benchmark.redis_client.ping()
        logger.info("✅ Connectivity verified")
        
    except Exception as e:
        logger.error(f"❌ Connectivity check failed: {e}")
        return
    
    if args.mode == 'latency':
        logger.info(f"Starting latency benchmark with {args.samples} samples...")
        results = await benchmark.measure_e2e_latency(args.samples)
        benchmark.print_latency_report(results)
        
        # Save detailed results
        with open('latency_results.json', 'w') as f:
            json.dump([asdict(r) for r in results], f, indent=2)
        logger.info("📁 Detailed results saved to latency_results.json")
        
    elif args.mode == 'throughput':
        logger.info(f"Starting throughput benchmark for {args.duration}s with {args.workers} workers...")
        metrics = benchmark.measure_throughput(args.duration, args.workers)
        benchmark.print_throughput_report(metrics)
        
        # Save results
        with open('throughput_results.json', 'w') as f:
            json.dump(asdict(metrics), f, indent=2)
        logger.info("📁 Results saved to throughput_results.json")
        
    elif args.mode == 'stress':
        logger.info(f"Starting stress test up to {args.max_rps} RPS...")
        results = benchmark.stress_test(max_rps=args.max_rps)
        
        print("\n" + "="*60)
        print("🔥 STRESS TEST REPORT")
        print("="*60)
        
        if results["system_breakdown"]:
            bd = results["system_breakdown"]
            print(f"🚨 System breakdown at {bd['breakdown_rps']} RPS")
            print(f"   Success rate: {bd['success_rate']*100:.1f}%")
            print(f"   P95 latency: {bd['p95_latency']:.1f} ms")
        else:
            print("✅ System handled maximum load without breakdown")
        
        print("\n📈 Ramp-up results:")
        for phase in results["ramp_up_phase"]:
            print(f"  {phase['target_rps']:3d} RPS → {phase['actual_rps']:5.1f} actual ({phase['success_rate']*100:5.1f}% success, {phase['p95_latency']:6.1f}ms P95)")
        
        # Save results
        with open('stress_results.json', 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info("📁 Results saved to stress_results.json")

if __name__ == "__main__":
    asyncio.run(main())