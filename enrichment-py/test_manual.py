#!/usr/bin/env python3
"""
Manual test of the enrichment service without FastAPI server
"""

import json
from models import EnrichmentRequest
from enrichment_service import EnrichmentService

def test_enrichment_service():
    """Test the enrichment service directly."""
    print("🧪 TESTING ENRICHMENT SERVICE DIRECTLY")
    print("=" * 50)
    
    service = EnrichmentService()
    
    # Test cases
    test_cases = [
        {
            "name": "Tesla negative sentiment",
            "request": EnrichmentRequest(
                news_id="test-1",
                headline="Tesla production halts due to supply shortage",
                body="Tesla Inc. faces major supply chain disruption"
            )
        },
        {
            "name": "Apple positive sentiment", 
            "request": EnrichmentRequest(
                news_id="test-2",
                headline="Apple expands supplier network with growth partnerships",
                body="Apple Inc. announces significant expansion"
            )
        },
        {
            "name": "Multiple companies",
            "request": EnrichmentRequest(
                news_id="test-3",
                headline="Microsoft and Google partnership beats expectations",
                body="Collaboration between tech giants exceeds forecasts"
            )
        },
        {
            "name": "No companies",
            "request": EnrichmentRequest(
                news_id="test-4",
                headline="General economic outlook improves for Q4",
                body="Analysts predict better market conditions"
            )
        }
    ]
    
    for test_case in test_cases:
        print(f"\n📰 {test_case['name']}:")
        print(f"   Headline: {test_case['request'].headline}")
        
        companies = service.enrich_news(test_case['request'])
        
        if companies:
            for company in companies:
                print(f"   ✅ {company.ticker} | Role: {company.role} | Sentiment: {company.sentiment:+.1f}")
        else:
            print("   ❌ No companies detected")
    
    # Show service stats
    print("\n" + "=" * 50)
    print("📊 SERVICE STATISTICS:")
    stats = service.get_stats()
    for key, value in stats.items():
        print(f"   • {key}: {value}")
    
    print("\n✅ Manual enrichment service test complete!")

if __name__ == "__main__":
    test_enrichment_service()
