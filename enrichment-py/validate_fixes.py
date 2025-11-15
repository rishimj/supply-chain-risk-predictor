#!/usr/bin/env python3
"""
Quick validation script for key robustness fixes
"""

import sys
sys.path.append('.')

from models import EnrichmentRequest
from enrichment_service import EnrichmentService
from sentiment_analyzer import SentimentAnalyzer

def test_fixes():
    """Test key robustness fixes."""
    print("🔧 VALIDATING KEY FIXES")
    print("=" * 40)
    
    service = EnrichmentService()
    analyzer = SentimentAnalyzer()
    
    # Test 1: Boundary conditions
    print("\n1. Testing boundary conditions...")
    boundary_tests = [
        ("Teslamotors production", False, "Should NOT detect Tesla in compound word"),
        ("Tesla production", True, "Should detect Tesla as separate word"),
        ("cat food company", False, "Should NOT detect CAT from short 'cat' keyword"),
        ("caterpillar equipment", True, "Should detect CAT from 'caterpillar'"),
    ]
    
    for headline, should_detect, description in boundary_tests:
        request = EnrichmentRequest(news_id="test", headline=headline)
        companies = service.enrich_news(request)
        
        found_tesla = any(c.ticker == "TSLA" for c in companies)
        found_cat = any(c.ticker == "CAT" for c in companies)
        
        if "Tesla" in headline:
            result = found_tesla == should_detect
        else:
            result = found_cat == should_detect
            
        status = "✅" if result else "❌"
        print(f"   {status} {description}: {headline}")
        if not result:
            print(f"      Found companies: {[c.ticker for c in companies]}")
    
    # Test 2: Error handling
    print("\n2. Testing error handling...")
    try:
        # Mock an error in sentiment analysis
        original_method = analyzer.analyze_sentiment
        
        def error_method(text):
            if "error" in text.lower():
                raise Exception("Mock error")
            return original_method(text)
        
        analyzer.analyze_sentiment = error_method
        service.sentiment_analyzer = analyzer
        
        # This should not crash
        request = EnrichmentRequest(news_id="test", headline="Tesla error test")
        companies = service.enrich_news(request)
        
        print("   ✅ Error handling: Service handled exception gracefully")
        print(f"      Companies returned: {len(companies)}")
        
    except Exception as e:
        print(f"   ❌ Error handling: Service crashed with: {e}")
    
    # Test 3: Sentiment details
    print("\n3. Testing sentiment details...")
    details = analyzer.get_sentiment_details("")
    required_fields = ["sentiment", "negative_keywords", "positive_keywords", "negative_count", "positive_count"]
    
    missing_fields = [field for field in required_fields if field not in details]
    if not missing_fields:
        print("   ✅ Sentiment details: All required fields present")
    else:
        print(f"   ❌ Sentiment details: Missing fields: {missing_fields}")
    
    # Test 4: Empty input handling
    print("\n4. Testing empty inputs...")
    empty_tests = [
        ("", "Empty string"),
        (None, "None value"),
        ("   ", "Whitespace only"),
    ]
    
    for text, description in empty_tests:
        try:
            sentiment = analyzer.analyze_sentiment(text)
            details = analyzer.get_sentiment_details(text)
            
            if sentiment == 0.0 and len(details["negative_keywords"]) == 0:
                print(f"   ✅ {description}: Handled correctly")
            else:
                print(f"   ❌ {description}: Unexpected result")
        except Exception as e:
            print(f"   ❌ {description}: Crashed with {e}")
    
    print("\n" + "=" * 40)
    print("🎯 Fix validation complete!")

if __name__ == "__main__":
    test_fixes()
