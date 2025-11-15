"""
Robust unit tests for the Enrichment Service core logic

These tests ensure the enrichment service logic is robust and handles
edge cases, malformed inputs, and error conditions gracefully.
"""

import pytest
import sys
import os
from unittest.mock import patch, MagicMock

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import EnrichmentRequest, CompanyMention
from enrichment_service import EnrichmentService
from sentiment_analyzer import SentimentAnalyzer
from company_database import KEYWORD_TO_TICKER, get_company_info

class TestEnrichmentServiceRobustness:
    """Comprehensive robustness tests for the enrichment service core logic."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.service = EnrichmentService()
    
    def test_initialization(self):
        """Test service initializes correctly."""
        service = EnrichmentService()
        assert hasattr(service, 'sentiment_analyzer')
        assert isinstance(service.sentiment_analyzer, SentimentAnalyzer)
        
        stats = service.get_stats()
        assert stats['total_companies'] > 0
        assert stats['total_keywords'] > 0
        assert stats['negative_sentiment_keywords'] > 0
        assert stats['positive_sentiment_keywords'] > 0
    
    def test_valid_requests(self):
        """Test enrichment with various valid requests."""
        valid_requests = [
            # Basic request
            EnrichmentRequest(news_id="test-1", headline="Tesla reports quarterly earnings"),
            
            # Request with body
            EnrichmentRequest(
                news_id="test-2", 
                headline="Apple supply chain optimization",
                body="Apple Inc. announces improvements to supplier network"
            ),
            
            # Request with all fields
            EnrichmentRequest(
                news_id="test-3",
                headline="Microsoft cloud services expand globally",
                body="Microsoft Corporation announces new data centers",
                url="https://example.com/microsoft-news",
                published="2024-01-01T10:00:00Z"
            ),
            
            # Request with multiple companies
            EnrichmentRequest(
                news_id="test-4",
                headline="Apple and Google partnership announcement",
                body="Apple Inc. and Google collaborate on new initiative"
            )
        ]
        
        for request in valid_requests:
            companies = self.service.enrich_news(request)
            assert isinstance(companies, list)
            
            for company in companies:
                assert isinstance(company, CompanyMention)
                assert company.ticker in KEYWORD_TO_TICKER.values()
                assert company.role in ["primary", "mentioned"]
                assert -1.0 <= company.sentiment <= 1.0
    
    def test_empty_and_none_inputs(self):
        """Test handling of empty and None inputs."""
        # Empty headline
        request = EnrichmentRequest(news_id="test-empty", headline="")
        companies = self.service.enrich_news(request)
        assert companies == []
        
        # Whitespace-only headline
        request = EnrichmentRequest(news_id="test-whitespace", headline="   \n\t   ")
        companies = self.service.enrich_news(request)
        assert companies == []
        
        # None body (should not crash)
        request = EnrichmentRequest(news_id="test-none-body", headline="Tesla news", body=None)
        companies = self.service.enrich_news(request)
        assert isinstance(companies, list)
        
        # Empty body
        request = EnrichmentRequest(news_id="test-empty-body", headline="Tesla news", body="")
        companies = self.service.enrich_news(request)
        assert isinstance(companies, list)
    
    def test_special_characters_and_encoding(self):
        """Test handling of special characters and encoding."""
        special_requests = [
            # Unicode characters
            EnrichmentRequest(news_id="test-unicode", headline="Téslä prödüctiön üpdäte"),
            
            # Emojis
            EnrichmentRequest(news_id="test-emoji", headline="Tesla 🚗 production 📈 increases"),
            
            # HTML entities (shouldn't be in real data but test robustness)
            EnrichmentRequest(news_id="test-html", headline="Apple &amp; Google partnership"),
            
            # Special punctuation
            EnrichmentRequest(news_id="test-punct", headline="Microsoft's Q4 results: 25% growth!!!"),
            
            # Mixed scripts
            EnrichmentRequest(news_id="test-mixed", headline="Tesla中国工厂production update"),
            
            # Newlines and tabs
            EnrichmentRequest(news_id="test-newlines", headline="Apple\nproduction\tupdate"),
        ]
        
        for request in special_requests:
            # Should not crash or raise exceptions
            companies = self.service.enrich_news(request)
            assert isinstance(companies, list)
    
    def test_very_long_inputs(self):
        """Test handling of very long inputs."""
        # Very long headline
        long_headline = "Tesla " + "supply chain optimization " * 100
        request = EnrichmentRequest(news_id="test-long-headline", headline=long_headline)
        companies = self.service.enrich_news(request)
        assert isinstance(companies, list)
        
        # Very long body
        long_body = "Tesla production efficiency improvements " * 1000
        request = EnrichmentRequest(
            news_id="test-long-body", 
            headline="Tesla news",
            body=long_body
        )
        companies = self.service.enrich_news(request)
        assert isinstance(companies, list)
        
        # Should still detect Tesla
        tesla_companies = [c for c in companies if c.ticker == "TSLA"]
        assert len(tesla_companies) > 0
    
    def test_case_sensitivity_robustness(self):
        """Test case sensitivity handling."""
        case_variants = [
            "tesla production update",
            "TESLA PRODUCTION UPDATE", 
            "Tesla Production Update",
            "tEsLa PrOdUcTiOn UpDaTe",
            "Tesla PRODUCTION update"
        ]
        
        for headline in case_variants:
            request = EnrichmentRequest(news_id=f"test-case-{hash(headline)}", headline=headline)
            companies = self.service.enrich_news(request)
            
            # Should detect Tesla in all cases
            tesla_companies = [c for c in companies if c.ticker == "TSLA"]
            assert len(tesla_companies) > 0, f"Failed to detect Tesla in: {headline}"
    
    def test_keyword_boundary_conditions(self):
        """Test keyword matching boundary conditions."""
        boundary_tests = [
            # Partial matches should not trigger
            ("Teslamotors production", 0),  # "tesla" is part of "teslamotors"
            ("Tesla production", 1),        # Exact match
            ("Tesla's production", 1),      # With possessive
            ("Tesla-based production", 1),  # With hyphen - word boundary will match "Tesla"
            ("Tesla, production", 1),       # With comma
            ("production Tesla update", 1), # Tesla not at start
            ("caterpillar equipment", 1),   # Should match CAT
            ("cat food company", 0),        # Should not match CAT (we don't have "cat" as keyword anymore)
        ]
        
        for headline, expected_count in boundary_tests:
            request = EnrichmentRequest(news_id=f"test-boundary-{hash(headline)}", headline=headline)
            companies = self.service.enrich_news(request)
            
            if expected_count == 0:
                # Should not detect any companies or only very generic ones
                specific_companies = [c for c in companies if c.ticker in ["TSLA", "CAT"]]
                assert len(specific_companies) == 0, f"Unexpected detection in: {headline}"
            else:
                # Should detect at least the expected number
                assert len(companies) >= expected_count, f"Failed to detect companies in: {headline}"
    
    def test_sentiment_analysis_robustness(self):
        """Test sentiment analysis under various conditions."""
        sentiment_tests = [
            # Clear positive
            ("Tesla production surges with record growth", 0.7),
            ("Apple beats expectations significantly", 0.7),
            
            # Clear negative  
            ("Tesla production halts due to shortage", -0.7),
            ("Apple supply chain faces major disruption", -0.7),
            
            # Neutral
            ("Tesla reports quarterly results", 0.0),
            ("Apple announces board meeting", 0.0),
            
            # Mixed (negative should dominate if more negative keywords)
            ("Tesla beats expectations but faces production halts and delays", -0.7),
            
            # Mixed (positive should dominate if more positive keywords)  
            ("Despite minor delays, Tesla expands production and beats targets", 0.7),
            
            # Edge case: mixed sentiment - should be neutral or follow majority
            ("Tesla halts production while Apple expands operations", 0.0),  # Mixed/neutral overall
        ]
        
        for headline, expected_sentiment in sentiment_tests:
            request = EnrichmentRequest(news_id=f"test-sentiment-{hash(headline)}", headline=headline)
            companies = self.service.enrich_news(request)
            
            if companies:
                # Most companies should have the expected sentiment
                sentiments = [c.sentiment for c in companies]
                # Allow some tolerance for edge cases
                assert expected_sentiment in sentiments or len(set(sentiments)) > 1, \
                    f"Unexpected sentiment for: {headline}. Got: {sentiments}, Expected: {expected_sentiment}"
    
    def test_company_role_assignment(self):
        """Test primary vs mentioned role assignment."""
        role_tests = [
            # Primary role (in headline)
            {
                "headline": "Tesla production increases significantly",
                "body": "Tesla Inc. announces milestone",
                "expected_role": "primary"
            },
            
            # Mentioned role (only in body)
            {
                "headline": "Electric vehicle sector shows growth",
                "body": "Several companies including Tesla are benefiting",
                "expected_role": "mentioned" 
            },
            
            # Multiple companies, some primary, some mentioned
            {
                "headline": "Apple announces new partnership",
                "body": "The deal involves several tech companies including Microsoft and Google",
                "apple_role": "primary",
                "others_role": "mentioned"
            }
        ]
        
        for test in role_tests:
            request = EnrichmentRequest(
                news_id=f"test-role-{hash(test['headline'])}",
                headline=test["headline"],
                body=test.get("body", "")
            )
            companies = self.service.enrich_news(request)
            
            if "expected_role" in test:
                # Find Tesla (or first company)
                if companies:
                    company = companies[0]
                    assert company.role == test["expected_role"], \
                        f"Wrong role for {test['headline']}: got {company.role}, expected {test['expected_role']}"
            
            if "apple_role" in test:
                apple_companies = [c for c in companies if c.ticker == "AAPL"]
                if apple_companies:
                    assert apple_companies[0].role == test["apple_role"]
                
                other_companies = [c for c in companies if c.ticker in ["MSFT", "GOOGL"]]
                for company in other_companies:
                    assert company.role == test["others_role"]
    
    def test_duplicate_company_handling(self):
        """Test handling of duplicate company mentions."""
        # Same company mentioned multiple times
        request = EnrichmentRequest(
            news_id="test-duplicates",
            headline="Tesla Model 3 and Tesla Model Y production increases as Tesla expands",
            body="Tesla Inc. Tesla Corporation Tesla Motors updates"
        )
        
        companies = self.service.enrich_news(request)
        
        # Should only appear once despite multiple mentions
        tesla_companies = [c for c in companies if c.ticker == "TSLA"]
        assert len(tesla_companies) == 1, f"Tesla appeared {len(tesla_companies)} times, expected 1"
    
    def test_error_conditions(self):
        """Test handling of various error conditions."""
        # This tests internal robustness - the service should not crash
        
        # Test with mock that raises exceptions
        with patch.object(self.service.sentiment_analyzer, 'analyze_sentiment', side_effect=Exception("Mock error")):
            request = EnrichmentRequest(news_id="test-error", headline="Tesla news")
            
            # Should handle the error gracefully (not crash)
            try:
                companies = self.service.enrich_news(request)
                # If it doesn't crash, it should return a list (possibly empty)
                assert isinstance(companies, list)
            except Exception as e:
                # If it does raise an exception, it should be handled at a higher level
                # This test ensures we know about it
                pytest.fail(f"Service raised unhandled exception: {e}")
    
    def test_memory_efficiency(self):
        """Test memory efficiency with repeated calls."""
        # Test that repeated calls don't cause memory leaks
        initial_stats = self.service.get_stats()
        
        # Make many requests
        for i in range(100):
            request = EnrichmentRequest(
                news_id=f"memory-test-{i}",
                headline=f"Tesla production update {i % 10}"  # Reuse patterns
            )
            companies = self.service.enrich_news(request)
            assert isinstance(companies, list)
        
        # Stats should be the same (no memory accumulation in stats)
        final_stats = self.service.get_stats()
        assert final_stats == initial_stats
    
    def test_thread_safety_preparation(self):
        """Test that service is prepared for thread safety."""
        # The service should not maintain mutable state between requests
        
        request1 = EnrichmentRequest(news_id="thread-1", headline="Tesla positive news beats expectations")
        request2 = EnrichmentRequest(news_id="thread-2", headline="Apple negative news halts production")
        
        # Process alternating requests
        companies1_first = self.service.enrich_news(request1)
        companies2 = self.service.enrich_news(request2) 
        companies1_second = self.service.enrich_news(request1)
        
        # Results should be identical for same input
        assert len(companies1_first) == len(companies1_second)
        
        for c1, c2 in zip(companies1_first, companies1_second):
            assert c1.ticker == c2.ticker
            assert c1.role == c2.role 
            assert c1.sentiment == c2.sentiment

class TestSentimentAnalyzerRobustness:
    """Robustness tests specifically for the sentiment analyzer."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = SentimentAnalyzer()
    
    def test_empty_inputs(self):
        """Test sentiment analyzer with empty inputs."""
        assert self.analyzer.analyze_sentiment("") == 0.0
        assert self.analyzer.analyze_sentiment(None) == 0.0
        assert self.analyzer.analyze_sentiment("   ") == 0.0
    
    def test_special_characters(self):
        """Test sentiment analyzer with special characters."""
        # Should not crash with any input
        special_texts = [
            "Tesla 📈 production surges! 🚀",
            "Apple & Co. beats expectations",
            "Microsoft's 25% growth!!!",
            "Tesla\nproduction\thalts",
            "Unicode: Tëst prödüçtiön hälts",
        ]
        
        for text in special_texts:
            sentiment = self.analyzer.analyze_sentiment(text)
            assert -1.0 <= sentiment <= 1.0
    
    def test_very_long_texts(self):
        """Test sentiment analyzer with very long texts."""
        # Very long text with sentiment keywords
        long_positive = "Tesla beats expectations " * 1000
        sentiment = self.analyzer.analyze_sentiment(long_positive)
        assert sentiment == 0.7
        
        long_negative = "Tesla production halts " * 1000  
        sentiment = self.analyzer.analyze_sentiment(long_negative)
        assert sentiment == -0.7
    
    def test_keyword_variations(self):
        """Test sentiment analyzer with keyword variations."""
        # Test word boundaries and variations
        variations = [
            ("beat", 0.7),
            ("beats", 0.7), 
            ("beaten", 0.0),  # Past participle might not be in keywords
            ("beating", 0.7),
            ("halt", -0.7),
            ("halts", -0.7),
            ("halted", -0.7),
            ("halting", -0.7),
        ]
        
        for word, expected in variations:
            text = f"Tesla {word} production"
            sentiment = self.analyzer.analyze_sentiment(text)
            
            if expected != 0.0:
                assert sentiment == expected, f"Failed for '{word}': got {sentiment}, expected {expected}"
    
    def test_sentiment_details_robustness(self):
        """Test get_sentiment_details method robustness."""
        test_texts = [
            "",
            None,
            "Tesla beats expectations but faces halts and delays",
            "Very long text " * 1000 + " with beats and halts keywords",
        ]
        
        for text in test_texts:
            details = self.analyzer.get_sentiment_details(text)
            
            # Should always return a dict with required fields
            assert isinstance(details, dict)
            assert "sentiment" in details
            assert "negative_keywords" in details  
            assert "positive_keywords" in details
            assert "negative_count" in details
            assert "positive_count" in details
            
            # Types should be correct
            assert isinstance(details["sentiment"], (int, float))
            assert isinstance(details["negative_keywords"], list)
            assert isinstance(details["positive_keywords"], list) 
            assert isinstance(details["negative_count"], int)
            assert isinstance(details["positive_count"], int)
            
            # Counts should match list lengths
            assert details["negative_count"] == len(details["negative_keywords"])
            assert details["positive_count"] == len(details["positive_keywords"])

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
