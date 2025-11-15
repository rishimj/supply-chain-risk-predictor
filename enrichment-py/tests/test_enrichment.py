"""
Unit tests for the Enrichment Service

Tests the core enrichment logic following TDD principles from context.md.
"""

import pytest
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import EnrichmentRequest
from enrichment_service import EnrichmentService
from sentiment_analyzer import SentimentAnalyzer

class TestEnrichmentService:
    """Test cases for the enrichment service core logic."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.service = EnrichmentService()
    
    def test_tesla_negative_sentiment(self):
        """Test case from context.md: Tesla with negative sentiment."""
        request = EnrichmentRequest(
            news_id="test-1",
            headline="Panasonic plant halts; Tesla output threatened",
            body="Panasonic supply issues threaten Tesla production"
        )
        
        companies = self.service.enrich_news(request)
        
        # Should detect Tesla as primary (in headline)
        tesla_mentions = [c for c in companies if c.ticker == "TSLA"]
        assert len(tesla_mentions) == 1
        
        tesla = tesla_mentions[0]
        assert tesla.role == "primary"
        assert tesla.sentiment == -0.7  # Negative due to "halts" keyword
    
    def test_apple_positive_sentiment(self):
        """Test case from context.md: Apple with positive sentiment."""
        request = EnrichmentRequest(
            news_id="test-2", 
            headline="Apple expands supplier base in India",
            body="Apple Inc. announces expansion of manufacturing partnerships"
        )
        
        companies = self.service.enrich_news(request)
        
        # Should detect Apple as primary
        apple_mentions = [c for c in companies if c.ticker == "AAPL"]
        assert len(apple_mentions) == 1
        
        apple = apple_mentions[0]
        assert apple.role == "primary"
        assert apple.sentiment == 0.7  # Positive due to "expands" keyword
    
    def test_empty_text_returns_empty(self):
        """Test case from context.md: Empty text returns empty companies."""
        request = EnrichmentRequest(
            news_id="test-3",
            headline="",
            body=None
        )
        
        companies = self.service.enrich_news(request)
        assert companies == []
    
    def test_company_mentioned_in_body_only(self):
        """Test company mentioned in body gets 'mentioned' role."""
        request = EnrichmentRequest(
            news_id="test-4",
            headline="Supply chain disruption affects tech sector",
            body="Several companies including Microsoft are impacted by the shortage"
        )
        
        companies = self.service.enrich_news(request)
        
        # Microsoft should be detected as "mentioned" (not in headline)
        msft_mentions = [c for c in companies if c.ticker == "MSFT"]
        assert len(msft_mentions) == 1
        assert msft_mentions[0].role == "mentioned"
    
    def test_multiple_companies_detected(self):
        """Test multiple companies can be detected in one article."""
        request = EnrichmentRequest(
            news_id="test-5",
            headline="Apple and Microsoft partnership beats expectations",
            body="The collaboration between Apple and Microsoft exceeded analyst forecasts"
        )
        
        companies = self.service.enrich_news(request)
        
        # Should detect both companies
        tickers = {c.ticker for c in companies}
        assert "AAPL" in tickers
        assert "MSFT" in tickers
        
        # Both should be primary (in headline)
        for company in companies:
            assert company.role == "primary"
            assert company.sentiment == 0.7  # Positive due to "beats" keyword
    
    def test_supplier_company_mapping(self):
        """Test supplier company mapping (Foxconn -> Apple)."""
        request = EnrichmentRequest(
            news_id="test-6",
            headline="Foxconn manufacturing operations halt due to COVID",
            body="Foxconn's production stoppage affects supply chains"
        )
        
        companies = self.service.enrich_news(request)
        
        # Foxconn should map to Apple
        apple_mentions = [c for c in companies if c.ticker == "AAPL"]
        assert len(apple_mentions) == 1
        assert apple_mentions[0].sentiment == -0.7  # Negative due to "halt"
    
    def test_longer_keyword_preferred(self):
        """Test that longer keywords are matched over shorter ones."""
        request = EnrichmentRequest(
            news_id="test-7", 
            headline="Caterpillar heavy machinery demand surges in construction",
            body="Caterpillar Inc. reports strong quarterly results"
        )
        
        companies = self.service.enrich_news(request)
        
        # Should detect Caterpillar (CAT), not match "cat" substring
        cat_mentions = [c for c in companies if c.ticker == "CAT"]
        assert len(cat_mentions) == 1
        assert cat_mentions[0].sentiment == 0.7  # Positive due to "surges"

class TestSentimentAnalyzer:
    """Test cases for sentiment analysis logic."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = SentimentAnalyzer()
    
    def test_negative_keywords(self):
        """Test negative sentiment keywords."""
        negative_texts = [
            "Production halts due to supply shortage",
            "Company faces major delays in delivery", 
            "Manufacturing operations cut due to crisis",
            "Supply chain disruption drops quarterly results"
        ]
        
        for text in negative_texts:
            sentiment = self.analyzer.analyze_sentiment(text)
            assert sentiment == -0.7, f"Expected negative sentiment for: {text}"
    
    def test_positive_keywords(self):
        """Test positive sentiment keywords."""
        positive_texts = [
            "Company beats quarterly expectations significantly",
            "Manufacturing operations expand with new partnerships",
            "Supply chain optimization surges profit margins",
            "Production growth exceeds analyst forecasts"
        ]
        
        for text in positive_texts:
            sentiment = self.analyzer.analyze_sentiment(text)
            assert sentiment == 0.7, f"Expected positive sentiment for: {text}"
    
    def test_neutral_sentiment(self):
        """Test neutral sentiment (no keywords)."""
        neutral_texts = [
            "Company reports quarterly results",
            "Manufacturing update provided to investors",
            "Supply chain status remains unchanged"
        ]
        
        for text in neutral_texts:
            sentiment = self.analyzer.analyze_sentiment(text)
            assert sentiment == 0.0, f"Expected neutral sentiment for: {text}"
    
    def test_mixed_sentiment_negative_wins(self):
        """Test mixed sentiment where negative keywords outnumber positive."""
        text = "Company beats expectations but faces major production halts and delays"
        sentiment = self.analyzer.analyze_sentiment(text)
        assert sentiment == -0.7  # More negative keywords
    
    def test_mixed_sentiment_positive_wins(self):
        """Test mixed sentiment where positive keywords outnumber negative."""
        text = "Despite minor delays, company expands operations and beats growth targets"
        sentiment = self.analyzer.analyze_sentiment(text)
        assert sentiment == 0.7  # More positive keywords
    
    def test_empty_text(self):
        """Test empty text returns neutral sentiment."""
        assert self.analyzer.analyze_sentiment("") == 0.0
        assert self.analyzer.analyze_sentiment(None) == 0.0
    
    def test_sentiment_details(self):
        """Test detailed sentiment analysis."""
        text = "Production halts and delays hurt quarterly results"
        details = self.analyzer.get_sentiment_details(text)
        
        assert details["sentiment"] == -0.7
        assert "halts" in details["negative_keywords"]
        assert "delays" in details["negative_keywords"] 
        assert details["negative_count"] == 2
        assert details["positive_count"] == 0

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
