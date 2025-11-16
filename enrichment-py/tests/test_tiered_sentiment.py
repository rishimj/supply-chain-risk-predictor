"""
Tests for Tiered Sentiment Analysis functionality.

Tests the new tiered approach that intelligently selects between
DistilBERT and VADER based on content importance.
"""

import pytest
import sys
import os
import asyncio
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sentiment_analyzer import (
    SupplyChainSentimentAnalyzer,
    SentimentModel,
    create_sentiment_analyzer,
    SentimentResult
)
from models import EnrichmentRequest
from enrichment_service import EnrichmentService


class TestTieredModelSelection:
    """Test the intelligent model selection logic."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = create_sentiment_analyzer("tiered")
    
    def test_should_use_distilbert_for_earnings(self):
        """Test that earnings news triggers DistilBERT."""
        text = "Apple reports quarterly earnings beat expectations"
        result = self.analyzer._should_use_distilbert(text, "AAPL")
        assert result is True, "Earnings should trigger DistilBERT"
    
    def test_should_use_distilbert_for_profit_keywords(self):
        """Test that profit/loss keywords trigger DistilBERT."""
        texts = [
            "Company reports profit increase",
            "Firm faces quarterly loss",
            "Revenue guidance updated",
            "Dividend announcement expected"
        ]
        for text in texts:
            result = self.analyzer._should_use_distilbert(text, "")
            assert result is True, f"Should use DistilBERT for: {text}"
    
    def test_should_use_distilbert_for_supply_chain_disruption(self):
        """Test that supply chain disruptions trigger DistilBERT."""
        texts = [
            "Factory closure affects production",
            "Semiconductor shortage impacts industry",
            "Supply chain disruption threatens delivery",
            "Manufacturing halt due to logistics crisis"
        ]
        for text in texts:
            result = self.analyzer._should_use_distilbert(text, "")
            assert result is True, f"Should use DistilBERT for: {text}"
    
    def test_should_use_distilbert_for_major_companies(self):
        """Test that major companies trigger DistilBERT."""
        major_companies = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA"]
        text = "Company announces new product"
        
        for ticker in major_companies:
            result = self.analyzer._should_use_distilbert(text, ticker)
            assert result is True, f"Should use DistilBERT for major company: {ticker}"
    
    def test_should_use_distilbert_for_long_content(self):
        """Test that long content (>200 chars) triggers DistilBERT."""
        long_text = "This is a very long article " * 10  # >200 chars
        result = self.analyzer._should_use_distilbert(long_text, "")
        assert result is True, "Long content should trigger DistilBERT"
    
    def test_should_use_vader_for_routine_news(self):
        """Test that routine news uses fast VADER."""
        routine_texts = [
            "Small startup announces product update",
            "Minor company news",
            "General market update"
        ]
        
        for text in routine_texts:
            result = self.analyzer._should_use_distilbert(text, "UNKNOWN")
            assert result is False, f"Should use VADER for routine news: {text}"
    
    def test_should_use_vader_for_short_content(self):
        """Test that short content uses VADER."""
        short_text = "Brief update"
        result = self.analyzer._should_use_distilbert(short_text, "")
        assert result is False, "Short content should use VADER"


class TestTieredSentimentAnalysis:
    """Test the tiered sentiment analysis execution."""
    
    @pytest.mark.asyncio
    async def test_tiered_analyzes_critical_news_with_distilbert(self):
        """Test that critical news uses DistilBERT when available."""
        analyzer = create_sentiment_analyzer("tiered")
        
        # Mock DistilBERT pipeline
        mock_pipeline = Mock()
        mock_pipeline.return_value = [{"label": "POSITIVE", "score": 0.95}]
        analyzer._distilbert_pipeline = mock_pipeline
        analyzer._vader_analyzer = Mock()
        
        # Critical news that should trigger DistilBERT
        text = "Apple reports quarterly earnings beat expectations"
        result = await analyzer.analyze_sentiment(text, "AAPL")
        
        # Should have attempted to use DistilBERT
        assert mock_pipeline.called, "DistilBERT should be called for critical news"
        assert result.model_used in ["distilbert_financial", "vader_fallback"], \
            f"Expected DistilBERT or fallback, got: {result.model_used}"
    
    @pytest.mark.asyncio
    async def test_tiered_uses_vader_for_routine_news(self):
        """Test that routine news uses fast VADER."""
        analyzer = create_sentiment_analyzer("tiered")
        
        # Mock VADER
        mock_vader = Mock()
        mock_vader.polarity_scores.return_value = {"compound": 0.5}
        analyzer._vader_analyzer = mock_vader
        analyzer._distilbert_pipeline = None  # Not needed for routine news
        
        # Routine news
        text = "Small company announces minor update"
        result = await analyzer.analyze_sentiment(text, "UNKNOWN")
        
        # Should use VADER
        assert result.model_used == "vader_fast", \
            f"Expected vader_fast, got: {result.model_used}"
        assert -1.0 <= result.sentiment_score <= 1.0
    
    @pytest.mark.asyncio
    async def test_tiered_fallback_to_vader_on_distilbert_failure(self):
        """Test that VADER is used as fallback when DistilBERT fails."""
        analyzer = create_sentiment_analyzer("tiered")
        
        # Mock DistilBERT to fail
        mock_pipeline = Mock(side_effect=Exception("Model error"))
        analyzer._distilbert_pipeline = mock_pipeline
        
        # Mock VADER as fallback
        mock_vader = Mock()
        mock_vader.polarity_scores.return_value = {"compound": -0.3}
        analyzer._vader_analyzer = mock_vader
        
        # Critical news that should try DistilBERT first
        text = "Apple reports quarterly earnings"
        result = await analyzer.analyze_sentiment(text, "AAPL")
        
        # Should fallback to VADER
        assert result.model_used == "vader_fallback", \
            f"Expected vader_fallback, got: {result.model_used}"
        assert mock_vader.polarity_scores.called, "VADER should be called as fallback"


class TestSupplyChainKeywordDetection:
    """Test supply chain specific keyword detection and risk weighting."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.analyzer = create_sentiment_analyzer("tiered")
    
    def test_critical_disruption_keywords(self):
        """Test detection of critical supply chain disruption keywords."""
        text = "Factory closure affects production due to semiconductor shortage"
        context = self.analyzer._extract_supply_chain_context(text)
        
        assert context['risk_multiplier'] > 1.0, "Critical disruption should increase risk"
        assert 'critical_disruption' in context['matched_categories']
    
    def test_operational_issues_keywords(self):
        """Test detection of operational issue keywords."""
        text = "Production delay and inventory shortage impact delivery"
        context = self.analyzer._extract_supply_chain_context(text)
        
        assert context['risk_multiplier'] > 1.0, "Operational issues should increase risk"
        assert 'operational_issues' in context['matched_categories']
    
    def test_positive_developments_keywords(self):
        """Test detection of positive supply chain developments."""
        text = "Company announces capacity expansion and new supplier partnerships"
        context = self.analyzer._extract_supply_chain_context(text)
        
        assert context['risk_multiplier'] < 1.0, "Positive developments should decrease risk"
        assert 'positive_developments' in context['matched_categories']
    
    @pytest.mark.asyncio
    async def test_supply_chain_context_applied_to_sentiment(self):
        """Test that supply chain context affects final sentiment score."""
        analyzer = create_sentiment_analyzer("tiered")
        
        # Mock VADER
        mock_vader = Mock()
        mock_vader.polarity_scores.return_value = {"compound": 0.0}  # Neutral base
        analyzer._vader_analyzer = mock_vader
        
        # Text with critical disruption
        text = "Factory closure and supply chain disruption"
        result = await analyzer.analyze_sentiment(text)
        
        # Should have negative bias from supply chain keywords
        assert result.supply_chain_risk_factor > 1.0, "Should have increased risk factor"
        assert result.reasoning is not None, "Should have reasoning for matched categories"


class TestModelTypes:
    """Test different model type configurations."""
    
    @pytest.mark.asyncio
    async def test_distilbert_financial_model_type(self):
        """Test DistilBERT financial model type."""
        analyzer = create_sentiment_analyzer("distilbert_financial")
        
        # Mock DistilBERT
        mock_pipeline = Mock()
        mock_pipeline.return_value = [{"label": "POSITIVE", "score": 0.9}]
        analyzer._distilbert_pipeline = mock_pipeline
        
        text = "Company reports strong earnings"
        result = await analyzer.analyze_sentiment(text)
        
        assert result.model_used == "distilbert_financial", \
            f"Expected distilbert_financial, got: {result.model_used}"
    
    @pytest.mark.asyncio
    async def test_vader_model_type(self):
        """Test VADER-only model type."""
        analyzer = create_sentiment_analyzer("vader")
        
        # Mock VADER
        mock_vader = Mock()
        mock_vader.polarity_scores.return_value = {"compound": 0.6}
        analyzer._vader_analyzer = mock_vader
        
        text = "Company announces good news"
        result = await analyzer.analyze_sentiment(text)
        
        assert result.model_used == "vader", \
            f"Expected vader, got: {result.model_used}"
    
    def test_tiered_model_type_initialization(self):
        """Test that tiered model initializes both models."""
        analyzer = create_sentiment_analyzer("tiered")
        
        # Should attempt to initialize both (may fail if dependencies missing)
        assert analyzer.model_type == SentimentModel.TIERED
        # At least one model should be available
        assert analyzer._distilbert_pipeline is not None or analyzer._vader_analyzer is not None


class TestEnrichmentServiceIntegration:
    """Test tiered sentiment analysis integration with enrichment service."""
    
    @pytest.mark.asyncio
    async def test_enrichment_service_uses_tiered_analyzer(self):
        """Test that enrichment service uses tiered analyzer."""
        service = EnrichmentService()
        
        # Check that tiered analyzer is initialized
        assert service.sentiment_analyzer.model_type == SentimentModel.TIERED, \
            "Enrichment service should use tiered analyzer"
    
    @pytest.mark.asyncio
    async def test_enrichment_with_critical_news(self):
        """Test enrichment with critical news (should use DistilBERT)."""
        service = EnrichmentService()
        
        # Mock the sentiment analyzer to track model selection
        original_analyze = service.sentiment_analyzer.analyze_sentiment
        
        async def mock_analyze(text, company_context=""):
            # Check if DistilBERT would be selected
            use_distilbert = service.sentiment_analyzer._should_use_distilbert(text, company_context)
            result = await original_analyze(text, company_context)
            # Store which model was used
            result._test_use_distilbert = use_distilbert
            return result
        
        service.sentiment_analyzer.analyze_sentiment = mock_analyze
        
        # Critical news with major company
        request = EnrichmentRequest(
            news_id="test-critical",
            headline="Apple reports quarterly earnings beat expectations",
            body="Apple Inc. announced strong quarterly results exceeding analyst forecasts"
        )
        
        companies = await service.enrich_news(request)
        
        # Should detect Apple
        apple_mentions = [c for c in companies if c.ticker == "AAPL"]
        assert len(apple_mentions) > 0, "Should detect Apple"
        assert -1.0 <= apple_mentions[0].sentiment <= 1.0, "Should have valid sentiment"
    
    @pytest.mark.asyncio
    async def test_enrichment_with_routine_news(self):
        """Test enrichment with routine news (should use VADER)."""
        service = EnrichmentService()
        
        # Routine news
        request = EnrichmentRequest(
            news_id="test-routine",
            headline="Small startup announces product update",
            body="Minor company news about product features"
        )
        
        companies = await service.enrich_news(request)
        
        # May or may not detect companies, but should process without error
        assert isinstance(companies, list), "Should return list of companies"
        for company in companies:
            assert -1.0 <= company.sentiment <= 1.0, "Should have valid sentiment"


class TestSentimentResult:
    """Test SentimentResult data structure."""
    
    @pytest.mark.asyncio
    async def test_sentiment_result_structure(self):
        """Test that SentimentResult has all required fields."""
        analyzer = create_sentiment_analyzer("vader")
        
        # Mock VADER
        mock_vader = Mock()
        mock_vader.polarity_scores.return_value = {"compound": 0.5}
        analyzer._vader_analyzer = mock_vader
        
        text = "Test news article"
        result = await analyzer.analyze_sentiment(text)
        
        # Check all fields exist
        assert hasattr(result, 'sentiment_score')
        assert hasattr(result, 'confidence')
        assert hasattr(result, 'supply_chain_risk_factor')
        assert hasattr(result, 'model_used')
        assert hasattr(result, 'reasoning')
        
        # Check value ranges
        assert -1.0 <= result.sentiment_score <= 1.0
        assert 0.0 <= result.confidence <= 1.0
        assert result.supply_chain_risk_factor >= 0.0
        assert isinstance(result.model_used, str)
    
    @pytest.mark.asyncio
    async def test_empty_text_returns_neutral(self):
        """Test that empty text returns neutral sentiment."""
        analyzer = create_sentiment_analyzer("tiered")
        
        result = await analyzer.analyze_sentiment("")
        
        assert result.sentiment_score == 0.0, "Empty text should return neutral"
        assert result.confidence == 0.0, "Empty text should have zero confidence"
        assert result.model_used == "none", "Empty text should use no model"


class TestFactoryFunction:
    """Test the create_sentiment_analyzer factory function."""
    
    def test_create_tiered_analyzer(self):
        """Test creating tiered analyzer."""
        analyzer = create_sentiment_analyzer("tiered")
        assert analyzer.model_type == SentimentModel.TIERED
    
    def test_create_distilbert_analyzer(self):
        """Test creating DistilBERT analyzer."""
        analyzer = create_sentiment_analyzer("distilbert_financial")
        assert analyzer.model_type == SentimentModel.DISTILBERT_FINANCIAL
    
    def test_create_vader_analyzer(self):
        """Test creating VADER analyzer."""
        analyzer = create_sentiment_analyzer("vader")
        assert analyzer.model_type == SentimentModel.VADER
    
    def test_create_with_default(self):
        """Test creating analyzer with default (should be tiered)."""
        analyzer = create_sentiment_analyzer()
        assert analyzer.model_type == SentimentModel.TIERED
    
    def test_create_with_case_insensitive(self):
        """Test that model type is case insensitive."""
        analyzer1 = create_sentiment_analyzer("TIERED")
        analyzer2 = create_sentiment_analyzer("tiered")
        assert analyzer1.model_type == analyzer2.model_type


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

