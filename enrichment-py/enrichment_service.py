"""
Enrichment Service Core Logic

This module implements the company detection and sentiment analysis
logic for the enrichment service.
"""

from typing import List, Set, Tuple
import logging
import asyncio
from models import EnrichmentRequest, CompanyMention
from company_database import KEYWORD_TO_TICKER, get_company_info
from sentiment_analyzer import create_sentiment_analyzer

logger = logging.getLogger(__name__)

class EnrichmentService:
    """Core enrichment service for detecting companies and analyzing sentiment."""
    
    def __init__(self):
        # Initialize lightweight DistilBERT sentiment analyzer
        self.sentiment_analyzer = create_sentiment_analyzer("distilbert_financial")
        logger.info(f"Initialized enrichment service with DistilBERT financial model and {len(KEYWORD_TO_TICKER)} company keywords")
    
    async def enrich_news(self, request: EnrichmentRequest) -> List[CompanyMention]:
        """
        Extract companies and sentiment from news article.
        
        Args:
            request: Enrichment request with news data
            
        Returns:
            List of company mentions with sentiment scores
        """
        companies = []
        
        # Combine headline and body for analysis
        full_text = request.headline
        if request.body:
            full_text += " " + request.body
            
        # Detect companies using keyword matching
        detected_companies = self._detect_companies(request.headline, full_text)
        
        for ticker, role in detected_companies:
            # Analyze sentiment for this specific company context using FinBERT
            sentiment = await self._analyze_company_sentiment(full_text, ticker)
            
            companies.append(CompanyMention(
                ticker=ticker,
                role=role,
                sentiment=sentiment
            ))
            
        logger.info(f"Enriched news {request.news_id}: found {len(companies)} companies")
        return companies
    
    def _detect_companies(self, headline: str, full_text: str) -> List[Tuple[str, str]]:
        """
        Detect companies mentioned in the text.
        
        Args:
            headline: News headline
            full_text: Full article text
            
        Returns:
            List of (ticker, role) tuples
        """
        detected = []
        seen_tickers = set()
        
        headline_lower = headline.lower()
        full_text_lower = full_text.lower()
        
        # Sort keywords by length (descending) to prefer longer, more specific matches
        sorted_keywords = sorted(KEYWORD_TO_TICKER.items(), key=lambda x: len(x[0]), reverse=True)
        
        for keyword, ticker in sorted_keywords:
            if ticker in seen_tickers:
                continue
                
            # Check if keyword is in the text with word boundaries
            # This prevents partial matches like "tesla" in "teslamotors"
            import re
            pattern = r'\b' + re.escape(keyword) + r'\b'
            if re.search(pattern, full_text_lower, re.IGNORECASE):
                # Determine role based on where the company is mentioned
                if re.search(pattern, headline_lower, re.IGNORECASE):
                    role = "primary"  # Mentioned in headline = primary focus
                else:
                    role = "mentioned"  # Only in body = mentioned
                    
                detected.append((ticker, role))
                seen_tickers.add(ticker)
                
        return detected
    
    async def _analyze_company_sentiment(self, text: str, ticker: str) -> float:
        """
        Analyze sentiment for a specific company in the context.
        
        Args:
            text: Full text to analyze
            ticker: Company ticker for context
            
        Returns:
            Sentiment score between -1.0 and +1.0
        """
        try:
            # Use lightweight DistilBERT for fast financial sentiment analysis
            result = await self.sentiment_analyzer.analyze_sentiment(
                text=text,
                company_context=ticker
            )
            
            # Log sentiment details for debugging
            logger.debug(f"DistilBERT sentiment for {ticker}: {result.sentiment_score:.3f} "
                        f"(confidence: {result.confidence:.3f}, model: {result.model_used})")
            
            if result.reasoning:
                logger.debug(f"Sentiment reasoning: {result.reasoning}")
            
            return result.sentiment_score
            
        except Exception as e:
            logger.error(f"Error analyzing sentiment for {ticker}: {e}")
            # Return neutral sentiment on error
            return 0.0
    
    def get_stats(self) -> dict:
        """Get enrichment service statistics."""
        return {
            "total_companies": len(set(KEYWORD_TO_TICKER.values())),
            "total_keywords": len(KEYWORD_TO_TICKER),
            "sentiment_model": self.sentiment_analyzer.model_type.value,
            "supply_chain_categories": len(self.sentiment_analyzer.supply_chain_keywords)
        }
