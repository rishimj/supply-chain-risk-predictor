"""
Advanced sentiment analysis for supply chain risk assessment.
Uses transformer models to provide nuanced sentiment scoring.
"""
import logging
import re
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import asyncio

try:
    from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
    TRANSFORMERS_AVAILABLE = True
except ImportError:
    TRANSFORMERS_AVAILABLE = False

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    VADER_AVAILABLE = True
except ImportError:
    VADER_AVAILABLE = False

try:
    from textblob import TextBlob
    TEXTBLOB_AVAILABLE = True
except ImportError:
    TEXTBLOB_AVAILABLE = False


logger = logging.getLogger(__name__)


class SentimentModel(Enum):
    """Available sentiment analysis models."""
    DISTILBERT_FINANCIAL = "distilbert_financial"
    VADER = "vader"
    TEXTBLOB = "textblob"
    TIERED = "tiered"


@dataclass
class SentimentResult:
    """Sentiment analysis result."""
    sentiment_score: float  # -1.0 to +1.0
    confidence: float       # 0.0 to 1.0
    supply_chain_risk_factor: float  # Additional risk weighting
    model_used: str
    reasoning: Optional[str] = None


class SupplyChainSentimentAnalyzer:
    """
    Advanced sentiment analyzer specifically tuned for supply chain risk assessment.
    Combines multiple models and domain-specific heuristics.
    """
    
    def __init__(self, model_type: SentimentModel = SentimentModel.TIERED):
        """
        Initialize the sentiment analyzer.
        
        Args:
            model_type: Type of model to use for sentiment analysis
        """
        self.model_type = model_type
        self._distilbert_pipeline = None
        self._vader_analyzer = None
        
        # Supply chain specific keyword weights
        self.supply_chain_keywords = {
            # High negative impact
            'critical_disruption': {
                'keywords': ['supply chain disruption', 'manufacturing halt', 'factory closure', 
                           'semiconductor shortage', 'raw material shortage', 'logistics crisis',
                           'port congestion', 'shipping delay', 'supply shortage'],
                'risk_multiplier': 2.0,
                'sentiment_bias': -0.8
            },
            # Medium negative impact
            'operational_issues': {
                'keywords': ['production delay', 'inventory shortage', 'supplier issues',
                           'quality control', 'recall', 'compliance violation',
                           'transportation bottleneck', 'warehouse capacity'],
                'risk_multiplier': 1.5,
                'sentiment_bias': -0.6
            },
            # Low negative impact
            'market_concerns': {
                'keywords': ['demand uncertainty', 'price volatility', 'market slowdown',
                           'economic headwinds', 'competition pressure'],
                'risk_multiplier': 1.2,
                'sentiment_bias': -0.4
            },
            # Positive indicators
            'positive_developments': {
                'keywords': ['capacity expansion', 'new supplier', 'automation upgrade',
                           'efficiency improvement', 'cost reduction', 'partnership',
                           'diversification', 'resilience building'],
                'risk_multiplier': 0.8,
                'sentiment_bias': 0.6
            }
        }
        
        logger.info(f"Initializing sentiment analyzer with model: {model_type.value}")
        self._initialize_models()
    
    def _initialize_models(self):
        """Initialize the selected models."""
        try:
            if self.model_type == SentimentModel.DISTILBERT_FINANCIAL:
                if TRANSFORMERS_AVAILABLE:
                    self._initialize_distilbert_financial()
                else:
                    logger.warning("Transformers not available, falling back to VADER")
                    if VADER_AVAILABLE:
                        self._vader_analyzer = SentimentIntensityAnalyzer()
            
            elif self.model_type == SentimentModel.VADER:
                if VADER_AVAILABLE:
                    self._vader_analyzer = SentimentIntensityAnalyzer()
                else:
                    logger.warning("VADER not available")
            
            elif self.model_type == SentimentModel.TIERED:
                # Initialize both models for tiered approach
                if TRANSFORMERS_AVAILABLE:
                    self._initialize_distilbert_financial()
                if VADER_AVAILABLE:
                    self._vader_analyzer = SentimentIntensityAnalyzer()
                logger.info("Initialized tiered sentiment analysis: DistilBERT + VADER")
            
        except Exception as e:
            logger.warning(f"Error initializing models: {e}, falling back to basic analysis")
    
    def _initialize_distilbert_financial(self):
        """Initialize lightweight DistilBERT model for financial sentiment analysis."""
        try:
            # Use lightweight financial sentiment model - much faster than FinBERT
            self._distilbert_pipeline = pipeline(
                "sentiment-analysis",
                model="nlptown/bert-base-multilingual-uncased-sentiment",
                device=-1  # Use CPU for better compatibility
            )
            logger.info("DistilBERT financial sentiment model loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load DistilBERT financial model: {e}")
    
    def _extract_supply_chain_context(self, text: str) -> Dict[str, float]:
        """
        Extract supply chain specific context and risk factors.
        
        Args:
            text: Text to analyze
            
        Returns:
            Dictionary with risk factors and weights
        """
        text_lower = text.lower()
        context = {
            'risk_multiplier': 1.0,
            'sentiment_bias': 0.0,
            'matched_categories': []
        }
        
        for category, info in self.supply_chain_keywords.items():
            for keyword in info['keywords']:
                if keyword in text_lower:
                    context['risk_multiplier'] *= info['risk_multiplier']
                    context['sentiment_bias'] += info['sentiment_bias']
                    context['matched_categories'].append(category)
                    break  # Only count each category once
        
        # Normalize sentiment bias
        if len(context['matched_categories']) > 0:
            context['sentiment_bias'] /= len(context['matched_categories'])
        
        return context
    
    async def analyze_sentiment(self, text: str, company_context: str = "") -> SentimentResult:
        """
        Analyze sentiment with supply chain risk context.
        
        Args:
            text: Text to analyze (headline + body)
            company_context: Additional company context
            
        Returns:
            SentimentResult with sentiment score and risk assessment
        """
        if not text.strip():
            return SentimentResult(
                sentiment_score=0.0,
                confidence=0.0,
                supply_chain_risk_factor=1.0,
                model_used="none"
            )
        
        # Extract supply chain context
        supply_context = self._extract_supply_chain_context(text)
        
        # Choose analysis approach based on model type
        sentiment_scores = []
        confidences = []
        models_used = []
        
        if self.model_type == SentimentModel.TIERED:
            # Tiered approach: decide which model to use
            use_distilbert = self._should_use_distilbert(text, company_context)
            
            if use_distilbert and self._distilbert_pipeline:
                try:
                    distilbert_result = await self._analyze_with_distilbert(text)
                    sentiment_scores.append(distilbert_result[0])
                    confidences.append(distilbert_result[1])
                    models_used.append("distilbert_financial")
                except Exception as e:
                    logger.warning(f"DistilBERT analysis failed, falling back to VADER: {e}")
                    if self._vader_analyzer:
                        vader_result = self._analyze_with_vader(text)
                        sentiment_scores.append(vader_result[0])
                        confidences.append(vader_result[1])
                        models_used.append("vader_fallback")
            else:
                # Use fast VADER for bulk processing
                if self._vader_analyzer:
                    vader_result = self._analyze_with_vader(text)
                    sentiment_scores.append(vader_result[0])
                    confidences.append(vader_result[1])
                    models_used.append("vader_fast")
        
        elif self.model_type == SentimentModel.DISTILBERT_FINANCIAL:
            # DistilBERT Financial (fast and accurate for financial content)
            if self._distilbert_pipeline:
                try:
                    distilbert_result = await self._analyze_with_distilbert(text)
                    sentiment_scores.append(distilbert_result[0])
                    confidences.append(distilbert_result[1])
                    models_used.append("distilbert_financial")
                except Exception as e:
                    logger.warning(f"DistilBERT analysis failed: {e}")
        
        elif self.model_type == SentimentModel.VADER:
            # VADER (fast fallback)
            if self._vader_analyzer:
                try:
                    vader_result = self._analyze_with_vader(text)
                    sentiment_scores.append(vader_result[0])
                    confidences.append(vader_result[1])
                    models_used.append("vader")
                except Exception as e:
                    logger.warning(f"VADER analysis failed: {e}")
        
        # TextBlob (fallback)
        if TEXTBLOB_AVAILABLE and len(sentiment_scores) == 0:
            try:
                textblob_result = self._analyze_with_textblob(text)
                sentiment_scores.append(textblob_result[0])
                confidences.append(textblob_result[1])
                models_used.append("textblob")
            except Exception as e:
                logger.warning(f"TextBlob analysis failed: {e}")
        
        # Use single model result or fallback
        if sentiment_scores:
            weighted_sentiment = sentiment_scores[0]  # Single model approach
            avg_confidence = confidences[0]
        else:
            # Fallback to keyword-based analysis
            weighted_sentiment = supply_context['sentiment_bias']
            avg_confidence = 0.3
            models_used.append("keyword_fallback")
        
        # Apply supply chain context bias
        final_sentiment = weighted_sentiment + (supply_context['sentiment_bias'] * 0.3)
        final_sentiment = max(-1.0, min(1.0, final_sentiment))  # Clamp to [-1, 1]
        
        return SentimentResult(
            sentiment_score=final_sentiment,
            confidence=avg_confidence,
            supply_chain_risk_factor=supply_context['risk_multiplier'],
            model_used=models_used[0] if models_used else "none",
            reasoning=f"Categories: {supply_context['matched_categories']}" if supply_context['matched_categories'] else None
        )
    
    async def _analyze_with_distilbert(self, text: str) -> Tuple[float, float]:
        """Analyze with lightweight DistilBERT financial model."""
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._distilbert_pipeline, text[:512])  # Truncate for model limits
        
        # Model returns sentiment labels with scores
        # Convert multi-class sentiment to continuous score
        label = result[0]['label']
        confidence = result[0]['score']
        
        # Map label to sentiment score
        if 'negative' in label.lower() or '1' in label:
            sentiment_score = -1.0 * confidence
        elif 'positive' in label.lower() or '5' in label:
            sentiment_score = 1.0 * confidence
        elif 'neutral' in label.lower() or '3' in label:
            sentiment_score = 0.0
        else:
            # Handle numbered labels (1-5 star rating)
            if label.isdigit():
                stars = int(label)
                sentiment_score = ((stars - 3) / 2)  # Convert 1-5 to -1 to +1
            else:
                sentiment_score = 0.0
        
        return sentiment_score, confidence
    
    def _analyze_with_vader(self, text: str) -> Tuple[float, float]:
        """Analyze with VADER."""
        scores = self._vader_analyzer.polarity_scores(text)
        # VADER compound score is already in [-1, 1] range
        sentiment_score = scores['compound']
        # Use the absolute value as confidence
        confidence = abs(sentiment_score)
        
        return sentiment_score, confidence
    
    def _analyze_with_textblob(self, text: str) -> Tuple[float, float]:
        """Analyze with TextBlob as fallback."""
        blob = TextBlob(text)
        sentiment_score = blob.sentiment.polarity  # Already [-1, 1]
        confidence = abs(sentiment_score)  # Simple confidence measure
        
        return sentiment_score, confidence
    
    def _should_use_distilbert(self, text: str, company_context: str = "") -> bool:
        """
        Decide whether to use DistilBERT or fast VADER based on content analysis.
        
        Args:
            text: Text to analyze
            company_context: Company ticker context
            
        Returns:
            True if DistilBERT should be used, False for VADER
        """
        text_lower = text.lower()
        
        # Use DistilBERT for critical financial events
        critical_keywords = [
            'earnings', 'quarterly', 'profit', 'loss', 'revenue', 'guidance',
            'bankruptcy', 'acquisition', 'merger', 'ipo', 'dividend',
            'ceo', 'cfo', 'management', 'board', 'resignation'
        ]
        
        # Use DistilBERT for major supply chain events
        critical_supply_chain = [
            'supply chain disruption', 'factory closure', 'semiconductor shortage',
            'raw material shortage', 'logistics crisis', 'port congestion',
            'manufacturing halt', 'critical shortage'
        ]
        
        # Use DistilBERT for major companies (more impact)
        major_companies = {
            'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA',
            'JPM', 'JNJ', 'V', 'PG', 'UNH', 'HD', 'MA'
        }
        
        # Check for critical content
        for keyword in critical_keywords + critical_supply_chain:
            if keyword in text_lower:
                return True
        
        # Check if it's a major company
        if company_context and company_context.upper() in major_companies:
            return True
        
        # Check text length - use DistilBERT for longer, more complex content
        if len(text) > 200:
            return True
        
        # Default to fast VADER for routine news
        return False


# Factory function for easy instantiation
def create_sentiment_analyzer(model_type: str = "tiered") -> SupplyChainSentimentAnalyzer:
    """
    Create a sentiment analyzer with the specified model type.
    
    Args:
        model_type: One of 'tiered', 'distilbert_financial', 'vader', 'textblob'
        
    Returns:
        Configured sentiment analyzer
    """
    model_enum = SentimentModel(model_type.lower())
    return SupplyChainSentimentAnalyzer(model_enum)