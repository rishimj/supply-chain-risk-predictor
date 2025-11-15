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
    FINBERT = "finbert"
    ROBERTA = "roberta"
    VADER = "vader"
    TEXTBLOB = "textblob"
    HYBRID = "hybrid"


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
    
    def __init__(self, model_type: SentimentModel = SentimentModel.HYBRID):
        """
        Initialize the sentiment analyzer.
        
        Args:
            model_type: Type of model to use for sentiment analysis
        """
        self.model_type = model_type
        self._finbert_pipeline = None
        self._roberta_pipeline = None
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
            if self.model_type in [SentimentModel.FINBERT, SentimentModel.HYBRID]:
                if TRANSFORMERS_AVAILABLE:
                    self._initialize_finbert()
                else:
                    logger.warning("Transformers not available, falling back to lighter models")
            
            if self.model_type in [SentimentModel.ROBERTA, SentimentModel.HYBRID]:
                if TRANSFORMERS_AVAILABLE:
                    self._initialize_roberta()
            
            if self.model_type in [SentimentModel.VADER, SentimentModel.HYBRID]:
                if VADER_AVAILABLE:
                    self._vader_analyzer = SentimentIntensityAnalyzer()
                else:
                    logger.warning("VADER not available")
            
        except Exception as e:
            logger.warning(f"Error initializing models: {e}, falling back to basic analysis")
    
    def _initialize_finbert(self):
        """Initialize FinBERT model for financial sentiment analysis."""
        try:
            # Use ProsusAI/finbert model - specifically trained on financial texts
            self._finbert_pipeline = pipeline(
                "sentiment-analysis",
                model="ProsusAI/finbert",
                tokenizer="ProsusAI/finbert",
                device=-1  # Use CPU for better compatibility
            )
            logger.info("FinBERT model loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load FinBERT: {e}")
    
    def _initialize_roberta(self):
        """Initialize RoBERTa model for general sentiment analysis."""
        try:
            self._roberta_pipeline = pipeline(
                "sentiment-analysis",
                model="cardiffnlp/twitter-roberta-base-sentiment-latest",
                device=-1  # Use CPU
            )
            logger.info("RoBERTa sentiment model loaded successfully")
        except Exception as e:
            logger.warning(f"Could not load RoBERTa: {e}")
    
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
        
        # Get sentiment from multiple models
        sentiment_scores = []
        confidences = []
        models_used = []
        
        # FinBERT (best for financial/business content)
        if self._finbert_pipeline:
            try:
                finbert_result = await self._analyze_with_finbert(text)
                sentiment_scores.append(finbert_result[0])
                confidences.append(finbert_result[1])
                models_used.append("finbert")
            except Exception as e:
                logger.warning(f"FinBERT analysis failed: {e}")
        
        # RoBERTa (good general purpose)
        if self._roberta_pipeline:
            try:
                roberta_result = await self._analyze_with_roberta(text)
                sentiment_scores.append(roberta_result[0])
                confidences.append(roberta_result[1])
                models_used.append("roberta")
            except Exception as e:
                logger.warning(f"RoBERTa analysis failed: {e}")
        
        # VADER (fast, handles punctuation well)
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
        
        # Combine results
        if sentiment_scores:
            # Weighted average based on confidence
            if sum(confidences) > 0:
                weighted_sentiment = sum(s * c for s, c in zip(sentiment_scores, confidences)) / sum(confidences)
                avg_confidence = sum(confidences) / len(confidences)
            else:
                weighted_sentiment = sum(sentiment_scores) / len(sentiment_scores)
                avg_confidence = 0.5
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
            model_used="+".join(models_used),
            reasoning=f"Categories: {supply_context['matched_categories']}" if supply_context['matched_categories'] else None
        )
    
    async def _analyze_with_finbert(self, text: str) -> Tuple[float, float]:
        """Analyze with FinBERT model."""
        # Run in thread pool to avoid blocking
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._finbert_pipeline, text[:512])  # Truncate for model limits
        
        # FinBERT returns positive, negative, neutral
        sentiment_map = {'positive': 1.0, 'negative': -1.0, 'neutral': 0.0}
        sentiment_score = sentiment_map.get(result[0]['label'].lower(), 0.0)
        confidence = result[0]['score']
        
        return sentiment_score, confidence
    
    async def _analyze_with_roberta(self, text: str) -> Tuple[float, float]:
        """Analyze with RoBERTa model."""
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, self._roberta_pipeline, text[:512])
        
        # RoBERTa returns LABEL_0 (negative), LABEL_1 (neutral), LABEL_2 (positive)
        label_map = {'LABEL_0': -1.0, 'LABEL_1': 0.0, 'LABEL_2': 1.0}
        sentiment_score = label_map.get(result[0]['label'], 0.0)
        confidence = result[0]['score']
        
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


# Factory function for easy instantiation
def create_sentiment_analyzer(model_type: str = "hybrid") -> SupplyChainSentimentAnalyzer:
    """
    Create a sentiment analyzer with the specified model type.
    
    Args:
        model_type: One of 'finbert', 'roberta', 'vader', 'textblob', 'hybrid'
        
    Returns:
        Configured sentiment analyzer
    """
    model_enum = SentimentModel(model_type.lower())
    return SupplyChainSentimentAnalyzer(model_enum)