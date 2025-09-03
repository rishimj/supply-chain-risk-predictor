"""
Data models for the Flink news processing pipeline.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional
import json


@dataclass
class NewsMessage:
    """Input news message from raw_news_fulltext Kafka topic."""
    news_id: str
    headline: str
    url: str
    published: str  # ISO-8601 timestamp
    full_text: Optional[str] = None

    @classmethod
    def from_json(cls, json_str: str) -> 'NewsMessage':
        data = json.loads(json_str)
        return cls(
            news_id=data['news_id'],
            headline=data['headline'],
            url=data['url'],
            published=data['published'],
            full_text=data.get('full_text')
        )

    def to_enrichment_request(self) -> dict:
        """Convert to format expected by enrichment API."""
        return {
            "news_id": self.news_id,
            "headline": self.headline,
            "body": self.full_text,
            "url": self.url,
            "published": self.published
        }


@dataclass
class CompanyMention:
    """Company mention from enrichment API response."""
    ticker: str
    role: str  # "primary" or "mentioned"
    sentiment: float  # -1.0 to +1.0


@dataclass
class EnrichmentResponse:
    """Response from enrichment API."""
    news_id: str
    companies: List[CompanyMention]

    @classmethod
    def from_json(cls, json_str: str) -> 'EnrichmentResponse':
        data = json.loads(json_str)
        companies = [
            CompanyMention(
                ticker=c['ticker'],
                role=c['role'],
                sentiment=c['sentiment']
            )
            for c in data['companies']
        ]
        return cls(
            news_id=data['news_id'],
            companies=companies
        )


@dataclass
class CompanyMentionEvent:
    """Individual company mention event for company_mentions topic."""
    news_id: str
    event_ts: str  # ISO-8601 timestamp
    ticker: str
    role: str
    sentiment: float

    def to_json(self) -> str:
        return json.dumps({
            "news_id": self.news_id,
            "event_ts": self.event_ts,
            "ticker": self.ticker,
            "role": self.role,
            "sentiment": self.sentiment
        })


@dataclass
class CompanyFeatures:
    """Aggregated features for a company (output to features_company topic and Redis)."""
    ticker: str
    window_end: str  # ISO-8601 timestamp
    neg_news_count_24h: int
    pos_news_count_24h: int
    sentiment_ewm_7d: float

    def to_json(self) -> str:
        return json.dumps({
            "ticker": self.ticker,
            "window_end": self.window_end,
            "neg_news_count_24h": self.neg_news_count_24h,
            "pos_news_count_24h": self.pos_news_count_24h,
            "sentiment_ewm_7d": self.sentiment_ewm_7d
        })

    def to_redis_key(self) -> str:
        """Generate Redis key: feat:{ticker}:{window_end_epoch}"""
        dt = datetime.fromisoformat(self.window_end.replace('Z', '+00:00'))
        epoch = int(dt.timestamp())
        return f"feat:{self.ticker}:{epoch}"

    def to_redis_value(self) -> str:
        """Generate Redis value with metadata."""
        dt = datetime.fromisoformat(self.window_end.replace('Z', '+00:00'))
        return json.dumps({
            "ticker": self.ticker,
            "window_end": self.window_end,
            "neg_news_count_24h": self.neg_news_count_24h,
            "pos_news_count_24h": self.pos_news_count_24h,
            "sentiment_ewm_7d": self.sentiment_ewm_7d,
            "_ver": "v1",
            "_ingest_ts": datetime.utcnow().isoformat() + "Z"
        })