#!/usr/bin/env python3
"""
Simplified news processor that demonstrates the end-to-end pipeline.
This replaces the full Flink job for testing purposes.
"""
import asyncio
import json
import logging
import redis
import time
from datetime import datetime
from kafka import KafkaConsumer
import sys
import os

# Add the flink-processor src to path
sys.path.append('flink-processor/src')

from src.models import NewsMessage, CompanyMentionEvent, CompanyFeatures
from src.enrichment_client import MockEnrichmentClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SimpleNewsProcessor:
    """Simplified processor that mimics Flink pipeline behavior."""
    
    def __init__(self):
        self.kafka_brokers = ['localhost:9092']
        self.redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
        self.enrichment_client = MockEnrichmentClient()
        
        # Feature aggregation state (in real Flink, this would be in windows)
        self.company_features = {}
        
    async def process_news_message(self, news_json: str) -> list:
        """Process a single news message through enrichment."""
        try:
            # Parse news message
            news = NewsMessage.from_json(news_json)
            logger.info(f"📰 Processing news: {news.news_id} - {news.headline}")
            
            # Call mock enrichment
            enrichment_response = await self.enrichment_client.enrich_news(news)
            logger.info(f"🏢 Found {len(enrichment_response.companies)} companies")
            
            # Create company mention events
            events = []
            event_timestamp = datetime.utcnow().isoformat() + "Z"
            
            for company in enrichment_response.companies:
                mention_event = CompanyMentionEvent(
                    news_id=news.news_id,
                    event_ts=event_timestamp,
                    ticker=company.ticker,
                    role=company.role,
                    sentiment=company.sentiment
                )
                events.append(mention_event)
                logger.info(f"  💼 {company.ticker}: {company.sentiment} sentiment")
            
            return events
            
        except Exception as e:
            logger.error(f"❌ Error processing news: {e}")
            return []
    
    def update_features(self, mention_events: list):
        """Update feature aggregations (simplified windowing)."""
        for event in mention_events:
            ticker = event.ticker
            
            # Initialize if not exists
            if ticker not in self.company_features:
                self.company_features[ticker] = {
                    'neg_count': 0,
                    'pos_count': 0,
                    'sentiment_sum': 0.0,
                    'sentiment_count': 0,
                    'sentiment_ewm': 0.0
                }
            
            features = self.company_features[ticker]
            
            # Update counts
            if event.sentiment < 0:
                features['neg_count'] += 1
            elif event.sentiment > 0:
                features['pos_count'] += 1
            
            # Update EWM (simplified)
            alpha = 0.1
            if features['sentiment_count'] == 0:
                features['sentiment_ewm'] = event.sentiment
            else:
                features['sentiment_ewm'] = alpha * event.sentiment + (1 - alpha) * features['sentiment_ewm']
            
            features['sentiment_count'] += 1
            
            # Create CompanyFeatures object
            company_features = CompanyFeatures(
                ticker=ticker,
                window_end=datetime.utcnow().isoformat() + "Z",
                neg_news_count_24h=features['neg_count'],
                pos_news_count_24h=features['pos_count'],
                sentiment_ewm_7d=round(features['sentiment_ewm'], 3)
            )
            
            # Store in Redis
            self.store_in_redis(company_features)
            logger.info(f"📊 Updated features for {ticker}: pos={features['pos_count']}, neg={features['neg_count']}, ewm={features['sentiment_ewm']:.3f}")
    
    def store_in_redis(self, features: CompanyFeatures):
        """Store features in Redis with TTL."""
        try:
            key = features.to_redis_key()
            value = features.to_redis_value()
            
            # Set with 7 day TTL
            self.redis_client.setex(key, 7 * 24 * 3600, value)
            logger.info(f"💾 Stored in Redis: {key}")
            
        except Exception as e:
            logger.error(f"❌ Redis storage error: {e}")
    
    async def run(self):
        """Main processing loop."""
        logger.info("🚀 Starting Simple News Processor")
        logger.info("📡 Connecting to Kafka...")
        
        try:
            # Create Kafka consumer
            consumer = KafkaConsumer(
                'raw_news_fulltext',
                bootstrap_servers=self.kafka_brokers,
                group_id='simple-processor',
                value_deserializer=lambda m: m.decode('utf-8'),
                auto_offset_reset='latest'  # Only process new messages
            )
            
            logger.info("✅ Connected to Kafka, waiting for messages...")
            logger.info("💡 Send a news article to test the pipeline!")
            
            # Process messages
            for message in consumer:
                logger.info(f"📨 Received message from partition {message.partition}, offset {message.offset}")
                
                # Process the news message
                mention_events = await self.process_news_message(message.value)
                
                if mention_events:
                    # Update features
                    self.update_features(mention_events)
                    logger.info("✅ Pipeline processing complete!")
                else:
                    logger.info("ℹ️  No companies found in this article")
                
                logger.info("-" * 60)
                
        except KeyboardInterrupt:
            logger.info("🛑 Stopping processor...")
        except Exception as e:
            logger.error(f"❌ Processor error: {e}")

def main():
    """Main entry point."""
    processor = SimpleNewsProcessor()
    
    # Test Redis connection
    try:
        processor.redis_client.ping()
        logger.info("✅ Redis connection successful")
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        return
    
    # Run the processor
    asyncio.run(processor.run())

if __name__ == "__main__":
    main()
