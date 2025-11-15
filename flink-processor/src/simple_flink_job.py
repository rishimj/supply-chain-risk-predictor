"""
Simplified Flink job for news processing - focusing on core functionality.
"""
import os
import json
import logging
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.functions import ProcessFunction
import redis

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleNewsProcessor(ProcessFunction):
    """Simple processor that extracts companies and writes to Redis."""
    
    def __init__(self):
        self.redis_client = None
        
    def open(self, runtime_context):
        """Initialize Redis connection."""
        redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
        self.redis_client = redis.from_url(redis_url, decode_responses=True)
        logger.info(f"Connected to Redis: {redis_url}")
        
    def process_element(self, value: str, ctx: ProcessFunction.Context, out):
        """Process news message and write to Redis."""
        try:
            # Parse news message
            news_data = json.loads(value)
            news_id = news_data.get('news_id', 'unknown')
            headline = news_data.get('headline', '')
            
            logger.info(f"Processing news: {news_id} - {headline[:50]}...")
            
            # Simple company extraction (mock for now)
            companies = []
            headline_lower = headline.lower()
            
            company_keywords = {
                'tesla': 'TSLA',
                'apple': 'AAPL', 
                'microsoft': 'MSFT',
                'amazon': 'AMZN',
                'google': 'GOOGL'
            }
            
            for keyword, ticker in company_keywords.items():
                if keyword in headline_lower:
                    # Simple sentiment
                    sentiment = 0.0
                    if any(neg in headline_lower for neg in ['halt', 'shortage', 'delay', 'cut']):
                        sentiment = -0.7
                    elif any(pos in headline_lower for pos in ['expand', 'growth', 'success']):
                        sentiment = 0.7
                    
                    companies.append({
                        'ticker': ticker,
                        'sentiment': sentiment
                    })
                    break  # Only first match
            
            # Write features to Redis for each company
            for company in companies:
                ticker = company['ticker']
                sentiment = company['sentiment']
                
                # Create feature data
                current_time = datetime.utcnow()
                epoch = int(current_time.timestamp())
                
                features = {
                    'ticker': ticker,
                    'window_end': current_time.isoformat() + 'Z',
                    'neg_news_count_24h': 1 if sentiment < 0 else 0,
                    'pos_news_count_24h': 1 if sentiment > 0 else 0,
                    'sentiment_ewm_7d': sentiment,
                    '_ver': 'v1',
                    '_ingest_ts': current_time.isoformat() + 'Z'
                }
                
                # Write to Redis
                redis_key = f"feat:{ticker}:{epoch}"
                redis_value = json.dumps(features)
                
                # Set with 7 day TTL
                self.redis_client.setex(redis_key, 7 * 24 * 3600, redis_value)
                
                # Also set latest key
                latest_key = f"feat:{ticker}:latest"
                self.redis_client.setex(latest_key, 7 * 24 * 3600, redis_value)
                
                logger.info(f"Written to Redis: {redis_key} -> {features}")
                
                # Output for downstream processing
                out.collect(json.dumps({
                    'ticker': ticker,
                    'sentiment': sentiment,
                    'processed_at': current_time.isoformat() + 'Z'
                }))
                
        except Exception as e:
            logger.error(f"Error processing news {value}: {e}")
            # Don't fail the job, just skip this message


def main():
    """Main entry point."""
    logger.info("Starting Simple Flink News Processor")
    
    # Environment configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    parallelism = int(os.getenv('FLINK_PARALLELISM', '1'))
    env.set_parallelism(parallelism)
    
    logger.info(f"Flink parallelism: {parallelism}")
    
    # Add Kafka connector JARs
    env.add_jars("file:///app/jars/flink-connector-kafka.jar")
    env.add_jars("file:///app/jars/kafka-clients.jar")
    
    # Configuration from environment
    kafka_brokers = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
    logger.info(f"Kafka brokers: {kafka_brokers}")
    
    # Kafka consumer for raw news
    kafka_props = {
        'bootstrap.servers': kafka_brokers,
        'group.id': 'simple-flink-processor',
        'auto.offset.reset': 'latest'
    }
    
    news_consumer = FlinkKafkaConsumer(
        topics='raw_news_fulltext',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Create the pipeline
    news_stream = env.add_source(news_consumer).name("kafka_source")
    
    # Process news messages
    processed_stream = news_stream.process(SimpleNewsProcessor()).name("process_news")
    
    # For debugging, print processed results
    processed_stream.print()
    
    logger.info("Starting Flink job execution...")
    
    # Execute the job
    env.execute("Simple News Processing Pipeline")


if __name__ == '__main__':
    main()