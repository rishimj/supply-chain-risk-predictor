"""
Simplified Flink job for processing news articles through enrichment and feature aggregation.
Writes features directly to Redis for online serving.
"""
import os
import json
import logging
import asyncio
from datetime import datetime, timedelta
from typing import Iterable

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.time import Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.functions import ProcessFunction, ProcessAllWindowFunction, SinkFunction
import redis

from models import NewsMessage, CompanyMentionEvent, CompanyFeatures
from enrichment_client import EnrichmentClient, MockEnrichmentClient
from alerting import create_alert_manager, AlertManager


logger = logging.getLogger(__name__)


class NewsEnrichmentFunction(ProcessFunction):
    """Process function that calls enrichment API and outputs company mentions."""
    
    def __init__(self, enrichment_url: str, use_mock: bool = False):
        self.enrichment_url = enrichment_url
        self.use_mock = use_mock
        self.client = None
    
    def open(self, runtime_context):
        """Initialize the enrichment client."""
        if self.use_mock:
            self.client = MockEnrichmentClient()
        else:
            self.client = EnrichmentClient(self.enrichment_url)
    
    def process_element(self, value: str, ctx: ProcessFunction.Context, out):
        """Process individual news message through enrichment."""
        try:
            # Parse news message
            news = NewsMessage.from_json(value)
            logger.info(f"Processing news: {news.news_id}")
            
            # Call enrichment API (using asyncio.run for now - not ideal but works)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                enrichment_response = loop.run_until_complete(self.client.enrich_news(news))
            finally:
                loop.close()
            
            # Convert to individual company mention events
            event_timestamp = datetime.utcnow().isoformat() + "Z"
            
            for company in enrichment_response.companies:
                mention_event = CompanyMentionEvent(
                    news_id=news.news_id,
                    event_ts=event_timestamp,
                    ticker=company.ticker,
                    role=company.role,
                    sentiment=company.sentiment
                )
                
                logger.info(f"Found company: {company.ticker} (sentiment: {company.sentiment})")
                out.collect(mention_event.to_json())
                
        except Exception as e:
            logger.error(f"Error processing news {value}: {e}")
            # Don't fail the job, just skip this message


class FeatureAggregator:
    """Aggregates company mentions into features."""
    
    def __init__(self):
        self.neg_count = 0
        self.pos_count = 0
        self.sentiment_sum = 0.0
        self.sentiment_count = 0
        self.sentiment_ewm = 0.0
        self.alpha = 0.1  # EWM decay factor
    
    def add_mention(self, sentiment: float):
        """Add a company mention to the aggregation."""
        if sentiment < 0:
            self.neg_count += 1
        elif sentiment > 0:
            self.pos_count += 1
        
        # Update exponential weighted moving average
        if self.sentiment_count == 0:
            self.sentiment_ewm = sentiment
        else:
            self.sentiment_ewm = self.alpha * sentiment + (1 - self.alpha) * self.sentiment_ewm
        
        self.sentiment_count += 1
    
    def get_features(self, ticker: str, window_end: datetime) -> CompanyFeatures:
        """Get the aggregated features for 5-minute window."""
        # Calculate overall sentiment score for this 5-minute window
        overall_sentiment = round(self.sentiment_ewm, 3) if self.sentiment_count > 0 else 0.0
        
        # Calculate supply chain risk score based on sentiment and counts
        # Risk factors:
        # - More negative sentiment = higher risk
        # - More total mentions = higher impact
        # - Supply chain keywords already weighted in sentiment analysis
        
        if self.sentiment_count == 0:
            risk_score = 0.0  # No news = no risk
        else:
            # Base risk from sentiment: -1.0 sentiment = 1.0 risk, +1.0 sentiment = 0.0 risk
            sentiment_risk = max(0.0, min(1.0, (1.0 - overall_sentiment) / 2.0))
            
            # Volume amplifier: more mentions = higher impact
            total_mentions = self.neg_count + self.pos_count
            volume_multiplier = min(1.5, 1.0 + (total_mentions - 1) * 0.1)  # Cap at 1.5x
            
            # Negative bias: if mostly negative mentions, increase risk
            if total_mentions > 0:
                negative_ratio = self.neg_count / total_mentions
                negative_bias = 1.0 + (negative_ratio - 0.5) * 0.5  # 0.75x to 1.25x
            else:
                negative_bias = 1.0
            
            # Final risk score
            risk_score = min(1.0, sentiment_risk * volume_multiplier * negative_bias)
            
        risk_score = round(risk_score, 3)
        
        return CompanyFeatures(
            ticker=ticker,
            window_end=window_end.isoformat() + "Z",
            neg_news_count_5m=self.neg_count,
            pos_news_count_5m=self.pos_count,
            sentiment_score_5m=overall_sentiment,
            risk_score_5m=risk_score
        )


class CompanyFeaturesWindowFunction(ProcessAllWindowFunction):
    """Window function that aggregates company mentions into features."""
    
    def process(self, context, elements: Iterable[str], out):
        """Process all elements in the window."""
        # Group mentions by ticker
        ticker_aggregators = {}
        element_count = 0
        
        for element_json in elements:
            element_count += 1
            try:
                element = json.loads(element_json)
                ticker = element['ticker']
                sentiment = element['sentiment']
                
                if ticker not in ticker_aggregators:
                    ticker_aggregators[ticker] = FeatureAggregator()
                
                ticker_aggregators[ticker].add_mention(sentiment)
                
            except Exception as e:
                logger.error(f"Error processing mention: {e}")
                continue
        
        # Output features for each ticker
        window_end = datetime.fromtimestamp(context.window().end / 1000.0)
        
        logger.info(f"Window processed: {element_count} mentions, {len(ticker_aggregators)} companies")
        
        for ticker, aggregator in ticker_aggregators.items():
            features = aggregator.get_features(ticker, window_end)
            logger.info(f"5-min Features for {ticker}: pos={features.pos_news_count_5m}, neg={features.neg_news_count_5m}, sentiment={features.sentiment_score_5m}, risk={features.risk_score_5m}")
            out.collect(features.to_json())


class AlertingSink(SinkFunction):
    """Sink that processes features for risk alerting."""
    
    def __init__(self, slack_webhook_url: str, alert_config: dict):
        self.slack_webhook_url = slack_webhook_url
        self.alert_config = alert_config
        self.alert_manager = None
    
    def open(self, configuration):
        """Initialize alert manager."""
        try:
            if self.slack_webhook_url and self.slack_webhook_url != "disabled":
                self.alert_manager = create_alert_manager(
                    webhook_url=self.slack_webhook_url,
                    channel=self.alert_config.get("channel", "#supply-chain-alerts"),
                    company_thresholds=self.alert_config.get("company_thresholds", {}),
                    default_threshold=self.alert_config.get("default_threshold", 0.8),
                    cooldown_minutes=self.alert_config.get("cooldown_minutes", 30)
                )
                logger.info(f"Alert manager initialized with Slack webhook")
            else:
                logger.info("Alerting disabled - no webhook URL configured")
        except Exception as e:
            logger.error(f"Failed to initialize alert manager: {e}")
    
    def invoke(self, value: str, context):
        """Process features and check for alerts."""
        if not self.alert_manager:
            return  # Alerting disabled
            
        try:
            features_dict = json.loads(value)
            features = CompanyFeatures(
                ticker=features_dict['ticker'],
                window_end=features_dict['window_end'],
                neg_news_count_5m=features_dict['neg_news_count_5m'],
                pos_news_count_5m=features_dict['pos_news_count_5m'],
                sentiment_score_5m=features_dict['sentiment_score_5m'],
                risk_score_5m=features_dict['risk_score_5m']
            )
            
            # Process alert asynchronously (in a sync context, we'll use asyncio.run)
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                alert_sent = loop.run_until_complete(self.alert_manager.process_features(features))
                if alert_sent:
                    logger.info(f"🚨 ALERT SENT: {features.ticker} risk_score={features.risk_score_5m:.3f}")
                else:
                    logger.debug(f"No alert: {features.ticker} risk_score={features.risk_score_5m:.3f}")
            finally:
                loop.close()
                
        except Exception as e:
            logger.error(f"Error processing alert for features: {e}")


class RedisFeatureSink(SinkFunction):
    """Sink that writes features to Redis."""
    
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis_client = None
    
    def open(self, configuration):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            # Test connection
            self.redis_client.ping()
            logger.info(f"Connected to Redis: {self.redis_url}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise
    
    def invoke(self, value: str, context):
        """Write feature to Redis."""
        try:
            features_dict = json.loads(value)
            
            # Create Redis key and value
            ticker = features_dict['ticker']
            window_end = features_dict['window_end']
            dt = datetime.fromisoformat(window_end.replace('Z', '+00:00'))
            epoch = int(dt.timestamp())
            
            key = f"feat:{ticker}:{epoch}"
            
            # Add metadata
            redis_value = {
                **features_dict,
                "_ver": "v1",
                "_ingest_ts": datetime.utcnow().isoformat() + "Z"
            }
            
            # Set with 7 day TTL
            self.redis_client.setex(key, timedelta(days=7), json.dumps(redis_value))
            
            # Also set latest key for easy access
            latest_key = f"feat:{ticker}:latest"
            self.redis_client.setex(latest_key, timedelta(days=7), json.dumps(redis_value))
            
            logger.info(f"Written to Redis: {key} and {latest_key} -> risk_score={redis_value.get('risk_score_5m', 'N/A')}")
            
        except Exception as e:
            logger.error(f"Error writing to Redis: {e}")
    
    def close(self):
        """Close Redis connection."""
        if self.redis_client:
            self.redis_client.close()


def create_news_processing_job():
    """Create and configure the Flink job."""
    
    # Environment configuration
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Add Kafka connector JARs
    env.add_jars("file:///app/jars/flink-connector-kafka.jar")
    env.add_jars("file:///app/jars/kafka-clients.jar")
    
    parallelism = int(os.getenv('FLINK_PARALLELISM', '2'))
    env.set_parallelism(parallelism)  # Configurable parallelism
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds
    
    # Configuration from environment
    kafka_brokers = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
    enrichment_url = os.getenv('ENRICHMENT_ENDPOINT', 'http://enrichment:8082')
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    use_mock_enrichment = os.getenv('USE_MOCK_ENRICHMENT', 'false').lower() == 'true'
    
    # Alerting configuration
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', 'disabled')
    alert_config = {
        "channel": os.getenv('SLACK_CHANNEL', '#supply-chain-alerts'),
        "company_thresholds": {
            "AAPL": float(os.getenv('ALERT_THRESHOLD_AAPL', '0.7')),
            "TSLA": float(os.getenv('ALERT_THRESHOLD_TSLA', '0.6')),
            "MSFT": float(os.getenv('ALERT_THRESHOLD_MSFT', '0.75')),
            "GOOGL": float(os.getenv('ALERT_THRESHOLD_GOOGL', '0.8')),
            "AMZN": float(os.getenv('ALERT_THRESHOLD_AMZN', '0.75'))
        },
        "default_threshold": float(os.getenv('ALERT_THRESHOLD_DEFAULT', '0.8')),
        "cooldown_minutes": int(os.getenv('ALERT_COOLDOWN_MINUTES', '30'))
    }
    
    logger.info(f"Starting job with config:")
    logger.info(f"  Parallelism: {parallelism}")
    logger.info(f"  Kafka: {kafka_brokers}")
    logger.info(f"  Enrichment: {enrichment_url} (mock: {use_mock_enrichment})")
    logger.info(f"  Redis: {redis_url}")
    logger.info(f"  Slack Alerts: {'enabled' if slack_webhook_url != 'disabled' else 'disabled'}")
    logger.info(f"  Alert Thresholds: {alert_config['company_thresholds']}")
    
    # Kafka consumer for raw news
    kafka_props = {
        'bootstrap.servers': kafka_brokers,
        'group.id': 'flink-processor',  # Different group than stream processor
        'auto.offset.reset': 'latest'   # Process new messages
    }
    
    news_consumer = FlinkKafkaConsumer(
        topics='raw_news_fulltext',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Create the pipeline
    news_stream = env.add_source(news_consumer).name("kafka_source")
    
    # Set watermark strategy for event time processing (newer API)
    news_stream = news_stream.assign_timestamps_and_watermarks(
        WatermarkStrategy
        .for_bounded_out_of_orderness(Time.minutes(5))
        .with_timestamp_assigner(lambda element, timestamp: int(datetime.utcnow().timestamp() * 1000))
    )
    
    # Step 1: Enrich news with company mentions
    mentions_stream = (news_stream
                      .process(NewsEnrichmentFunction(enrichment_url, use_mock_enrichment))
                      .name("enrich_news"))
    
    # Step 2: Aggregate features using tumbling windows (5min windows, no overlap)
    features_stream = (mentions_stream
                      .window_all(TumblingEventTimeWindows.of(Time.minutes(5)))
                      .process(CompanyFeaturesWindowFunction())
                      .name("aggregate_features"))
    
    # Step 3: Write features to Redis
    features_stream.add_sink(RedisFeatureSink(redis_url)).name("redis_sink")
    
    return env


def main():
    """Main entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger.info("Starting News Processing Job")
    
    env = create_news_processing_job()
    
    # Execute the job
    env.execute("News Processing Pipeline")


if __name__ == '__main__':
    main()