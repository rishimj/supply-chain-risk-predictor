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
from pyflink.common.time import Time, Duration
from pyflink.datastream.window import SlidingEventTimeWindows
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream.functions import ProcessFunction, ProcessAllWindowFunction, SinkFunction
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream.checkpointing_mode import CheckpointingMode
import redis

from models import NewsMessage, CompanyMentionEvent, CompanyFeatures
from enrichment_client import EnrichmentClient, MockEnrichmentClient
from alerting import create_alert_manager, AlertManager
from postgres_logger import PostgresAlertLogger


logger = logging.getLogger(__name__)


class MentionTimestampAssigner(TimestampAssigner):
    """Extracts event timestamp from company mention events for event-time processing."""
    
    def extract_timestamp(self, value: str, record_timestamp: int) -> int:
        """
        Extract timestamp from the event_ts field (article's published time).
        Returns timestamp in milliseconds for Flink's event-time processing.
        """
        try:
            data = json.loads(value)
            # Parse ISO-8601 timestamp: "2024-01-15T10:30:00Z"
            event_ts = data.get('event_ts', '')
            if event_ts:
                dt = datetime.fromisoformat(event_ts.replace('Z', '+00:00'))
                return int(dt.timestamp() * 1000)  # Convert to milliseconds
        except Exception as e:
            logger.warning(f"Failed to extract timestamp from event, using record timestamp: {e}")
        
        # Fallback to Kafka record timestamp (ingestion time)
        return record_timestamp if record_timestamp > 0 else int(datetime.utcnow().timestamp() * 1000)


class NewsEnrichmentFunction(ProcessFunction):
    """Process function that calls enrichment API and outputs company mentions."""
    
    def __init__(self, enrichment_url: str, use_mock: bool = False, batch_size: int = 10, batch_timeout_ms: int = 1000):
        self.enrichment_url = enrichment_url
        self.use_mock = use_mock
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.client = None
        self.batch_buffer = []
        self.last_batch_time = None
    
    def open(self, runtime_context):
        """Initialize the enrichment client."""
        if self.use_mock:
            self.client = MockEnrichmentClient()
        else:
            self.client = EnrichmentClient(self.enrichment_url, batch_timeout=5.0)
        self.last_batch_time = datetime.utcnow()
        logger.info(f"NewsEnrichmentFunction initialized with batch_size={self.batch_size}, batch_timeout={self.batch_timeout_ms}ms")
    
    def process_element(self, value: str, ctx: ProcessFunction.Context, out):
        """Process news messages with batching for better throughput."""
        try:
            # Parse news message
            news = NewsMessage.from_json(value)
            
            # Add to batch buffer
            self.batch_buffer.append(news)
            
            # Check if we should flush the batch
            should_flush = False
            
            # Flush if batch is full
            if len(self.batch_buffer) >= self.batch_size:
                should_flush = True
                logger.debug(f"Flushing batch: size limit reached ({self.batch_size} articles)")
            
            # Flush if timeout reached
            elif self.last_batch_time:
                time_since_last_batch = (datetime.utcnow() - self.last_batch_time).total_seconds() * 1000
                if time_since_last_batch >= self.batch_timeout_ms:
                    should_flush = True
                    logger.debug(f"Flushing batch: timeout reached ({time_since_last_batch:.0f}ms)")
            
            if should_flush:
                self._flush_batch(out)
                
        except Exception as e:
            logger.error(f"Error processing news {value}: {e}")
            # Don't fail the job, just skip this message
    
    def close(self):
        """Flush any remaining items in the batch on close."""
        if self.batch_buffer:
            logger.info(f"Flushing remaining {len(self.batch_buffer)} articles on close")
            # Create a dummy output collector for close
            class DummyCollector:
                def collect(self, value):
                    pass
            self._flush_batch(DummyCollector())
    
    def _flush_batch(self, out):
        """Flush the current batch to enrichment API."""
        if not self.batch_buffer:
            return
        
        batch = self.batch_buffer
        self.batch_buffer = []
        self.last_batch_time = datetime.utcnow()
        
        try:
            logger.info(f"Processing batch of {len(batch)} articles")
            
            # Call batch enrichment API
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                enrichment_responses = loop.run_until_complete(self.client.enrich_batch(batch))
            finally:
                loop.close()
            
            # Process each response
            for news, enrichment_response in zip(batch, enrichment_responses):
                event_timestamp = news.published
                
                for company in enrichment_response.companies:
                    mention_event = CompanyMentionEvent(
                        news_id=news.news_id,
                        event_ts=event_timestamp,
                        ticker=company.ticker,
                        role=company.role,
                        sentiment=company.sentiment
                    )
                    
                    logger.debug(f"Found company: {company.ticker} (sentiment: {company.sentiment}, published: {event_timestamp})")
                    out.collect(mention_event.to_json())
            
            total_companies = sum(len(r.companies) for r in enrichment_responses)
            logger.info(f"Batch processed: {len(batch)} articles → {total_companies} company mentions")
                
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
            # Don't fail the job, just skip this batch


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
    """
    Window function that aggregates company mentions into risk features.
    
    Processes 5-minute sliding windows of company mentions and outputs:
    - Negative/positive news counts
    - Sentiment score (EWM)
    - Risk score (0.0-1.0)
    
    With late data handling, windows may fire multiple times as late articles arrive.
    This is handled downstream by idempotent Redis writes (same key overwrites).
    """
    
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
        window_start = datetime.fromtimestamp(context.window().start / 1000.0)
        window_end = datetime.fromtimestamp(context.window().end / 1000.0)
        
        logger.info(f"Window [{window_start.strftime('%H:%M:%S')} - {window_end.strftime('%H:%M:%S')}] processed: {element_count} mentions, {len(ticker_aggregators)} companies")
        
        for ticker, aggregator in ticker_aggregators.items():
            features = aggregator.get_features(ticker, window_end)
            logger.info(f"Features for {ticker}: pos={features.pos_news_count_5m}, neg={features.neg_news_count_5m}, sentiment={features.sentiment_score_5m:.3f}, risk={features.risk_score_5m:.3f}")
            out.collect(features.to_json())


class AlertingSink(SinkFunction):
    """Sink that processes features for risk alerting."""
    
    def __init__(self, slack_webhook_url: str, alert_config: dict, redis_url: str, postgres_url: str = None):
        self.slack_webhook_url = slack_webhook_url
        self.alert_config = alert_config
        self.redis_url = redis_url
        self.postgres_url = postgres_url
        self.alert_manager = None
        self.redis_client = None
        self.postgres_logger = None
    
    def open(self, configuration):
        """Initialize alert manager with Redis and PostgreSQL connections."""
        try:
            # Initialize Redis for cooldown tracking
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            self.redis_client.ping()
            logger.info(f"AlertingSink connected to Redis: {self.redis_url}")
            
            # Initialize PostgreSQL logger
            if self.postgres_url:
                self.postgres_logger = PostgresAlertLogger(self.postgres_url, pool_size=5)
                # Initialize pool in sync context using asyncio
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self.postgres_logger.initialize())
                    logger.info(f"AlertingSink connected to PostgreSQL: {self.postgres_url}")
                finally:
                    loop.close()
            else:
                logger.info("PostgreSQL logging disabled - no URL configured")
            
            # Initialize alert manager
            if self.slack_webhook_url and self.slack_webhook_url != "disabled":
                self.alert_manager = create_alert_manager(
                    webhook_url=self.slack_webhook_url,
                    channel=self.alert_config.get("channel", "#supply-chain-alerts"),
                    company_thresholds=self.alert_config.get("company_thresholds", {}),
                    default_threshold=self.alert_config.get("default_threshold", 0.7),
                    cooldown_minutes=self.alert_config.get("cooldown_minutes", 30),
                    redis_client=self.redis_client,
                    postgres_logger=self.postgres_logger
                )
                logger.info(f"Alert manager initialized with Redis cooldown and PostgreSQL logging")
            else:
                logger.info("Alerting disabled - no webhook URL configured")
        except Exception as e:
            logger.error(f"Failed to initialize AlertingSink: {e}")
            raise
    
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
    
    def close(self):
        """Close connections gracefully."""
        # Close PostgreSQL connection pool
        if self.postgres_logger:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self.postgres_logger.close())
                logger.info("PostgreSQL connection pool closed")
            finally:
                loop.close()
        
        # Close Redis connection
        if self.redis_client:
            self.redis_client.close()
            logger.info("Redis connection closed")


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
    
    parallelism = int(os.getenv('FLINK_PARALLELISM', '4'))
    env.set_parallelism(parallelism)  # Configurable parallelism
    
    # Configure checkpointing for fault tolerance
    env.enable_checkpointing(10000)  # Checkpoint every 10 seconds
    checkpoint_config = env.get_checkpoint_config()
    checkpoint_config.set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)
    checkpoint_config.set_min_pause_between_checkpoints(5000)  # 5 seconds between checkpoints
    checkpoint_config.set_checkpoint_timeout(60000)  # 60 second timeout
    checkpoint_config.set_max_concurrent_checkpoints(1)
    
    # Configure checkpoint storage (persistent across restarts)
    checkpoint_dir = os.getenv('FLINK_CHECKPOINT_DIR', 'file:///tmp/flink-checkpoints')
    checkpoint_config.set_checkpoint_storage_dir(checkpoint_dir)
    
    # Keep checkpoints on job cancellation for recovery
    checkpoint_config.enable_externalized_checkpoints(
        checkpoint_config.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    )
    
    # Configure restart strategy for fault tolerance
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            restart_attempts=3,  # Try 3 times
            delay_between_attempts=10000  # 10 seconds between attempts
        )
    )
    
    # Configuration from environment
    kafka_brokers = os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')
    enrichment_url = os.getenv('ENRICHMENT_ENDPOINT', 'http://enrichment:8082')
    redis_url = os.getenv('REDIS_URL', 'redis://redis:6379/0')
    postgres_url = os.getenv('POSTGRES_URL')  # Optional
    use_mock_enrichment = os.getenv('USE_MOCK_ENRICHMENT', 'false').lower() == 'true'
    
    # Batch processing configuration
    batch_size = int(os.getenv('ENRICHMENT_BATCH_SIZE', '10'))
    batch_timeout_ms = int(os.getenv('ENRICHMENT_BATCH_TIMEOUT_MS', '1000'))
    
    # Alerting configuration
    slack_webhook_url = os.getenv('SLACK_WEBHOOK_URL', 'disabled')
    alert_config = {
        "channel": os.getenv('SLACK_CHANNEL', '#supply-chain-alerts'),
        "company_thresholds": {},  # No per-company overrides
        "default_threshold": 0.7,  # Universal threshold
        "cooldown_minutes": 30     # Fixed 30-min cooldown
    }
    
    logger.info(f"Starting job with config:")
    logger.info(f"  Parallelism: {parallelism}")
    logger.info(f"  Checkpoint dir: {checkpoint_dir}")
    logger.info(f"  Kafka: {kafka_brokers}")
    logger.info(f"  Enrichment: {enrichment_url} (mock: {use_mock_enrichment})")
    logger.info(f"  Batch Size: {batch_size} articles, Timeout: {batch_timeout_ms}ms")
    logger.info(f"  Redis: {redis_url}")
    logger.info(f"  PostgreSQL: {postgres_url if postgres_url else 'disabled'}")
    logger.info(f"  Slack Alerts: {'enabled' if slack_webhook_url != 'disabled' else 'disabled'}")
    logger.info(f"  Window: 5-minute sliding (1-min slide), 2-min watermark delay, 5-min late tolerance")
    logger.info(f"  Alert Threshold: 0.7 (universal), Cooldown: 30min (Redis)")
    
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
    
    # Step 1: Enrich news with company mentions (with batching for high throughput)
    # The enrichment function preserves the article's published timestamp as event_ts
    mentions_stream = (news_stream
                      .process(NewsEnrichmentFunction(
                          enrichment_url, 
                          use_mock_enrichment,
                          batch_size=batch_size,
                          batch_timeout_ms=batch_timeout_ms
                      ))
                      .name("enrich_news"))
    
    # Step 2: Configure watermarks for event-time processing
    # Watermarks allow Flink to handle out-of-order and late-arriving articles
    # - 2 minutes out-of-orderness: articles arriving up to 2 min late are still in-order
    # - Uses article's published time (not processing time) for accurate windowing
    watermark_strategy = (
        WatermarkStrategy
        .for_bounded_out_of_orderness(Duration.of_minutes(2))
        .with_timestamp_assigner(MentionTimestampAssigner())
    )
    
    # Apply watermarks to the mentions stream
    watermarked_stream = (mentions_stream
                         .assign_timestamps_and_watermarks(watermark_strategy)
                         .name("assign_watermarks"))
    
    # Step 3: Aggregate features using SLIDING windows with late data handling
    # - 5-minute window: aggregates 5 minutes of data for smoother risk calculation
    # - 1-minute slide: emits updated results every minute (rolling view)
    # - 5-minute allowed lateness: articles arriving up to 5 min late still update windows
    features_stream = (watermarked_stream
                      .window_all(SlidingEventTimeWindows.of(
                          Time.minutes(5),   # Window size: 5 minutes of data
                          Time.minutes(1)    # Slide interval: emit every 1 minute
                      ))
                      .allowed_lateness(Time.minutes(5))  # Accept late data up to 5 more minutes
                      .process(CompanyFeaturesWindowFunction())
                      .name("aggregate_features_5m_sliding"))
    
    # Step 4: Write features to Redis (idempotent - same window_end overwrites)
    features_stream.add_sink(RedisFeatureSink(redis_url)).name("redis_sink")
    
    # Step 5: Send alerts for high-risk features (threshold: 0.7, Redis cooldown, PostgreSQL logging)
    features_stream.add_sink(
        AlertingSink(slack_webhook_url, alert_config, redis_url, postgres_url)
    ).name("alerting_sink")
    
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