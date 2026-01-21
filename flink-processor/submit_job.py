"""
Script to submit the Flink job to a remote cluster.
This can run standalone or submit to JobManager.

Supports two job modes:
- standard: Full pipeline with tiered sentiment and batch processing
- shock: Optimized shock-only detection with async enrichment
"""
import os
import sys
import logging

logger = logging.getLogger(__name__)

def check_cluster_mode():
    """Check if we should run in cluster mode or standalone."""
    jobmanager_host = os.getenv('FLINK_JOBMANAGER_HOST')
    return jobmanager_host is not None

def get_job_mode():
    """Get the job mode from environment variable."""
    return os.getenv('FLINK_JOB_MODE', 'standard').lower()

def main():
    """Main entry point - decides whether to run standalone or submit to cluster."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    cluster_mode = check_cluster_mode()
    job_mode = get_job_mode()
    
    if cluster_mode:
        jobmanager_host = os.getenv('FLINK_JOBMANAGER_HOST', 'flink-jobmanager')
        jobmanager_port = os.getenv('FLINK_JOBMANAGER_PORT', '8081')
        logger.info(f"Cluster mode detected - JobManager at {jobmanager_host}:{jobmanager_port}")
        logger.info("Note: PyFlink jobs run in standalone mode by default")
        logger.info("For true cluster submission, use Flink CLI or REST API")
        logger.info("Running job in embedded mode with cluster configuration...")
    else:
        logger.info("Standalone mode - running job locally")
    
    # Select and run the appropriate job based on mode
    logger.info(f"Job mode: {job_mode}")
    
    if job_mode == 'shock':
        logger.info("Starting SHOCK DETECTION job (async, shock-only)")
        logger.info("Features: keyword pre-filter, async enrichment, simplified aggregation")
        from src.shock_detection_job import main as run_job
    else:
        logger.info("Starting STANDARD job (batch, tiered sentiment)")
        logger.info("Features: batch processing, tiered sentiment, full tracking")
        from src.news_processing_job import main as run_job
    
    run_job()

if __name__ == '__main__':
    main()
