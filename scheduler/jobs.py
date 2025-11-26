"""Scheduled jobs using APScheduler."""
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from datetime import datetime

from crawler.scraper import run_crawler
from scheduler.change_detector import change_detector
from utilities.config import settings
from utilities.database import db
from utilities.logging_config import scheduler_logger as logger


class SchedulerManager:
    """Manage scheduled jobs."""
    
    def __init__(self):
        """Initialize scheduler."""
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
    
    async def daily_crawl_job(self):
        """Job to run daily crawl."""
        try:
            logger.info("Starting scheduled daily crawl...")
            await run_crawler(resume=True)
            logger.info("Daily crawl completed successfully")
        except Exception as e:
            logger.error(f"Daily crawl job failed: {e}")
    
    async def change_detection_job(self):
        """Job to detect changes."""
        try:
            logger.info("Starting change detection job...")
            summary = await change_detector.run_change_detection()
            
            # Generate report if changes detected
            if summary["changes_detected"] > 0:
                report_path = await change_detector.generate_change_report(format="json")
                logger.info(f"Change report generated: {report_path}")
                
                # Send alert if configured
                if settings.enable_email_alerts and settings.alert_email:
                    await self.send_alert(summary)
            
            logger.info("Change detection completed successfully")
        except Exception as e:
            logger.error(f"Change detection job failed: {e}")
    
    async def send_alert(self, summary: dict):
        """
        Send alert about detected changes.
        
        Args:
            summary: Change detection summary
        """
        # Placeholder for email alert implementation
        logger.info(f"Alert would be sent to {settings.alert_email}: {summary}")
        # In production, implement actual email sending using smtplib or a service like SendGrid
    
    def start(self):
        """Start the scheduler."""
        if self.is_running:
            logger.warning("Scheduler is already running")
            return
        
        # Add daily crawl job
        # Run every day at 2 AM
        self.scheduler.add_job(
            self.daily_crawl_job,
            CronTrigger(hour=2, minute=0),
            id="daily_crawl",
            name="Daily Book Crawl",
            replace_existing=True
        )
        
        # Add change detection job
        # Run every day at 3 AM (after crawl)
        self.scheduler.add_job(
            self.change_detection_job,
            CronTrigger(hour=3, minute=0),
            id="change_detection",
            name="Change Detection",
            replace_existing=True
        )
        
        # Alternative: Use interval-based scheduling
        # Uncomment to use interval instead of cron
        # self.scheduler.add_job(
        #     self.daily_crawl_job,
        #     IntervalTrigger(hours=settings.scheduler_interval_hours),
        #     id="daily_crawl",
        #     name="Daily Book Crawl",
        #     replace_existing=True
        # )
        
        self.scheduler.start()
        self.is_running = True
        logger.info("Scheduler started successfully")
        
        # Print scheduled jobs
        jobs = self.scheduler.get_jobs()
        for job in jobs:
            logger.info(f"Scheduled job: {job.name} - Next run: {job.next_run_time}")
    
    def stop(self):
        """Stop the scheduler."""
        if not self.is_running:
            logger.warning("Scheduler is not running")
            return
        
        self.scheduler.shutdown()
        self.is_running = False
        logger.info("Scheduler stopped")
    
    async def run_job_now(self, job_name: str):
        """
        Run a specific job immediately.
        
        Args:
            job_name: Name of the job to run
        """
        if job_name == "daily_crawl":
            await self.daily_crawl_job()
        elif job_name == "change_detection":
            await self.change_detection_job()
        else:
            logger.error(f"Unknown job name: {job_name}")


# Global scheduler instance
scheduler_manager = SchedulerManager()


async def run_scheduler():
    """Run the scheduler indefinitely."""
    # Connect to database
    await db.connect()
    
    # Start scheduler
    scheduler_manager.start()
    
    try:
        # Keep running
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler_manager.stop()
        await db.disconnect()
