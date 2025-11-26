"""Main entry point for the application."""
import asyncio
import argparse
import sys

from crawler.scraper import run_crawler
from scheduler.jobs import run_scheduler
from utilities.logging_config import setup_logging

logger = setup_logging("main")


def main():
    """Main entry point with CLI interface."""
    parser = argparse.ArgumentParser(description="Books Crawler Application")
    parser.add_argument(
        "command",
        choices=["crawl", "schedule", "api"],
        help="Command to run: crawl (run crawler), schedule (run scheduler), api (run API server)"
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Resume from last crawl (only for crawl command)"
    )
    
    args = parser.parse_args()
    
    if args.command == "crawl":
        logger.info("Starting crawler...")
        asyncio.run(run_crawler(resume=args.resume))
    
    elif args.command == "schedule":
        logger.info("Starting scheduler...")
        asyncio.run(run_scheduler())
    
    elif args.command == "api":
        logger.info("Starting API server...")
        import uvicorn
        from api.main import app
        
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8000,
            log_level="info"
        )
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
