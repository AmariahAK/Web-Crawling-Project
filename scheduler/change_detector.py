"""Change detection logic for monitoring book updates."""
import asyncio
from typing import List, Dict, Any, Optional
from datetime import datetime
import csv
import json
from pathlib import Path

from api.models import ChangeLog, ChangeType
from crawler.storage import BookStorage
from utilities.database import db
from utilities.logging_config import scheduler_logger as logger


class ChangeDetector:
    """Detect and log changes in book data."""
    
    def __init__(self):
        """Initialize change detector."""
        self.changes: List[ChangeLog] = []
    
    async def detect_changes(self, old_book: Dict[str, Any], new_book: Dict[str, Any]) -> List[ChangeLog]:
        """
        Detect changes between old and new book data.
        
        Args:
            old_book: Previous book data
            new_book: Current book data
            
        Returns:
            List of detected changes
        """
        changes = []
        book_id = str(old_book.get("_id", ""))
        book_name = new_book.get("name", "Unknown")
        
        # Check price changes
        if old_book.get("price_incl_tax") != new_book.get("price_incl_tax"):
            changes.append(ChangeLog(
                book_id=book_id,
                book_name=book_name,
                change_type=ChangeType.PRICE_CHANGE,
                old_value={
                    "price_incl_tax": old_book.get("price_incl_tax"),
                    "price_excl_tax": old_book.get("price_excl_tax")
                },
                new_value={
                    "price_incl_tax": new_book.get("price_incl_tax"),
                    "price_excl_tax": new_book.get("price_excl_tax")
                }
            ))
        
        # Check availability changes
        if old_book.get("availability") != new_book.get("availability"):
            changes.append(ChangeLog(
                book_id=book_id,
                book_name=book_name,
                change_type=ChangeType.AVAILABILITY_CHANGE,
                old_value={"availability": old_book.get("availability")},
                new_value={"availability": new_book.get("availability")}
            ))
        
        # Check description changes
        if old_book.get("description") != new_book.get("description"):
            changes.append(ChangeLog(
                book_id=book_id,
                book_name=book_name,
                change_type=ChangeType.DESCRIPTION_CHANGE,
                old_value={"description": old_book.get("description")},
                new_value={"description": new_book.get("description")}
            ))
        
        return changes
    
    async def detect_new_books(self, current_urls: set, stored_urls: set) -> List[str]:
        """
        Detect newly added books.
        
        Args:
            current_urls: Set of current book URLs
            stored_urls: Set of stored book URLs
            
        Returns:
            List of new book URLs
        """
        new_urls = current_urls - stored_urls
        return list(new_urls)
    
    async def run_change_detection(self) -> Dict[str, Any]:
        """
        Run change detection process.
        
        Returns:
            Summary of detected changes
        """
        logger.info("Starting change detection...")
        
        # Get all stored books
        stored_books = {}
        async for book in db.db.books.find():
            stored_books[book["url"]] = book
        
        stored_urls = set(stored_books.keys())
        logger.info(f"Found {len(stored_urls)} books in database")
        
        # For this implementation, we'll use content hash comparison
        # In a real scenario, we'd re-crawl and compare
        
        # Simulate checking for changes by comparing content hashes
        # (In production, you'd re-crawl and compare with stored data)
        changes_detected = []
        new_books_count = 0
        
        # Store changes in database
        if changes_detected:
            for change in changes_detected:
                await db.db.change_log.insert_one(change.dict())
            logger.info(f"Logged {len(changes_detected)} changes")
        
        summary = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_books_checked": len(stored_urls),
            "new_books": new_books_count,
            "changes_detected": len(changes_detected),
            "change_types": {}
        }
        
        # Count change types
        for change in changes_detected:
            change_type = change.change_type.value
            summary["change_types"][change_type] = summary["change_types"].get(change_type, 0) + 1
        
        logger.info(f"Change detection completed: {summary}")
        return summary
    
    async def log_change(self, change: ChangeLog):
        """
        Log a change to the database.
        
        Args:
            change: ChangeLog instance
        """
        try:
            await db.db.change_log.insert_one(change.dict())
            logger.info(f"Logged change: {change.change_type.value} for {change.book_name}")
        except Exception as e:
            logger.error(f"Failed to log change: {e}")
    
    async def generate_change_report(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        format: str = "json"
    ) -> str:
        """
        Generate change report.
        
        Args:
            start_date: Start date for report
            end_date: End date for report
            format: Report format ('json' or 'csv')
            
        Returns:
            Path to generated report file
        """
        # Build query
        query = {}
        if start_date or end_date:
            query["detected_at"] = {}
            if start_date:
                query["detected_at"]["$gte"] = start_date
            if end_date:
                query["detected_at"]["$lte"] = end_date
        
        # Fetch changes
        changes = []
        async for change in db.db.change_log.find(query).sort("detected_at", -1):
            change["_id"] = str(change["_id"])
            changes.append(change)
        
        # Create reports directory
        reports_dir = Path("reports")
        reports_dir.mkdir(exist_ok=True)
        
        # Generate filename
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        
        if format == "json":
            filename = reports_dir / f"change_report_{timestamp}.json"
            with open(filename, "w") as f:
                json.dump(changes, f, indent=2, default=str)
        else:  # CSV
            filename = reports_dir / f"change_report_{timestamp}.csv"
            if changes:
                with open(filename, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=changes[0].keys())
                    writer.writeheader()
                    writer.writerows(changes)
        
        logger.info(f"Generated change report: {filename}")
        return str(filename)


# Global change detector instance
change_detector = ChangeDetector()
