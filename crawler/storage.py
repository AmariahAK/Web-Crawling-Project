"""MongoDB storage operations for crawler data."""
import hashlib
import json
from typing import Optional, Dict, Any
from datetime import datetime
from bson import ObjectId
from gridfs import GridFS
from api.models import Book, CrawlMetadata, CrawlStatus, CrawlProgress
from utilities.database import db
from utilities.logging_config import crawler_logger as logger


class BookStorage:
    """Handle storage operations for book data."""
    
    def __init__(self):
        """Initialize storage handler."""
        self.gridfs: Optional[GridFS] = None
    
    def _init_gridfs(self):
        """Initialize GridFS for HTML snapshot storage."""
        if not self.gridfs and db.sync_client:
            sync_db = db.sync_client[db.db.name]
            self.gridfs = GridFS(sync_db)
    
    @staticmethod
    def generate_content_hash(book_data: Dict[str, Any]) -> str:
        """
        Generate content hash for change detection.
        
        Args:
            book_data: Book data dictionary
            
        Returns:
            SHA256 hash of relevant fields
        """
        # Hash only fields that we care about for change detection
        relevant_fields = {
            "name": book_data.get("name"),
            "price_incl_tax": book_data.get("price_incl_tax"),
            "price_excl_tax": book_data.get("price_excl_tax"),
            "availability": book_data.get("availability"),
            "description": book_data.get("description"),
        }
        
        # Create deterministic JSON string
        json_str = json.dumps(relevant_fields, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()
    
    async def store_html_snapshot(self, url: str, html: str) -> Optional[str]:
        """
        Store HTML snapshot in GridFS.
        
        Args:
            url: Book URL
            html: HTML content
            
        Returns:
            GridFS file ID or None
        """
        try:
            self._init_gridfs()
            if not self.gridfs:
                logger.warning("GridFS not initialized, skipping HTML snapshot")
                return None
            
            # Store in GridFS with metadata
            file_id = self.gridfs.put(
                html.encode('utf-8'),
                filename=url,
                content_type="text/html",
                upload_date=datetime.utcnow()
            )
            
            return str(file_id)
            
        except Exception as e:
            logger.error(f"Failed to store HTML snapshot for {url}: {e}")
            return None
    
    async def upsert_book(self, book: Book) -> bool:
        """
        Insert or update book in database.
        
        Args:
            book: Book model instance
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Convert to dict
            book_dict = book.dict()
            
            # Convert datetime to proper format
            if isinstance(book_dict["crawl_metadata"]["timestamp"], datetime):
                book_dict["crawl_metadata"]["timestamp"] = book_dict["crawl_metadata"]["timestamp"]
            
            # Upsert using URL as unique identifier
            result = await db.db.books.update_one(
                {"url": book.url},
                {"$set": book_dict},
                upsert=True
            )
            
            if result.upserted_id:
                logger.info(f"Inserted new book: {book.name}")
            else:
                logger.info(f"Updated existing book: {book.name}")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to upsert book {book.url}: {e}")
            return False
    
    async def get_book_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve book by URL.
        
        Args:
            url: Book URL
            
        Returns:
            Book document or None
        """
        try:
            return await db.db.books.find_one({"url": url})
        except Exception as e:
            logger.error(f"Failed to retrieve book {url}: {e}")
            return None
    
    async def save_crawl_progress(self, progress: CrawlProgress) -> bool:
        """
        Save crawl progress to database.
        
        Args:
            progress: CrawlProgress model instance
            
        Returns:
            True if successful, False otherwise
        """
        try:
            progress_dict = progress.dict()
            
            # Update or insert progress document
            await db.db.crawl_progress.update_one(
                {},  # Single progress document
                {"$set": progress_dict},
                upsert=True
            )
            
            logger.info(f"Saved crawl progress: {progress.progress_percentage:.1f}%")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save crawl progress: {e}")
            return False
    
    async def get_crawl_progress(self) -> Optional[CrawlProgress]:
        """
        Retrieve last crawl progress.
        
        Returns:
            CrawlProgress instance or None
        """
        try:
            doc = await db.db.crawl_progress.find_one()
            if doc:
                # Remove MongoDB _id field
                doc.pop("_id", None)
                return CrawlProgress(**doc)
            return None
            
        except Exception as e:
            logger.error(f"Failed to retrieve crawl progress: {e}")
            return None
    
    async def get_total_books_count(self) -> int:
        """
        Get total number of books in database.
        
        Returns:
            Count of books
        """
        try:
            return await db.db.books.count_documents({})
        except Exception as e:
            logger.error(f"Failed to count books: {e}")
            return 0


# Global storage instance
storage = BookStorage()
