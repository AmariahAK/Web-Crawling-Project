"""MongoDB database connection and management."""
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pymongo import MongoClient, ASCENDING, DESCENDING
from typing import Optional
from utilities.config import settings
from utilities.logging_config import setup_logging

logger = setup_logging("database")


class Database:
    """MongoDB database connection manager."""
    
    def __init__(self):
        """Initialize database connection."""
        self.client: Optional[AsyncIOMotorClient] = None
        self.db: Optional[AsyncIOMotorDatabase] = None
        self.sync_client: Optional[MongoClient] = None
    
    async def connect(self):
        """Establish database connection."""
        try:
            self.client = AsyncIOMotorClient(settings.mongodb_uri)
            self.db = self.client[settings.database_name]
            
            # Test connection
            await self.client.admin.command('ping')
            logger.info(f"Connected to MongoDB: {settings.database_name}")
            
            # Create indexes
            await self.create_indexes()
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection."""
        if self.client:
            self.client.close()
            logger.info("Disconnected from MongoDB")
    
    async def create_indexes(self):
        """Create database indexes for efficient querying."""
        try:
            # Books collection indexes
            await self.db.books.create_index("url", unique=True)
            await self.db.books.create_index("category")
            await self.db.books.create_index("price_incl_tax")
            await self.db.books.create_index("rating")
            await self.db.books.create_index("num_reviews")
            await self.db.books.create_index([("category", ASCENDING), ("price_incl_tax", ASCENDING)])
            
            # Change log indexes
            await self.db.change_log.create_index("book_id")
            await self.db.change_log.create_index("detected_at")
            await self.db.change_log.create_index("change_type")
            
            # Crawl progress index
            await self.db.crawl_progress.create_index("timestamp")
            
            logger.info("Database indexes created successfully")
            
        except Exception as e:
            logger.error(f"Failed to create indexes: {e}")
            raise
    
    def get_sync_client(self) -> MongoClient:
        """Get synchronous MongoDB client for non-async operations."""
        if not self.sync_client:
            self.sync_client = MongoClient(settings.mongodb_uri)
        return self.sync_client
    
    async def health_check(self) -> bool:
        """Check database connection health."""
        try:
            await self.client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"Database health check failed: {e}")
            return False


# Global database instance
db = Database()
