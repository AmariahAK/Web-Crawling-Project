"""Async web scraper with retry logic and resume capability."""
import asyncio
import httpx
from typing import List, Optional, Set
from urllib.parse import urljoin
from datetime import datetime

from crawler.parser import BookParser
from crawler.storage import storage, BookStorage
from api.models import Book, CrawlMetadata, CrawlStatus, CrawlProgress
from utilities.config import settings
from utilities.database import db
from utilities.logging_config import crawler_logger as logger


class BookScraper:
    """Async web scraper for books.toscrape.com."""
    
    def __init__(self):
        """Initialize scraper."""
        self.base_url = settings.base_url
        self.max_retries = settings.crawler_max_retries
        self.timeout = settings.crawler_timeout_seconds
        self.concurrency = settings.crawler_concurrency
        self.semaphore = asyncio.Semaphore(self.concurrency)
        self.storage = storage
        
        # Track progress
        self.total_books = 0
        self.completed_books = 0
        self.failed_urls: Set[str] = set()
    
    async def fetch_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        retries: int = 0
    ) -> Optional[str]:
        """
        Fetch URL with exponential backoff retry logic.
        
        Args:
            client: HTTP client
            url: URL to fetch
            retries: Current retry count
            
        Returns:
            HTML content or None if all retries failed
        """
        try:
            async with self.semaphore:
                response = await client.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response.text
                
        except (httpx.HTTPError, httpx.TimeoutException) as e:
            if retries < self.max_retries:
                # Exponential backoff: 2^retries seconds
                wait_time = 2 ** retries
                logger.warning(
                    f"Request failed for {url}, retrying in {wait_time}s "
                    f"(attempt {retries + 1}/{self.max_retries}): {e}"
                )
                await asyncio.sleep(wait_time)
                return await self.fetch_with_retry(client, url, retries + 1)
            else:
                logger.error(f"Failed to fetch {url} after {self.max_retries} retries: {e}")
                return None
    
    async def scrape_book(
        self,
        client: httpx.AsyncClient,
        book_url: str
    ) -> Optional[Book]:
        """
        Scrape a single book page.
        
        Args:
            client: HTTP client
            book_url: URL of book page
            
        Returns:
            Book model or None if scraping failed
        """
        try:
            # Fetch HTML
            html = await self.fetch_with_retry(client, book_url)
            if not html:
                self.failed_urls.add(book_url)
                return None
            
            # Parse book data
            parser = BookParser(html, self.base_url)
            book_data = parser.parse_book_details(book_url)
            
            if not book_data:
                logger.warning(f"Failed to parse book data from {book_url}")
                self.failed_urls.add(book_url)
                return None
            
            # Generate content hash
            book_data["content_hash"] = BookStorage.generate_content_hash(book_data)
            
            # Add crawl metadata
            book_data["crawl_metadata"] = CrawlMetadata(
                timestamp=datetime.utcnow(),
                status=CrawlStatus.SUCCESS,
                source_url=book_url
            )
            
            # Store HTML snapshot
            html_snapshot_id = await self.storage.store_html_snapshot(book_url, html)
            book_data["html_snapshot_id"] = html_snapshot_id
            
            # Create Book model
            book = Book(**book_data)
            
            # Store in database
            success = await self.storage.upsert_book(book)
            if success:
                self.completed_books += 1
                logger.info(
                    f"Successfully scraped book {self.completed_books}/{self.total_books}: "
                    f"{book.name}"
                )
            else:
                self.failed_urls.add(book_url)
            
            return book if success else None
            
        except Exception as e:
            logger.error(f"Error scraping book {book_url}: {e}")
            self.failed_urls.add(book_url)
            return None
    
    async def get_all_book_urls(self, client: httpx.AsyncClient) -> List[str]:
        """
        Get all book URLs from catalog pages with pagination.
        
        Args:
            client: HTTP client
            
        Returns:
            List of book URLs
        """
        all_book_urls = []
        current_url = urljoin(self.base_url, "catalogue/page-1.html")
        
        logger.info("Discovering all book URLs...")
        
        while current_url:
            html = await self.fetch_with_retry(client, current_url)
            if not html:
                logger.error(f"Failed to fetch catalog page: {current_url}")
                break
            
            # Extract book links from current page (pass current_url as base)
            book_urls = BookParser.extract_book_links(html, current_url)
            all_book_urls.extend(book_urls)
            logger.info(f"Found {len(book_urls)} books on {current_url}")
            
            # Get next page URL (pass current_url as base)
            next_url = BookParser.extract_next_page_url(html, current_url)
            current_url = next_url
        
        logger.info(f"Total books discovered: {len(all_book_urls)}")
        return all_book_urls
    
    async def crawl_all_books(self, resume: bool = False) -> CrawlProgress:
        """
        Crawl all books from the website.
        
        Args:
            resume: Whether to resume from last crawl
            
        Returns:
            CrawlProgress with results
        """
        logger.info("Starting book crawl...")
        
        # Connect to database
        await db.connect()
        
        async with httpx.AsyncClient(follow_redirects=True) as client:
            # Get all book URLs
            book_urls = await self.get_all_book_urls(client)
            self.total_books = len(book_urls)
            
            if resume:
                # Check which books are already in database
                existing_urls = set()
                async for doc in db.db.books.find({}, {"url": 1}):
                    existing_urls.add(doc["url"])
                
                # Filter out already scraped books
                book_urls = [url for url in book_urls if url not in existing_urls]
                logger.info(f"Resuming crawl, {len(book_urls)} books remaining")
            
            # Create tasks for all books
            tasks = [self.scrape_book(client, url) for url in book_urls]
            
            # Execute with progress tracking
            batch_size = 50
            for i in range(0, len(tasks), batch_size):
                batch = tasks[i:i + batch_size]
                await asyncio.gather(*batch)
                
                # Save progress
                progress = CrawlProgress(
                    total_pages=self.total_books,
                    completed_pages=self.completed_books,
                    failed_pages=list(self.failed_urls),
                    status=CrawlStatus.PARTIAL if self.failed_urls else CrawlStatus.SUCCESS
                )
                await self.storage.save_crawl_progress(progress)
        
        # Final progress
        final_status = CrawlStatus.SUCCESS if not self.failed_urls else CrawlStatus.PARTIAL
        progress = CrawlProgress(
            total_pages=self.total_books,
            completed_pages=self.completed_books,
            failed_pages=list(self.failed_urls),
            status=final_status
        )
        await self.storage.save_crawl_progress(progress)
        
        logger.info(
            f"Crawl completed: {self.completed_books}/{self.total_books} books, "
            f"{len(self.failed_urls)} failed"
        )
        
        return progress


async def run_crawler(resume: bool = False):
    """
    Run the crawler.
    
    Args:
        resume: Whether to resume from last crawl
    """
    scraper = BookScraper()
    progress = await scraper.crawl_all_books(resume=resume)
    
    print(f"\n{'='*60}")
    print(f"Crawl Summary:")
    print(f"{'='*60}")
    print(f"Total books: {progress.total_pages}")
    print(f"Successfully crawled: {progress.completed_pages}")
    print(f"Failed: {len(progress.failed_pages)}")
    print(f"Progress: {progress.progress_percentage:.1f}%")
    print(f"Status: {progress.status.value}")
    print(f"{'='*60}\n")
