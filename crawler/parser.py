"""HTML parsing utilities for extracting book data."""
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
import re
from urllib.parse import urljoin
from utilities.logging_config import crawler_logger as logger


class BookParser:
    """Parser for extracting book information from HTML."""
    
    # Rating class to number mapping
    RATING_MAP = {
        "One": 1,
        "Two": 2,
        "Three": 3,
        "Four": 4,
        "Five": 5
    }
    
    def __init__(self, html: str, base_url: str):
        """
        Initialize parser with HTML content.
        
        Args:
            html: HTML content to parse
            base_url: Base URL for resolving relative URLs
        """
        self.soup = BeautifulSoup(html, "lxml")
        self.base_url = base_url
    
    def parse_book_details(self, book_url: str) -> Optional[Dict[str, Any]]:
        """
        Parse book details from book detail page.
        
        Args:
            book_url: URL of the book page
            
        Returns:
            Dictionary with book data or None if parsing fails
        """
        try:
            data = {
                "url": book_url,
                "name": self._extract_name(),
                "description": self._extract_description(),
                "category": self._extract_category(),
                "price_excl_tax": self._extract_price_excl_tax(),
                "price_incl_tax": self._extract_price_incl_tax(),
                "availability": self._extract_availability(),
                "num_reviews": self._extract_num_reviews(),
                "rating": self._extract_rating(),
                "image_url": self._extract_image_url(),
            }
            
            # Validate that required fields are present
            if not data["name"] or data["price_incl_tax"] is None:
                logger.warning(f"Missing required fields for {book_url}")
                return None
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to parse book details from {book_url}: {e}")
            return None
    
    def _extract_name(self) -> str:
        """Extract book name."""
        title_elem = self.soup.find("h1")
        return title_elem.text.strip() if title_elem else ""
    
    def _extract_description(self) -> Optional[str]:
        """Extract book description."""
        # Description is in the article tag, after the product_page div
        article = self.soup.find("article", class_="product_page")
        if article:
            # Find the description paragraph (usually after the product info)
            desc_elem = article.find("div", id="product_description")
            if desc_elem:
                # Get the next sibling which is the actual description
                next_p = desc_elem.find_next_sibling("p")
                if next_p:
                    return next_p.text.strip()
        return None
    
    def _extract_category(self) -> str:
        """Extract book category from breadcrumb."""
        breadcrumb = self.soup.find("ul", class_="breadcrumb")
        if breadcrumb:
            # Category is the second-to-last item in breadcrumb
            links = breadcrumb.find_all("a")
            if len(links) >= 3:
                return links[2].text.strip()
        return "Unknown"
    
    def _extract_price_excl_tax(self) -> Optional[float]:
        """Extract price excluding tax."""
        return self._extract_price_from_table("Price (excl. tax)")
    
    def _extract_price_incl_tax(self) -> Optional[float]:
        """Extract price including tax."""
        return self._extract_price_from_table("Price (incl. tax)")
    
    def _extract_price_from_table(self, label: str) -> Optional[float]:
        """
        Extract price from product information table.
        
        Args:
            label: Table row label
            
        Returns:
            Price as float or None
        """
        table = self.soup.find("table", class_="table-striped")
        if table:
            rows = table.find_all("tr")
            for row in rows:
                th = row.find("th")
                if th and label in th.text:
                    td = row.find("td")
                    if td:
                        # Extract numeric value from price string (e.g., "£51.77")
                        price_text = td.text.strip()
                        match = re.search(r"[\d.]+", price_text)
                        if match:
                            return float(match.group())
        return None
    
    def _extract_availability(self) -> str:
        """Extract availability status."""
        avail_elem = self.soup.find("p", class_="instock availability")
        if avail_elem:
            return avail_elem.text.strip()
        return "Unknown"
    
    def _extract_num_reviews(self) -> int:
        """Extract number of reviews."""
        table = self.soup.find("table", class_="table-striped")
        if table:
            rows = table.find_all("tr")
            for row in rows:
                th = row.find("th")
                if th and "Number of reviews" in th.text:
                    td = row.find("td")
                    if td:
                        try:
                            return int(td.text.strip())
                        except ValueError:
                            pass
        return 0
    
    def _extract_rating(self) -> int:
        """Extract book rating."""
        # Rating is in a <p> tag with class "star-rating [Rating]"
        rating_elem = self.soup.find("p", class_="star-rating")
        if rating_elem:
            # Get the second class which contains the rating word
            classes = rating_elem.get("class", [])
            for cls in classes:
                if cls in self.RATING_MAP:
                    return self.RATING_MAP[cls]
        return 3  # Default to 3 if not found
    
    def _extract_image_url(self) -> str:
        """Extract book cover image URL."""
        img_elem = self.soup.find("img")
        if img_elem and img_elem.get("src"):
            # Convert relative URL to absolute
            relative_url = img_elem["src"]
            return urljoin(self.base_url, relative_url)
        return ""
    
    @staticmethod
    def extract_book_links(html: str, current_page_url: str) -> list:
        """
        Extract book links from catalog page.
        
        Args:
            html: HTML content of catalog page
            current_page_url: URL of the current catalog page (used as base for relative URLs)
            
        Returns:
            List of absolute book URLs
        """
        soup = BeautifulSoup(html, "lxml")
        book_links = []
        
        # Find all book article elements
        articles = soup.find_all("article", class_="product_pod")
        for article in articles:
            h3 = article.find("h3")
            if h3:
                a = h3.find("a")
                if a and a.get("href"):
                    relative_url = a["href"]
                    # Convert relative URL to absolute using current page as base
                    absolute_url = urljoin(current_page_url, relative_url)
                    book_links.append(absolute_url)
        
        return book_links
    
    @staticmethod
    def extract_next_page_url(html: str, current_page_url: str) -> Optional[str]:
        """
        Extract next page URL from pagination.
        
        Args:
            html: HTML content of catalog page
            current_page_url: URL of the current catalog page (used as base for relative URLs)
            
        Returns:
            Absolute URL of next page or None
        """
        soup = BeautifulSoup(html, "lxml")
        next_link = soup.find("li", class_="next")
        if next_link:
            a = next_link.find("a")
            if a and a.get("href"):
                relative_url = a["href"]
                return urljoin(current_page_url, relative_url)
        return None
