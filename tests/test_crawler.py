"""Unit tests for crawler functionality."""
import pytest
from crawler.parser import BookParser
from crawler.storage import BookStorage


class TestBookParser:
    """Test BookParser functionality."""
    
    def test_extract_book_links(self):
        """Test extracting book links from catalog page."""
        html = """
        <article class="product_pod">
            <h3><a href="catalogue/book1.html">Book 1</a></h3>
        </article>
        <article class="product_pod">
            <h3><a href="catalogue/book2.html">Book 2</a></h3>
        </article>
        """
        base_url = "https://books.toscrape.com/"
        
        links = BookParser.extract_book_links(html, base_url)
        
        assert len(links) == 2
        assert "book1.html" in links[0]
        assert "book2.html" in links[1]
    
    def test_extract_next_page_url(self):
        """Test extracting next page URL."""
        html = """
        <li class="next">
            <a href="catalogue/page-2.html">next</a>
        </li>
        """
        base_url = "https://books.toscrape.com/"
        
        next_url = BookParser.extract_next_page_url(html, base_url)
        
        assert next_url is not None
        assert "page-2.html" in next_url
    
    def test_rating_extraction(self):
        """Test rating extraction from star class."""
        html = '<p class="star-rating Four"></p>'
        parser = BookParser(html, "https://books.toscrape.com/")
        
        rating = parser._extract_rating()
        
        assert rating == 4


class TestBookStorage:
    """Test BookStorage functionality."""
    
    def test_generate_content_hash(self):
        """Test content hash generation."""
        book_data1 = {
            "name": "Test Book",
            "price_incl_tax": 10.99,
            "price_excl_tax": 9.99,
            "availability": "In stock",
            "description": "Test description"
        }
        
        book_data2 = {
            "name": "Test Book",
            "price_incl_tax": 10.99,
            "price_excl_tax": 9.99,
            "availability": "In stock",
            "description": "Test description"
        }
        
        hash1 = BookStorage.generate_content_hash(book_data1)
        hash2 = BookStorage.generate_content_hash(book_data2)
        
        assert hash1 == hash2
    
    def test_content_hash_changes_with_price(self):
        """Test that content hash changes when price changes."""
        book_data1 = {
            "name": "Test Book",
            "price_incl_tax": 10.99,
            "price_excl_tax": 9.99,
            "availability": "In stock",
            "description": "Test description"
        }
        
        book_data2 = {
            "name": "Test Book",
            "price_incl_tax": 12.99,  # Different price
            "price_excl_tax": 11.99,
            "availability": "In stock",
            "description": "Test description"
        }
        
        hash1 = BookStorage.generate_content_hash(book_data1)
        hash2 = BookStorage.generate_content_hash(book_data2)
        
        assert hash1 != hash2
