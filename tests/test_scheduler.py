"""Unit tests for scheduler functionality."""
import pytest
from datetime import datetime
from scheduler.change_detector import ChangeDetector
from api.models import ChangeType


class TestChangeDetector:
    """Test ChangeDetector functionality."""
    
    @pytest.mark.asyncio
    async def test_detect_price_change(self):
        """Test detecting price changes."""
        detector = ChangeDetector()
        
        old_book = {
            "_id": "123",
            "name": "Test Book",
            "price_incl_tax": 10.99,
            "price_excl_tax": 9.99,
            "availability": "In stock",
            "description": "Test"
        }
        
        new_book = {
            "name": "Test Book",
            "price_incl_tax": 12.99,
            "price_excl_tax": 11.99,
            "availability": "In stock",
            "description": "Test"
        }
        
        changes = await detector.detect_changes(old_book, new_book)
        
        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.PRICE_CHANGE
        assert changes[0].old_value["price_incl_tax"] == 10.99
        assert changes[0].new_value["price_incl_tax"] == 12.99
    
    @pytest.mark.asyncio
    async def test_detect_availability_change(self):
        """Test detecting availability changes."""
        detector = ChangeDetector()
        
        old_book = {
            "_id": "123",
            "name": "Test Book",
            "price_incl_tax": 10.99,
            "price_excl_tax": 9.99,
            "availability": "In stock",
            "description": "Test"
        }
        
        new_book = {
            "name": "Test Book",
            "price_incl_tax": 10.99,
            "price_excl_tax": 9.99,
            "availability": "Out of stock",
            "description": "Test"
        }
        
        changes = await detector.detect_changes(old_book, new_book)
        
        assert len(changes) == 1
        assert changes[0].change_type == ChangeType.AVAILABILITY_CHANGE
    
    @pytest.mark.asyncio
    async def test_detect_new_books(self):
        """Test detecting new books."""
        detector = ChangeDetector()
        
        current_urls = {"url1", "url2", "url3"}
        stored_urls = {"url1", "url2"}
        
        new_urls = await detector.detect_new_books(current_urls, stored_urls)
        
        assert len(new_urls) == 1
        assert "url3" in new_urls
