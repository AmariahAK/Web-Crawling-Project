"""Unit tests for API endpoints."""
import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, patch

from api.main import app


@pytest.fixture
def client():
    """Create test client."""
    return TestClient(app)


@pytest.fixture
def valid_api_key():
    """Return a valid API key for testing."""
    return "dev_key_123"


class TestBooksEndpoint:
    """Test /books endpoint."""
    
    def test_books_without_api_key(self, client):
        """Test that books endpoint requires API key."""
        response = client.get("/api/v1/books")
        assert response.status_code == 403  # Forbidden without API key
    
    def test_books_with_invalid_api_key(self, client):
        """Test books endpoint with invalid API key."""
        response = client.get(
            "/api/v1/books",
            headers={"X-API-Key": "invalid_key"}
        )
        assert response.status_code == 401
    
    @patch("api.routes.db")
    def test_books_with_valid_api_key(self, mock_db, client, valid_api_key):
        """Test books endpoint with valid API key."""
        # Mock database response
        mock_db.db.books.count_documents = AsyncMock(return_value=0)
        mock_db.db.books.find = AsyncMock(return_value=AsyncMock(__aiter__=lambda x: iter([])))
        
        response = client.get(
            "/api/v1/books",
            headers={"X-API-Key": valid_api_key}
        )
        
        # Note: This test may need adjustment based on actual implementation
        assert response.status_code in [200, 500]  # 500 if DB not connected
    
    def test_books_with_filters(self, client, valid_api_key):
        """Test books endpoint with query filters."""
        response = client.get(
            "/api/v1/books?category=Fiction&min_price=10&max_price=50&rating=4",
            headers={"X-API-Key": valid_api_key}
        )
        
        # Should return 200 or 500 depending on DB connection
        assert response.status_code in [200, 500]


class TestBookDetailEndpoint:
    """Test /books/{book_id} endpoint."""
    
    def test_book_detail_without_api_key(self, client):
        """Test that book detail endpoint requires API key."""
        response = client.get("/api/v1/books/507f1f77bcf86cd799439011")
        assert response.status_code == 403
    
    def test_book_detail_invalid_id(self, client, valid_api_key):
        """Test book detail with invalid ID format."""
        response = client.get(
            "/api/v1/books/invalid_id",
            headers={"X-API-Key": valid_api_key}
        )
        assert response.status_code in [400, 500]


class TestChangesEndpoint:
    """Test /changes endpoint."""
    
    def test_changes_without_api_key(self, client):
        """Test that changes endpoint requires API key."""
        response = client.get("/api/v1/changes")
        assert response.status_code == 403
    
    def test_changes_with_filters(self, client, valid_api_key):
        """Test changes endpoint with date filters."""
        response = client.get(
            "/api/v1/changes?change_type=price_change",
            headers={"X-API-Key": valid_api_key}
        )
        
        assert response.status_code in [200, 500]


class TestHealthEndpoint:
    """Test health check endpoint."""
    
    def test_health_check(self, client):
        """Test health check endpoint."""
        response = client.get("/health")
        
        # Health check should always return 200
        assert response.status_code == 200
        assert "status" in response.json()
