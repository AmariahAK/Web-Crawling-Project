"""Pydantic models for data validation."""
from pydantic import BaseModel, Field, HttpUrl, validator
from typing import Optional, List
from datetime import datetime
from enum import Enum


class BookRating(int, Enum):
    """Book rating enumeration (1-5 stars)."""
    ONE = 1
    TWO = 2
    THREE = 3
    FOUR = 4
    FIVE = 5


class CrawlStatus(str, Enum):
    """Crawl status enumeration."""
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"


class CrawlMetadata(BaseModel):
    """Metadata about the crawl operation."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: CrawlStatus
    source_url: str
    error_message: Optional[str] = None


class Book(BaseModel):
    """Book model with validation."""
    url: str
    name: str
    description: Optional[str] = None
    category: str
    price_excl_tax: float = Field(gt=0, description="Price excluding tax")
    price_incl_tax: float = Field(gt=0, description="Price including tax")
    availability: str
    num_reviews: int = Field(ge=0, description="Number of reviews")
    rating: int = Field(ge=1, le=5, description="Rating from 1-5")
    image_url: str
    content_hash: str = Field(description="Hash for change detection")
    crawl_metadata: CrawlMetadata
    html_snapshot_id: Optional[str] = None
    
    @validator("price_incl_tax")
    def price_incl_must_be_gte_excl(cls, v, values):
        """Validate that price including tax is >= price excluding tax."""
        if "price_excl_tax" in values and v < values["price_excl_tax"]:
            raise ValueError("Price including tax must be >= price excluding tax")
        return v
    
    class Config:
        """Pydantic config."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class BookResponse(BaseModel):
    """API response model for a single book."""
    id: str
    url: str
    name: str
    description: Optional[str]
    category: str
    price_excl_tax: float
    price_incl_tax: float
    availability: str
    num_reviews: int
    rating: int
    image_url: str
    last_updated: datetime


class ChangeType(str, Enum):
    """Type of change detected."""
    NEW_BOOK = "new_book"
    PRICE_CHANGE = "price_change"
    AVAILABILITY_CHANGE = "availability_change"
    DESCRIPTION_CHANGE = "description_change"
    OTHER = "other"


class ChangeLog(BaseModel):
    """Model for tracking changes."""
    book_id: str
    book_name: str
    change_type: ChangeType
    old_value: Optional[dict] = None
    new_value: dict
    detected_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        """Pydantic config."""
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }


class ChangeLogResponse(BaseModel):
    """API response model for change log."""
    id: str
    book_id: str
    book_name: str
    change_type: str
    old_value: Optional[dict]
    new_value: dict
    detected_at: datetime


class PaginatedResponse(BaseModel):
    """Generic paginated response."""
    items: List[dict]
    total: int
    page: int
    page_size: int
    total_pages: int


class CrawlProgress(BaseModel):
    """Model for tracking crawl progress."""
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    total_pages: int
    completed_pages: int
    failed_pages: List[str] = Field(default_factory=list)
    status: CrawlStatus
    
    @property
    def progress_percentage(self) -> float:
        """Calculate progress percentage."""
        if self.total_pages == 0:
            return 0.0
        return (self.completed_pages / self.total_pages) * 100
