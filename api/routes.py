"""API routes for book and change endpoints."""
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from typing import Optional, List
from datetime import datetime
from bson import ObjectId

from api.models import BookResponse, ChangeLogResponse, PaginatedResponse
from api.auth import verify_api_key, limiter, get_rate_limit
from utilities.database import db
from utilities.logging_config import api_logger as logger


router = APIRouter()


@router.get("/books", response_model=PaginatedResponse)
@limiter.limit(get_rate_limit())
async def get_books(
    request: Request,
    category: Optional[str] = Query(None, description="Filter by category"),
    min_price: Optional[float] = Query(None, ge=0, description="Minimum price"),
    max_price: Optional[float] = Query(None, ge=0, description="Maximum price"),
    rating: Optional[int] = Query(None, ge=1, le=5, description="Filter by rating"),
    sort_by: Optional[str] = Query("name", regex="^(name|rating|price|reviews)$", description="Sort field"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get books with filtering, sorting, and pagination.
    
    Args:
        request: FastAPI request
        category: Filter by category
        min_price: Minimum price filter
        max_price: Maximum price filter
        rating: Filter by rating
        sort_by: Sort field (name, rating, price, reviews)
        page: Page number
        page_size: Items per page
        api_key: API key for authentication
        
    Returns:
        Paginated list of books
    """
    try:
        # Build query
        query = {}
        
        if category:
            query["category"] = category
        
        if min_price is not None or max_price is not None:
            query["price_incl_tax"] = {}
            if min_price is not None:
                query["price_incl_tax"]["$gte"] = min_price
            if max_price is not None:
                query["price_incl_tax"]["$lte"] = max_price
        
        if rating is not None:
            query["rating"] = rating
        
        # Determine sort field
        sort_field_map = {
            "name": "name",
            "rating": "rating",
            "price": "price_incl_tax",
            "reviews": "num_reviews"
        }
        sort_field = sort_field_map.get(sort_by, "name")
        
        # Get total count
        total = await db.db.books.count_documents(query)
        
        # Calculate pagination
        skip = (page - 1) * page_size
        total_pages = (total + page_size - 1) // page_size
        
        # Fetch books
        cursor = db.db.books.find(query).sort(sort_field, -1).skip(skip).limit(page_size)
        books = []
        
        async for book in cursor:
            books.append({
                "id": str(book["_id"]),
                "url": book["url"],
                "name": book["name"],
                "description": book.get("description"),
                "category": book["category"],
                "price_excl_tax": book["price_excl_tax"],
                "price_incl_tax": book["price_incl_tax"],
                "availability": book["availability"],
                "num_reviews": book["num_reviews"],
                "rating": book["rating"],
                "image_url": book["image_url"],
                "last_updated": book["crawl_metadata"]["timestamp"]
            })
        
        logger.info(f"Fetched {len(books)} books (page {page}/{total_pages})")
        
        return PaginatedResponse(
            items=books,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
    except Exception as e:
        logger.error(f"Error fetching books: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/books/{book_id}", response_model=BookResponse)
@limiter.limit(get_rate_limit())
async def get_book(
    request: Request,
    book_id: str,
    api_key: str = Depends(verify_api_key)
):
    """
    Get full details of a specific book.
    
    Args:
        request: FastAPI request
        book_id: Book ID
        api_key: API key for authentication
        
    Returns:
        Book details
    """
    try:
        # Validate ObjectId
        if not ObjectId.is_valid(book_id):
            raise HTTPException(status_code=400, detail="Invalid book ID format")
        
        # Fetch book
        book = await db.db.books.find_one({"_id": ObjectId(book_id)})
        
        if not book:
            raise HTTPException(status_code=404, detail="Book not found")
        
        logger.info(f"Fetched book: {book['name']}")
        
        return BookResponse(
            id=str(book["_id"]),
            url=book["url"],
            name=book["name"],
            description=book.get("description"),
            category=book["category"],
            price_excl_tax=book["price_excl_tax"],
            price_incl_tax=book["price_incl_tax"],
            availability=book["availability"],
            num_reviews=book["num_reviews"],
            rating=book["rating"],
            image_url=book["image_url"],
            last_updated=book["crawl_metadata"]["timestamp"]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching book {book_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/changes", response_model=PaginatedResponse)
@limiter.limit(get_rate_limit())
async def get_changes(
    request: Request,
    start_date: Optional[datetime] = Query(None, description="Start date filter"),
    end_date: Optional[datetime] = Query(None, description="End date filter"),
    change_type: Optional[str] = Query(None, description="Filter by change type"),
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(20, ge=1, le=100, description="Items per page"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get recent changes with filtering and pagination.
    
    Args:
        request: FastAPI request
        start_date: Start date filter
        end_date: End date filter
        change_type: Filter by change type
        page: Page number
        page_size: Items per page
        api_key: API key for authentication
        
    Returns:
        Paginated list of changes
    """
    try:
        # Build query
        query = {}
        
        if start_date or end_date:
            query["detected_at"] = {}
            if start_date:
                query["detected_at"]["$gte"] = start_date
            if end_date:
                query["detected_at"]["$lte"] = end_date
        
        if change_type:
            query["change_type"] = change_type
        
        # Get total count
        total = await db.db.change_log.count_documents(query)
        
        # Calculate pagination
        skip = (page - 1) * page_size
        total_pages = (total + page_size - 1) // page_size
        
        # Fetch changes
        cursor = db.db.change_log.find(query).sort("detected_at", -1).skip(skip).limit(page_size)
        changes = []
        
        async for change in cursor:
            changes.append({
                "id": str(change["_id"]),
                "book_id": change["book_id"],
                "book_name": change["book_name"],
                "change_type": change["change_type"],
                "old_value": change.get("old_value"),
                "new_value": change["new_value"],
                "detected_at": change["detected_at"]
            })
        
        logger.info(f"Fetched {len(changes)} changes (page {page}/{total_pages})")
        
        return PaginatedResponse(
            items=changes,
            total=total,
            page=page,
            page_size=page_size,
            total_pages=total_pages
        )
        
    except Exception as e:
        logger.error(f"Error fetching changes: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
