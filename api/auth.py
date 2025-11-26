"""API authentication and rate limiting."""
from fastapi import Security, HTTPException, status, Request
from fastapi.security import APIKeyHeader
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from utilities.config import settings
from utilities.logging_config import api_logger as logger


# API Key authentication
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=True)


def verify_api_key(api_key: str = Security(api_key_header)) -> str:
    """
    Verify API key from request header.
    
    Args:
        api_key: API key from header
        
    Returns:
        API key if valid
        
    Raises:
        HTTPException: If API key is invalid
    """
    valid_keys = settings.api_keys_list
    
    if api_key not in valid_keys:
        logger.warning(f"Invalid API key attempted: {api_key[:8]}...")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid API key"
        )
    
    return api_key


# Rate limiter
limiter = Limiter(key_func=get_remote_address)


def get_rate_limit() -> str:
    """
    Get rate limit string.
    
    Returns:
        Rate limit string for slowapi
    """
    return f"{settings.rate_limit_per_hour}/hour"
