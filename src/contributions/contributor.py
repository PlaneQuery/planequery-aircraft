"""Contributor identification utilities."""
import hashlib
import uuid


# DNS namespace UUID for generating UUIDv5
DNS_NAMESPACE = uuid.UUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8')


def generate_contributor_uuid(github_user_id: int) -> str:
    """
    Generate a deterministic UUID v5 from a GitHub user ID.
    
    This ensures the same GitHub account always gets the same contributor UUID.
    
    Args:
        github_user_id: The numeric GitHub user ID
        
    Returns:
        UUID string in standard format
    """
    name = f"github:{github_user_id}"
    return str(uuid.uuid5(DNS_NAMESPACE, name))


def sanitize_username(username: str, max_length: int = 20) -> str:
    """
    Sanitize a GitHub username for use in filenames.
    
    Args:
        username: GitHub username
        max_length: Maximum length of sanitized name
        
    Returns:
        Lowercase alphanumeric string with underscores
    """
    sanitized = ""
    for char in username.lower():
        if char.isalnum():
            sanitized += char
        else:
            sanitized += "_"
    
    # Collapse multiple underscores
    while "__" in sanitized:
        sanitized = sanitized.replace("__", "_")
    
    return sanitized.strip("_")[:max_length]


def generate_submission_filename(
    username: str,
    date_str: str,
    content_hash: str,
    extension: str = ".json"
) -> str:
    """
    Generate a unique filename for a community submission.
    
    Format: {sanitized_username}_{date}_{short_hash}.json
    
    Args:
        username: GitHub username
        date_str: Date in YYYY-MM-DD format
        content_hash: Hash of the submission content (will be truncated to 8 chars)
        extension: File extension (default: .json)
        
    Returns:
        Unique filename string
    """
    sanitized_name = sanitize_username(username)
    short_hash = content_hash[:8]
    return f"{sanitized_name}_{date_str}_{short_hash}{extension}"


def compute_content_hash(content: str) -> str:
    """
    Compute SHA256 hash of content.
    
    Args:
        content: String content to hash
        
    Returns:
        Hex digest of SHA256 hash
    """
    return hashlib.sha256(content.encode()).hexdigest()
