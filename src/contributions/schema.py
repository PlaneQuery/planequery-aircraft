"""Schema validation for community submissions."""
import json
import re
from pathlib import Path
from typing import Any

try:
    from jsonschema import Draft202012Validator
except ImportError:
    Draft202012Validator = None


SCHEMA_PATH = Path(__file__).parent.parent.parent / "schemas" / "community_submission.v1.schema.json"


def load_schema() -> dict:
    """Load the community submission schema."""
    with open(SCHEMA_PATH) as f:
        return json.load(f)


def validate_submission(data: dict | list, schema: dict | None = None) -> list[str]:
    """
    Validate submission(s) against schema.
    
    Args:
        data: Single submission dict or list of submissions
        schema: Optional schema dict. If None, loads from default path.
        
    Returns:
        List of error messages. Empty list means validation passed.
    """
    if Draft202012Validator is None:
        raise ImportError("jsonschema is required: pip install jsonschema")
    
    if schema is None:
        schema = load_schema()
    
    submissions = data if isinstance(data, list) else [data]
    errors = []
    
    validator = Draft202012Validator(schema)
    
    for i, submission in enumerate(submissions):
        prefix = f"[{i}] " if len(submissions) > 1 else ""
        for error in validator.iter_errors(submission):
            path = ".".join(str(p) for p in error.path) if error.path else "(root)"
            errors.append(f"{prefix}{path}: {error.message}")
    
    return errors


def extract_json_from_issue_body(body: str) -> str | None:
    """
    Extract JSON from GitHub issue body.
    
    Looks for JSON in the 'Submission JSON' section wrapped in code blocks.
    
    Args:
        body: The issue body text
        
    Returns:
        Extracted JSON string or None if not found
    """
    # Match JSON in "### Submission JSON" section
    pattern = r"### Submission JSON\s*\n\s*```(?:json)?\s*\n([\s\S]*?)\n\s*```"
    match = re.search(pattern, body)
    
    if match:
        return match.group(1).strip()
    
    return None


def extract_contributor_name_from_issue_body(body: str) -> str | None:
    """
    Extract contributor name from GitHub issue body.
    
    Looks for the 'Contributor Name' field in the issue form.
    
    Args:
        body: The issue body text
        
    Returns:
        Contributor name string or None if not found/empty
    """
    # Match "### Contributor Name" section
    pattern = r"### Contributor Name\s*\n\s*(.+?)(?=\n###|\n\n|$)"
    match = re.search(pattern, body)
    
    if match:
        name = match.group(1).strip()
        # GitHub issue forms show "_No response_" for empty optional fields
        if name and name != "_No response_":
            return name
    
    return None


def parse_and_validate(json_str: str, schema: dict | None = None) -> tuple[list | dict | None, list[str]]:
    """
    Parse JSON string and validate against schema.
    
    Args:
        json_str: JSON string to parse
        schema: Optional schema dict
        
    Returns:
        Tuple of (parsed data or None, list of errors)
    """
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        return None, [f"Invalid JSON: {e}"]
    
    errors = validate_submission(data, schema)
    return data, errors
