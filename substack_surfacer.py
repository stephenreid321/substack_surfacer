#!/usr/bin/env python3
"""
Substack Surfacer

This script takes a list of Substack newsletter URLs and fetches all posts 
published within a specified time window, along with their like counts and metadata.

The script includes resume functionality - if it fails halfway through, it will
skip newsletters that have already been scraped (by checking for existing CSV files).

URL File Format:
The --urls file can be either:
1. One URL per line (simple format)
2. Tab-separated format with URL and category: URL<TAB>CATEGORY

Example urls.txt with categories:
https://newsletter1.substack.com economics
https://newsletter2.substack.com philosophy

Usage:
    python substack_surfacer.py --urls urls.txt --from 2025-01-01 --to 2025-12-31 --output posts.csv
    python substack_surfacer.py --url https://example.substack.com --from 2025-01-01 --to 2025-12-31
    python substack_surfacer.py --user stephenreid --from 2025-01-01 --to 2025-12-31
"""

import os
import csv
import argparse
import time
import re
import requests
import sys
import json
import io
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from typing import List, Dict, Optional, Callable, Any

# Configuration constants
DEFAULT_MAX_RETRIES = 8
MAX_BACKOFF_WAIT_TIME = 60  # seconds
REQUEST_TIMEOUT = 10  # seconds
DEFAULT_WORKERS = 12  # 24 was no faster; 48 hit Substack rate limits

_thread_local = threading.local()


def http_get(url: str, **kwargs) -> requests.Response:
    """GET using a per-thread requests.Session so connections are reused."""
    session = getattr(_thread_local, 'session', None)
    if session is None:
        session = _thread_local.session = requests.Session()
    kwargs.setdefault('timeout', REQUEST_TIMEOUT)
    return session.get(url, **kwargs)


class ThreadBufferedStdout(io.TextIOBase):
    """Buffers output per worker thread so each newsletter's log prints as one block."""

    def __init__(self, real):
        self.real = real
        self.lock = threading.Lock()

    def write(self, text):
        buf = getattr(_thread_local, 'log_buffer', None)
        if buf is not None:
            buf.append(text)
        else:
            with self.lock:
                self.real.write(text)
                self.real.flush()
        return len(text)

    def flush(self):
        self.real.flush()

    def start_buffer(self):
        _thread_local.log_buffer = []

    def end_buffer(self):
        text = ''.join(getattr(_thread_local, 'log_buffer', None) or [])
        _thread_local.log_buffer = None
        with self.lock:
            self.real.write(text)
            self.real.flush()


TRANSIENT_MAX_RETRIES = 3  # for timeouts, connection errors and 5xx responses


def retry_with_backoff(func: Callable[[], Any], max_retries: int = DEFAULT_MAX_RETRIES,
                      operation_name: str = "operation") -> Any:
    """Run func, retrying on rate limits (429), 5xx responses, timeouts and connection errors.

    Returns func's result, or an error string ('Rate limited' / 'Error: ...') if it gives up.
    """
    transient_failures = 0
    for attempt in range(max_retries):
        try:
            return func()
        except requests.HTTPError as e:
            response = e.response
            status = response.status_code if response is not None else None
            if status == 429:
                if attempt == max_retries - 1:
                    return 'Rate limited'
                retry_after = response.headers.get('Retry-After', '')
                wait_time = int(retry_after) if retry_after.isdigit() else 2 ** attempt
                wait_time = min(wait_time, MAX_BACKOFF_WAIT_TIME)
                print(f"⏳ Rate limited for {operation_name}, waiting {wait_time}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            if status is not None and status >= 500:
                transient_failures += 1
                if transient_failures < TRANSIENT_MAX_RETRIES:
                    print(f"🔁 HTTP {status} for {operation_name}, retrying in {2 ** transient_failures}s")
                    time.sleep(2 ** transient_failures)
                    continue
            return f'Error: {e}'
        except (requests.Timeout, requests.ConnectionError) as e:
            transient_failures += 1
            if transient_failures < TRANSIENT_MAX_RETRIES:
                print(f"🔁 {type(e).__name__} for {operation_name}, retrying in {2 ** transient_failures}s")
                time.sleep(2 ** transient_failures)
                continue
            return f'Error: {e}'
        except Exception as e:
            return f'Error: {e}'

    return 'Failed after retries'

def ensure_directory(file_path: str):
    """Create directory for file path if it doesn't exist."""
    output_dir = os.path.dirname(file_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        
def parse_datetime(date_str: str, raise_on_error: bool = False) -> Optional[datetime]:
    """
    Parse datetime string in various formats, returning None if parsing fails.
    
    Args:
        date_str: The date string to parse
        raise_on_error: If True, raises ValueError on parsing failure instead of returning None
    
    Returns:
        datetime object with UTC timezone, or None if parsing fails (when raise_on_error=False)
    """
    if not date_str:
        if raise_on_error:
            raise ValueError("Empty date string provided")
        return None
    
    # Try ISO format first (most common for APIs)
    try:
        if 'T' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        pass
    
    # Try various datetime formats
    formats = [
        '%Y-%m-%d %H:%M:%S',  # 2025-01-01 12:00:00
        '%Y-%m-%d',           # 2025-01-01
        '%Y/%m/%d',           # 2024/01/01
        '%m/%d/%Y',           # 01/01/2024
        '%d/%m/%Y',           # 01/01/2024 (European)
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    
    # If we get here, parsing failed
    if raise_on_error:
        raise ValueError(f"Unable to parse date: {date_str}")
    return None

def load_urls_from_file(file_path: str) -> List[Dict[str, str]]:
    """Load Substack URLs from a text file. Supports both single URLs and tab-separated URL+category format."""
    urls = []
    
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if line and not line.startswith('#'):  # Skip empty lines and comments
                # Check if line contains tab-separated values
                if '\t' in line:
                    parts = line.split('\t')
                    if len(parts) >= 2:
                        url = parts[0].strip()
                        category = parts[1].strip()
                        
                        # Basic URL validation
                        if url.startswith(('http://', 'https://')):
                            urls.append({'url': url, 'category': category})
                        else:
                            print(f"⚠️  Line {line_num}: Invalid URL format: {url}")
                    else:
                        print(f"⚠️  Line {line_num}: Invalid tab-separated format: {line}")
                else:
                    # Single URL format (backward compatibility)
                    if line.startswith(('http://', 'https://')):
                        urls.append({'url': line, 'category': ''})
                    else:
                        print(f"⚠️  Line {line_num}: Invalid URL format: {line}")
    
    return urls


def normalize_substack_url(url: str) -> str:
    """Normalize Substack URL to ensure it's in the correct format."""
    # Remove trailing slashes and ensure proper format
    url = url.rstrip('/')
    
    # Handle @username format (e.g., https://substack.com/@tuckerwalsh -> https://tuckerwalsh.substack.com)
    if 'substack.com/@' in url:
        parsed = urlparse(url)
        username = parsed.path.lstrip('/@')  # Remove leading /@ from path
        url = f"{parsed.scheme}://{username}.substack.com"
    
    # If it's a post URL, extract the newsletter URL
    elif '/p/' in url:
        parsed = urlparse(url)
        url = f"{parsed.scheme}://{parsed.netloc}"
    
    return url


def get_free_subscriber_count(publication_url: str, max_retries: int = DEFAULT_MAX_RETRIES) -> str:
    """Get free subscriber count for a Substack newsletter with retries."""
    if not publication_url:
        return 'No URL'
    
    publication_url = normalize_substack_url(publication_url)
    
    def get_subscriber_count():
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = http_get(publication_url, headers=headers)
        response.raise_for_status()
        
        content = response.text
        
        # Look for freeSubscriberCount in the content
        subscriber_match = re.search(r'\\?"freeSubscriberCount\\?":\\?"([^"\\]+)', content)
        if subscriber_match:
            return subscriber_match.group(1)
        
        # Fallback to freeSubscriberCountOrderOfMagnitude if freeSubscriberCount not found
        magnitude_match = re.search(r'\\?"freeSubscriberCountOrderOfMagnitude\\?":\\?"([^"\\]+)', content)
        if magnitude_match:
            return magnitude_match.group(1)
        
        return 'UNKNOWN'  # No subscriber count found
    
    result = retry_with_backoff(get_subscriber_count, max_retries, f"subscriber count {publication_url}")
    if result.startswith(('Error', 'Rate limited', 'Failed')):
        print(f"   ⚠️  Could not get subscriber count ({result[:100]}), using UNKNOWN")
        return 'UNKNOWN'
    return result


ARCHIVE_PAGE_SIZE = 25  # Substack's archive API rejects limits above 25


def fetch_archive_posts(newsletter_url: str, from_date: datetime) -> List[Dict]:
    """Page through a newsletter's archive (newest first) until posts are older than from_date."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    archive_url = f"{newsletter_url.rstrip('/')}/api/v1/archive"
    posts: List[Dict] = []
    cursor = None
    while True:
        params = {'sort': 'new', 'limit': ARCHIVE_PAGE_SIZE}
        if cursor:
            params['cursor'] = cursor

        def get_page():
            response = http_get(archive_url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()

        page = retry_with_backoff(get_page, DEFAULT_MAX_RETRIES, "archive page")
        if not isinstance(page, dict):
            raise RuntimeError(f"Failed to fetch archive page: {page}")

        page_posts = page.get('posts', [])
        posts.extend(page_posts)
        cursor = page.get('nextCursor')
        if not page_posts or not cursor:
            break
        oldest = parse_datetime(page_posts[-1].get('post_date') or '')
        if oldest and oldest < from_date:
            break
    return posts


def fetch_newsletter_posts(newsletter_url: str, from_date: datetime, to_date: datetime, max_posts: Optional[int] = None, category: str = '') -> List[Dict]:
    """Fetch posts with from_date <= post_date < to_date (to_date is an exclusive end)."""
    try:
        original_newsletter_url = newsletter_url
        newsletter_url = normalize_substack_url(newsletter_url)
        print(f"📰 Fetching posts from: {newsletter_url}")
        
        
        # Always use URL extraction for newsletter name consistency
        newsletter_name = extract_newsletter_name_from_url(newsletter_url)
        
        print("   📄 Fetching posts...")
        posts = fetch_archive_posts(newsletter_url, from_date)

        if not posts:
            print("   ⚠️  No posts found (empty archive)")
            return []

        print(f"   📄 Retrieved {len(posts)} posts, filtering by date window...")
        
        filtered_posts = []
        for post in posts:

            try:
                post_data = post  # archive entries already carry all the metadata we need
                if not post_data:
                    continue
                
                
                # Parse post date
                post_date_str = post_data.get('post_date') or post_data.get('created_at')
                if not post_date_str:
                    print(f"   ⚠️  No date found for post: {post_data.get('title', 'UNKNOWN')}")
                    continue
                
                # Convert to datetime using the flexible parser
                post_date = parse_datetime(post_date_str)
                if not post_date:
                    print(f"   ⚠️  Could not parse date '{post_date_str}' for post: {post_data.get('title', 'UNKNOWN')}")
                    continue
                
                if from_date <= post_date < to_date:
                    # Safely extract author name
                    published_bylines = post_data.get('publishedBylines', [])
                    author_name = newsletter_name  # Default fallback
                    if published_bylines and len(published_bylines) > 0:
                        author_names = [byline.get('name', '') for byline in published_bylines if byline.get('name')]
                        author_name = ', '.join(author_names) if author_names else newsletter_name
                    
                    post_info = {
                        'newsletter_name': newsletter_name,
                        'newsletter_url': newsletter_url,
                        'category': category,
                        'free_subscriber_count': None,  # filled in below, only if there are posts
                        'likes': post_data.get('reaction_count', 'UNKNOWN'),
                        'likes_per_100_free_subscribers': None,
                        'post_url': post_data.get('canonical_url', ''),                        
                        'author': author_name,                        
                        'post_title': post_data.get('title', 'No Title'),
                        'post_subtitle': post_data.get('subtitle', ''),                        
                        'post_date': post_date.strftime('%Y-%m-%d %H:%M:%S'),
                        'word_count': post_data.get('wordcount', 0),                        
                        'is_paid': post_data.get('audience', 'everyone') != 'everyone',
                        'post_id': post_data.get('id', ''),
                    }
                    filtered_posts.append(post_info)
                elif post_date < from_date:
                    # Post is older than from_date, stop fetching since posts are chronological
                    print(f"   ⏹️  Reached post older than {from_date.strftime('%Y-%m-%d')} (post date: {post_date.strftime('%Y-%m-%d')}), stopping...")
                    break
            
            except Exception as e:
                print(f"⚠️  Unexpected error processing post: {str(e)[:100]}")
                continue
        
        # Only fetch the (heavy) homepage for a subscriber count when we have posts to attach it to
        if filtered_posts:
            print("   👥 Fetching subscriber count...")
            free_subscriber_count = get_free_subscriber_count(newsletter_url)
            print(f"   👥 Free subscribers: {free_subscriber_count}")
            for post_info in filtered_posts:
                post_info['free_subscriber_count'] = free_subscriber_count
                post_info['likes_per_100_free_subscribers'] = calculate_engagement_rate(post_info['likes'], free_subscriber_count)

        if max_posts and len(filtered_posts) > max_posts:
            print(f"   ⚠️  {len(filtered_posts)} posts in window, keeping the newest {max_posts} (--max-posts)")
            filtered_posts = filtered_posts[:max_posts]

        return filtered_posts
        
    except Exception as e:
        print(f"❌ Error processing newsletter {newsletter_url}: {e}")
        raise


def parse_subscriber_count(free_subscriber_count: str) -> Optional[float]:
    """Turn '1,234', '3K+' or '1M+' into a number (bottom of the band); None if unparseable."""
    text = str(free_subscriber_count or '').replace(',', '').rstrip('+').strip()
    multiplier = 1
    if text[-1:].upper() in ('K', 'M'):
        multiplier = 1_000 if text[-1].upper() == 'K' else 1_000_000
        text = text[:-1]
    try:
        return float(text) * multiplier
    except ValueError:
        return None


def calculate_engagement_rate(likes: Any, free_subscriber_count: str) -> Any:
    """Likes per 100 free subscribers, or 'UNKNOWN' if either number is missing."""
    subscriber_num = parse_subscriber_count(free_subscriber_count)
    if not isinstance(likes, (int, float)) or not subscriber_num:
        return 'UNKNOWN'
    return round(100 * (likes / subscriber_num), 2)


def save_posts_to_csv(posts: List[Dict], output_file: str):
    """Save post data to a CSV file."""
    ensure_directory(output_file)
    
    if not posts:
        print("📝 Creating empty CSV file (no posts to save)...")
    else:
        print(f"💾 Writing {len(posts)} posts to CSV...")
            
    fieldnames = [
        'newsletter_name', 'newsletter_url', 'category', 'free_subscriber_count',
        'likes', 'likes_per_100_free_subscribers',
        'post_url', 'author', 'post_title', 'post_subtitle', 'post_date',
        'word_count', 'is_paid', 'post_id'
    ]
    
    # Write to a temp file and rename into place, so a crash mid-write never leaves a
    # truncated CSV that resume would mistake for a finished newsletter
    tmp_file = f"{output_file}.tmp.{os.getpid()}.{threading.get_ident()}"
    try:
        with open(tmp_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for post in posts:
                writer.writerow(post)
            csvfile.flush()
            os.fsync(csvfile.fileno())
        os.replace(tmp_file, output_file)
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

    print(f"✅ Successfully saved {len(posts)} posts to: {output_file}")


def newsletter_csv_path(url: str, window_dir: str) -> str:
    """Per-newsletter CSV path inside the folder for this run's date window."""
    return os.path.join(window_dir, f"{sanitize_filename(extract_newsletter_name_from_url(url))}.csv")


def check_existing_newsletter_file(url: str, window_dir: str) -> Optional[str]:
    """Return this newsletter's CSV for the same date window if it exists (empty files count as done)."""
    path = newsletter_csv_path(url, window_dir)
    return path if os.path.exists(path) else None


def load_existing_posts_from_csv(csv_file: str) -> List[Dict]:
    """Load existing posts from a CSV file."""
    posts = []
    try:
        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                posts.append(dict(row))
        print(f"   📄 Loaded {len(posts)} existing posts from {csv_file}")
        return posts
    except Exception as e:
        print(f"   ⚠️  Error loading existing CSV {csv_file}: {e}")
        return []


def sanitize_filename(name: str) -> str:
    """Sanitize a string to be safe for use as a filename."""
    # Replace or remove characters that are not safe for filenames
    # Keep alphanumeric, spaces, hyphens, and underscores
    
    # Replace unsafe characters with underscores
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace multiple spaces/underscores with single underscore
    sanitized = re.sub(r'[_\s]+', '_', sanitized)
    # Remove leading/trailing underscores and spaces
    sanitized = sanitized.strip('_ ')
    # Limit length to avoid filesystem issues
    if len(sanitized) > 50:
        sanitized = sanitized.rstrip('_')
    return sanitized if sanitized else 'unknown'


def extract_newsletter_name_from_url(url: str) -> str:
    """Extract newsletter name from Substack URL."""
    try:
        # First normalize the URL to handle @username format and other cases
        normalized_url = normalize_substack_url(url)
        parsed = urlparse(normalized_url)
        
        if '.substack.com' in parsed.netloc:
            # Extract the subdomain part (e.g., 'cryptogood' from 'cryptogood.substack.com')
            subdomain = parsed.netloc.split('.substack.com')[0]
            newsletter_name = subdomain if subdomain else parsed.netloc
        else:
            # For custom domains, use the full domain
            newsletter_name = parsed.netloc
            
        return sanitize_filename(newsletter_name)
    except Exception as e:
        print(f"   ⚠️  Error extracting newsletter name from URL {url}: {e}")
        return 'unknown'


def get_newsletters_from_profile(profile_url: str) -> List[str]:
    """Extract newsletter URLs from a Substack profile page."""
    try:
        print(f"📋 Fetching newsletters from profile: {profile_url}")
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(profile_url, headers=headers, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        content = response.text

        # Current format: preloads JSON lives in <script id="preloads-json" type="application/json">
        json_tag = re.search(r'<script[^>]*id="preloads-json"[^>]*>(.*?)</script>', content, re.DOTALL)
        if json_tag:
            try:
                data = json.loads(json_tag.group(1))
                if isinstance(data, str):
                    data = json.loads(data)
                return _extract_newsletter_urls(data)
            except json.JSONDecodeError as e:
                print(f"   ❌ Failed to parse profile JSON: {e}")
                return []

        # Legacy format: window._preloads = JSON.parse("...")
        # More flexible regex to handle various whitespace patterns and escaped quotes
        # The JSON string can contain escaped quotes and span multiple lines
        preloads_match = re.search(r'window\._preloads\s*=\s*JSON\.parse\s*\(\s*"((?:[^"\\]|\\.)*)"\s*\)', content, re.DOTALL)
        if not preloads_match:
            # Try alternative pattern in case the JSON string uses single quotes or different structure
            preloads_match = re.search(r'window\._preloads\s*=\s*JSON\.parse\s*\(\s*\'((?:[^\'\\]|\\.)*)\'\s*\)', content, re.DOTALL)
        
        if not preloads_match:
            print("   ❌ Could not find profile data in page")
            return []
        
        # Decode the JSON string (it's double-encoded)
        json_str = preloads_match.group(1)
        # Unescape the JSON string
        json_str = json_str.replace('\\"', '"').replace('\\\\', '\\')
        
        try:
            data = json.loads(json_str)
            print(f"   ✅ Successfully parsed JSON data")
        except json.JSONDecodeError as e:
            print(f"   ❌ Failed to parse profile JSON: {e}")
            return []

        return _extract_newsletter_urls(data)

    except requests.RequestException as e:
        print(f"   ❌ Error fetching profile page: {e}")
        return []
    except Exception as e:
        print(f"   ❌ Unexpected error parsing profile: {e}")
        return []


def _extract_newsletter_urls(data: dict) -> List[str]:
    """Pull newsletter URLs from a profile page's preloads data."""
    subscriptions = data.get('profile', {}).get('subscriptions', [])
    if not subscriptions:
        print("   ⚠️  No subscriptions found in profile")
        return []

    newsletter_urls = []
    for subscription in subscriptions:
        publication = subscription.get('publication', {})
        if not publication:
            continue
        custom_domain = publication.get('custom_domain')
        subdomain = publication.get('subdomain')
        if custom_domain:
            newsletter_urls.append(f"https://{custom_domain}")
        elif subdomain:
            newsletter_urls.append(f"https://{subdomain}.substack.com")

    print(f"   ✅ Found {len(newsletter_urls)} newsletter(s)")
    return newsletter_urls


def main():
    parser = argparse.ArgumentParser(
        description='Fetch Substack posts published within a time window with like counts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python substack_surfacer.py --url https://example.substack.com --from 2025-01-01 --to 2025-12-31
  python substack_surfacer.py --urls newsletters.txt --from 2023-12-01 --to 2023-12-31 --output posts.csv
  python substack_surfacer.py --user stephenreid --from 2025-01-01 --to 2025-12-31
  python substack_surfacer.py --urls urls.txt --from "2025-01-01 12:00:00" --to "2025-01-31 23:59:59"
  
Resume functionality:
  If the script fails halfway through, simply run the same command again.
  It will skip newsletters already scraped for the same --from/--to window
  (per-newsletter files live in substacks/<from>_to_<to>/).
        """
    )
    
    # URL input options (mutually exclusive)
    url_group = parser.add_mutually_exclusive_group(required=True)
    url_group.add_argument('--url', type=str, help='Single Substack newsletter URL')
    url_group.add_argument('--urls', type=str, help='File containing Substack URLs (one per line, or tab-separated URL<TAB>CATEGORY format)')
    url_group.add_argument('--user', type=str, help='Substack profile username (e.g., stephenreid or @stephenreid)')
    
    # Required arguments
    parser.add_argument('--from', type=str, required=True, dest='from_date',
                       help='Start date for time window (YYYY-MM-DD)')
    parser.add_argument('--to', type=str, required=True, dest='to_date',
                       help='End of time window: a date (YYYY-MM-DD) includes that whole day; a date and time is an exact cut-off')
    
    # Optional arguments
    parser.add_argument('--output', type=str, default='posts.csv', 
                       help='Output file name (default: posts.csv)')
    parser.add_argument('--max-posts', type=int, default=None,
                       help='Keep at most this many (newest) posts per newsletter (default: no limit)')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                       help=f'Newsletters to fetch in parallel (default: {DEFAULT_WORKERS})')
    parser.add_argument('--force-rescrape', action='store_true',
                       help='Force re-scraping of all newsletters, even if CSV files exist')
    
    args = parser.parse_args()
    
    # Parse the from and to dates
    try:
        from_date = parse_datetime(args.from_date, raise_on_error=True)
        to_date = parse_datetime(args.to_date, raise_on_error=True)
        # A bare date means "through the end of that day"; a time is an exact (inclusive) cut-off
        if ':' in args.to_date:
            to_date_exclusive = to_date + timedelta(seconds=1)
        else:
            to_date_exclusive = to_date + timedelta(days=1)
        
        # Validate date range
        if from_date >= to_date:
            raise ValueError("From date must be earlier than to date")
        
    except ValueError as e:
        print(f"❌ {e}")
        return
    
    # Get list of URLs
    if args.url:
        urls = [{'url': args.url, 'category': ''}]
    elif args.user:
        # Handle profile username - add @ if not present and construct full URL
        username = args.user.lstrip('@')  # Remove @ if present
        profile_url = f"https://substack.com/@{username}"
        newsletter_urls = get_newsletters_from_profile(profile_url)
        urls = [{'url': url, 'category': ''} for url in newsletter_urls]
    else:
        urls = load_urls_from_file(args.urls)
        if not urls:
            print("❌ No valid URLs found.")
            return
    
    print(f"🔗 Processing {len(urls)} newsletter(s)...")

    # Each date window gets its own folder, so resume never reuses results from a different window
    window_dir = os.path.join('substacks', sanitize_filename(f"{args.from_date}_to_{args.to_date}"))
    if len(urls) > 1:
        print(f"📁 Per-newsletter files: {window_dir}/")
    
    def process_newsletter(i: int, url_data: Dict[str, str]):
        """Returns (status, posts) where status is 'skipped', 'processed' or 'failed'."""
        url = url_data['url']
        category = url_data['category']
        category_display = f" ({category})" if category else ""
        print(f"\n📰 Processing newsletter {i}/{len(urls)}: {url}{category_display}")

        # Check if this newsletter was already scraped (unless force-rescrape is enabled)
        if not args.force_rescrape:
            existing_file = check_existing_newsletter_file(url, window_dir)
            if existing_file:
                print(f"   ✅ Found existing file: {existing_file}")
                print(f"   ⏭️  Skipping this newsletter (use --force-rescrape to override)")
                existing_posts = load_existing_posts_from_csv(existing_file)
                # Update category in existing posts if it wasn't set before
                for post in existing_posts:
                    if not post.get('category') and category:
                        post['category'] = category
                return 'skipped', existing_posts

        try:
            posts = fetch_newsletter_posts(url, from_date, to_date_exclusive, args.max_posts, category)
        except Exception as e:
            print(f"   ❌ Error processing newsletter {url}: {e}")
            print(f"   🔄 You can resume by running the same command again - completed newsletters will be skipped")
            return 'failed', []

        if not posts:
            print(f"   📝 No posts found for this newsletter - creating empty file to mark as processed")
        else:
            print(f"   ✅ Found {len(posts)} posts")

        if len(urls) > 1:
            individual_output = newsletter_csv_path(url, window_dir)
        else:
            individual_output = args.output

        save_posts_to_csv(posts, individual_output)
        print(f"   ✅ Saved {len(posts)} posts to: {individual_output}")
        return 'processed', posts

    def run_buffered(i, url_data):
        log.start_buffer()
        try:
            return process_newsletter(i, url_data)
        finally:
            log.end_buffer()

    log = ThreadBufferedStdout(sys.stdout)
    sys.stdout = log
    results: Dict[int, Any] = {}
    try:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
            futures = {pool.submit(run_buffered, i, url_data): i for i, url_data in enumerate(urls, 1)}
            for done_count, future in enumerate(as_completed(futures), 1):
                results[futures[future]] = future.result()
                print(f"   ⏳ {done_count}/{len(urls)} newsletters done")
    finally:
        sys.stdout = log.real

    # Keep the combined output in input order regardless of completion order
    all_posts = []
    skipped_newsletters = []
    processed_newsletters = []
    failed_newsletters = []
    for i, url_data in enumerate(urls, 1):
        status, posts = results[i]
        all_posts.extend(posts)
        {'skipped': skipped_newsletters, 'processed': processed_newsletters,
         'failed': failed_newsletters}[status].append(url_data['url'])

    # Save combined results if processing multiple newsletters
    if len(urls) > 1:
        print(f"\n💾 Saving combined results from all {len(urls)} newsletters...")
        
        combined_output = os.path.join('combined.csv')
        
        save_posts_to_csv(all_posts, combined_output)
        
        print(f"✅ Saved combined results: {combined_output}")
    
    # Summary
    print(f"\n🎉 Completed! Processed {len(all_posts)} posts total.")
    if processed_newsletters:
        print(f"📊 Newly processed newsletters: {len(processed_newsletters)}")
    if skipped_newsletters:
        print(f"⏭️  Skipped newsletters (already existed): {len(skipped_newsletters)}")
    if failed_newsletters:
        print(f"❌ Failed newsletters (re-run to retry): {len(failed_newsletters)}")
        for url in failed_newsletters:
            print(f"   - {url}")
    
    print(f"📊 Individual newsletter files saved after each processing step")
    if len(urls) > 1:
        print(f"📊 Combined results also saved for all newsletters")
    
    if processed_newsletters and not skipped_newsletters:
        print(f"🚀 All newsletters were freshly processed")
    elif skipped_newsletters and not processed_newsletters:
        print(f"♻️  All newsletters were loaded from existing files")
    else:
        print(f"🔄 Resume functionality used - some newsletters skipped, others processed")


if __name__ == '__main__':
    main() 