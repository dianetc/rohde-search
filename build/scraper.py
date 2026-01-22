#!/usr/bin/env python3
"""
Full scraper for Ali Rohde Jobs newsletter.
Run this locally to fetch all 241+ editions and build the complete dataset.

Usage:
    python scraper.py --full              # Full scrape of all editions
    python scraper.py --update            # Update with latest edition only
    python scraper.py --api-key YOUR_KEY  # Add descriptions with Claude API
"""

import json
import re
import time
import os
import argparse
from datetime import datetime
from pathlib import Path

try:
    import requests
    from bs4 import BeautifulSoup
except ImportError:
    print("Please install required packages: pip install requests beautifulsoup4")
    exit(1)

# Configuration
BASE_URL = "https://alirohdejobs.substack.com"
DATA_DIR = Path("../data")
COMPANIES_FILE = DATA_DIR / "companies.json"
DESCRIPTIONS_CACHE_FILE = DATA_DIR / "descriptions_cache.json"

# Known URL overrides for editions with non-standard URLs
# Add any new ones you discover here
KNOWN_URL_OVERRIDES = {
    238: f"{BASE_URL}/p/edition-237-ali-rohde-jobs-975",
    224: f"{BASE_URL}/p/edition-224-ali-rohde-jobs-188",
    46: f"{BASE_URL}/p/ali-rohde-jobs-cos-at-headway-nuro",
    10: f"{BASE_URL}/p/edition-10",
    9: f"{BASE_URL}/p/edition-09-ali-rohde-jobs",
    8: f"{BASE_URL}/p/edition-08",
    7: f"{BASE_URL}/p/edition-06-ali-rohde-jobs-3b9",
    6: f"{BASE_URL}/p/edition-06-ali-rohde-jobs",
    5: f"{BASE_URL}/p/edition-05-ali-rohde-jobs",
    4: f"{BASE_URL}/p/edition-04-ali-rohde-jobs",
    3: f"{BASE_URL}/p/edition-03",
    2: f"{BASE_URL}/p/edition-02-ali-rohde-jobs",
    1: f"{BASE_URL}/p/edition-01-ali-rohde-jobs",
}


def fetch_page(url: str, retries: int = 5) -> str:
    """Fetch a page with retries and rate limit handling."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
    }
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            # Handle rate limiting
            if response.status_code == 429:
                wait_time = min(60, 10 * (2 ** attempt))
                print(f"    Rate limited! Waiting {wait_time}s...")
                time.sleep(wait_time)
                continue
            
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            if attempt == retries - 1:
                raise
            wait_time = 2 ** attempt
            print(f"  Retry {attempt + 1} for {url}: {e}")
            time.sleep(wait_time)
    return ""


def get_all_edition_urls() -> list[dict]:
    """Get all edition URLs by scanning the archive."""
    editions = []
    seen_editions = set()
    
    # Fetch main archive page
    print("Fetching archive...")
    html = fetch_page(f"{BASE_URL}/archive?sort=new")
    
    # Extract edition links using regex
    pattern = re.compile(r'/p/edition-(\d+)-ali-rohde-jobs[^"\']*')
    
    for match in pattern.finditer(html):
        edition_num = int(match.group(1))
        if edition_num not in seen_editions:
            seen_editions.add(edition_num)
            url = f"{BASE_URL}{match.group(0)}"
            url = url.split('"')[0].split("'")[0]
            editions.append({
                'number': edition_num,
                'url': url
            })
    
    # Also try standard URL pattern for editions we might have missed
    max_edition = max(seen_editions) if seen_editions else 241
    for num in range(1, max_edition + 1):
        if num not in seen_editions:
            # Check if we have an override for this edition
            if num in KNOWN_URL_OVERRIDES:
                url = KNOWN_URL_OVERRIDES[num]
            else:
                url = f"{BASE_URL}/p/edition-{num}-ali-rohde-jobs"
            editions.append({
                'number': num,
                'url': url
            })
    
    return sorted(editions, key=lambda x: x['number'], reverse=True)


def fetch_edition(edition_num: int, default_url: str) -> str:
    """Fetch an edition, trying override URL first if available."""
    # Try override URL first
    if edition_num in KNOWN_URL_OVERRIDES:
        try:
            return fetch_page(KNOWN_URL_OVERRIDES[edition_num])
        except:
            pass  # Fall through to default
    
    # Try the default/discovered URL
    try:
        return fetch_page(default_url)
    except requests.HTTPError as e:
        if e.response.status_code != 404:
            raise
    
    raise Exception(f"Could not find edition {edition_num}")


def parse_job_line(line: str) -> dict | None:
    """Parse a job listing line."""
    line = line.strip()
    if not line or len(line) < 15:
        return None
    
    # Strip markdown links: [Text](URL) -> Text
    line = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', line)
    
    match = re.match(
        r'^([^,]+),\s*'
        r'([^(]+?)\s*'
        r'\(([^)]+)\),?\s*'
        r'(.+?)$',
        line
    )
    
    if not match:
        return None
    
    job_title = match.group(1).strip()
    company_name = match.group(2).strip()
    details = match.group(3).strip()
    location = match.group(4).strip()
    
    if len(company_name) < 2 or len(company_name) > 100:
        return None
    if len(location) > 100:
        return None
    skip_words = ['subscribe', 'click here', 'fill out', 'form here', 'newsletter']
    if any(s in company_name.lower() for s in skip_words):
        return None
    
    details_parts = [p.strip() for p in details.split(',')]
    industry = details_parts[0] if details_parts else ""
    stage = ""
    investors = ""
    
    for part in details_parts[1:]:
        pl = part.lower()
        if any(s in pl for s in ['series', 'seed', 'public', 'early-stage', 'early stage', 'late-stage', 'acquired']):
            stage = part.strip()
        elif 'backed' in pl:
            investors = part.strip()
    
    company_name = re.sub(r'[,\s]+$', '', company_name)
    location = re.sub(r'^[/\s]+|[/\s]+$', '', location)
    
    return {
        'company': company_name,
        'industry': industry.strip(),
        'stage': stage.strip(),
        'location': location.strip(),
        'investors': investors.strip()
    }


def parse_edition(html: str, edition_num: int) -> list[dict]:
    """Parse edition HTML and extract companies."""
    soup = BeautifulSoup(html, 'html.parser')
    companies = []
    current_category = None
    
    date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,?\s*\d{4}', html)
    date = date_match.group(0) if date_match else ""
    
    # Find all paragraph and list item elements - these contain the job listings
    elements = soup.find_all(['p', 'li', 'h3', 'h4'])
    
    for el in elements:
        text = el.get_text(strip=True)
        if not text:
            continue
        
        lower = text.lower()
        if 'chief of staff' in lower and 'role' in lower:
            current_category = 'Chief of Staff'
            continue
        elif 'bizops' in lower and 'role' in lower:
            current_category = 'BizOps'
            continue
        elif 'vc' in lower and 'role' in lower:
            current_category = 'VC'
            continue
        
        # Get the text including link text (job titles are often in links)
        # Convert <a> tags to their text content
        line = text
        
        # Also try to extract href and text from any links
        links = el.find_all('a')
        if links:
            # Reconstruct the line by getting link text followed by remaining text
            parts = []
            for link in links:
                parts.append(link.get_text(strip=True))
            # Get full text and strip the link texts we already have
            line = el.get_text(strip=True)
        
        parsed = parse_job_line(line)
        if parsed:
            parsed['edition'] = edition_num
            parsed['date'] = date
            parsed['role_category'] = current_category
            companies.append(parsed)
    
    return companies


def deduplicate_companies(companies: list[dict]) -> list[dict]:
    """Deduplicate companies, keeping most recent."""
    company_map = {}
    
    for c in companies:
        key = c['company'].lower().strip()
        
        if key not in company_map:
            company_map[key] = {
                'company': c['company'],
                'industry': c['industry'],
                'stage': c.get('stage', ''),
                'location': c['location'],
                'investors': c.get('investors', ''),
                'editions': [c['edition']],
                'latest_edition': c['edition'],
                'latest_date': c.get('date', ''),
                'role_categories': [c.get('role_category')] if c.get('role_category') else [],
                'description': ''
            }
        else:
            existing = company_map[key]
            if c['edition'] not in existing['editions']:
                existing['editions'].append(c['edition'])
            if c['edition'] > existing['latest_edition']:
                existing['latest_edition'] = c['edition']
                existing['latest_date'] = c.get('date', '')
                if not existing['industry'] and c['industry']:
                    existing['industry'] = c['industry']
                if not existing['stage'] and c['stage']:
                    existing['stage'] = c['stage']
                if not existing['location'] and c['location']:
                    existing['location'] = c['location']
            if c.get('role_category') and c['role_category'] not in existing['role_categories']:
                existing['role_categories'].append(c['role_category'])
    
    result = list(company_map.values())
    result.sort(key=lambda x: x['latest_edition'], reverse=True)
    return result


def load_descriptions_cache() -> dict:
    if DESCRIPTIONS_CACHE_FILE.exists():
        with open(DESCRIPTIONS_CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_descriptions_cache(cache: dict):
    DATA_DIR.mkdir(exist_ok=True)
    with open(DESCRIPTIONS_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def generate_descriptions(companies: list[dict], api_key: str) -> list[dict]:
    """Generate descriptions using Claude API."""
    try:
        from anthropic import Anthropic
    except ImportError:
        print("Install anthropic package for descriptions: pip install anthropic")
        return companies
    
    cache = load_descriptions_cache()
    client = Anthropic(api_key=api_key)
    updated = 0
    
    for i, company in enumerate(companies):
        key = company['company'].lower().strip()
        
        if key in cache and cache[key]:
            company['description'] = cache[key]
        else:
            print(f"  Generating description for {company['company']} ({i+1}/{len(companies)})...")
            
            prompt = f"""Generate a concise 1-2 sentence description of what this company does. Be factual and brief.

Company: {company['company']}
Industry: {company['industry']}
Stage: {company['stage']}
Location: {company['location']}

If you don't have enough information, make a reasonable inference based on the industry and company name. Don't mention funding stage or location."""

            try:
                response = client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=150,
                    messages=[{"role": "user", "content": prompt}]
                )
                description = response.content[0].text.strip()
                company['description'] = description
                cache[key] = description
                updated += 1
                
                if updated % 20 == 0:
                    save_descriptions_cache(cache)
                
                time.sleep(0.3)
            except Exception as e:
                print(f"    Error: {e}")
                company['description'] = ""
    
    save_descriptions_cache(cache)
    print(f"Generated {updated} new descriptions")
    return companies


def save_companies(companies: list[dict]):
    DATA_DIR.mkdir(exist_ok=True)
    with open(COMPANIES_FILE, 'w') as f:
        json.dump({
            'last_updated': datetime.now().isoformat(),
            'total_companies': len(companies),
            'companies': companies
        }, f, indent=2)
    print(f"Saved {len(companies)} companies to {COMPANIES_FILE}")


def load_existing_companies() -> list[dict]:
    if COMPANIES_FILE.exists():
        with open(COMPANIES_FILE) as f:
            data = json.load(f)
            return data.get('companies', [])
    return []


def full_scrape(api_key: str = None):
    """Scrape all editions."""
    print("Starting full scrape...")
    editions = get_all_edition_urls()
    print(f"Found {len(editions)} editions to scrape")
    
    all_companies = []
    failed = []
    
    for i, edition in enumerate(editions):
        try:
            print(f"[{i+1}/{len(editions)}] Fetching edition {edition['number']}...")
            html = fetch_edition(edition['number'], edition['url'])
            companies = parse_edition(html, edition['number'])
            all_companies.extend(companies)
            print(f"  Found {len(companies)} listings")
            
            # Rate limiting - longer delay to avoid 429s
            time.sleep(1.5)
            
        except Exception as e:
            print(f"  Error: {e}")
            failed.append(edition['number'])
            continue
    
    print(f"\nTotal raw listings: {len(all_companies)}")
    if failed:
        print(f"Failed editions: {failed}")
    
    companies = deduplicate_companies(all_companies)
    print(f"Unique companies: {len(companies)}")
    
    if api_key:
        print("\nGenerating descriptions...")
        companies = generate_descriptions(companies, api_key)
    
    save_companies(companies)


def update_latest(api_key: str = None):
    """Update with latest edition only."""
    print("Checking for new editions...")
    
    existing = load_existing_companies()
    existing_editions = set()
    for c in existing:
        existing_editions.update(c.get('editions', [c.get('latest_edition', 0)]))
    
    max_existing = max(existing_editions) if existing_editions else 0
    print(f"Current latest edition: {max_existing}")
    
    html = fetch_page(f"{BASE_URL}/archive?sort=new")
    match = re.search(r'/p/edition-(\d+)-ali-rohde-jobs', html)
    
    if not match:
        print("Could not find latest edition")
        return
    
    latest_num = int(match.group(1))
    print(f"Latest available: {latest_num}")
    
    if latest_num <= max_existing:
        print("Already up to date!")
        return
    
    new_companies = []
    for num in range(max_existing + 1, latest_num + 1):
        url = KNOWN_URL_OVERRIDES.get(num, f"{BASE_URL}/p/edition-{num}-ali-rohde-jobs")
        print(f"Fetching edition {num}...")
        try:
            html = fetch_edition(num, url)
            companies = parse_edition(html, num)
            new_companies.extend(companies)
            print(f"  Found {len(companies)} listings")
            time.sleep(1.5)
        except Exception as e:
            print(f"  Error: {e}")
    
    if not new_companies:
        print("No new companies found")
        return
    
    all_raw = []
    for c in existing:
        for ed in c.get('editions', [c.get('latest_edition')]):
            all_raw.append({
                'company': c['company'],
                'industry': c['industry'],
                'stage': c.get('stage', ''),
                'location': c['location'],
                'investors': c.get('investors', ''),
                'edition': ed,
                'date': c.get('latest_date', ''),
                'role_category': c.get('role_categories', [''])[0] if c.get('role_categories') else ''
            })
    
    all_raw.extend(new_companies)
    companies = deduplicate_companies(all_raw)
    
    cache = load_descriptions_cache()
    for c in companies:
        key = c['company'].lower().strip()
        if key in cache:
            c['description'] = cache[key]
    
    if api_key:
        companies = generate_descriptions(companies, api_key)
    
    save_companies(companies)
    print(f"Updated! Now have {len(companies)} companies")


def main():
    parser = argparse.ArgumentParser(description='Scrape Ali Rohde Jobs newsletter')
    parser.add_argument('--full', action='store_true', help='Full scrape of all editions')
    parser.add_argument('--update', action='store_true', help='Update with latest edition only')
    parser.add_argument('--api-key', type=str, help='Anthropic API key for descriptions')
    args = parser.parse_args()
    
    api_key = args.api_key or os.environ.get('ANTHROPIC_API_KEY')
    
    if args.update:
        update_latest(api_key)
    elif args.full:
        full_scrape(api_key)
    else:
        print("Usage:")
        print("  python scraper.py --full              # Scrape all editions")
        print("  python scraper.py --update            # Update with latest only")
        print("  python scraper.py --api-key YOUR_KEY  # Add Claude API key for descriptions")


if __name__ == '__main__':
    main()
