#!/usr/bin/env python3
"""
Build the companies.json data file from raw edition HTML files.
This script processes HTML files stored in raw_editions/ and outputs companies.json.
"""

import json
import re
import os
from pathlib import Path
from datetime import datetime

DATA_DIR = Path("../data")
RAW_DIR = DATA_DIR / "raw_editions"
COMPANIES_FILE = DATA_DIR / "companies.json"
DESCRIPTIONS_CACHE_FILE = DATA_DIR / "descriptions_cache.json"


def parse_job_line(line: str) -> dict | None:
    """Parse a single job listing line."""
    line = line.strip()
    if not line or len(line) < 10:
        return None
    
    # Pattern: Job Title, Company Name (Industry, Stage), Location
    match = re.match(
        r'^([^,]+),\s*'  # Job title
        r'([^(]+?)\s*'   # Company name
        r'\(([^)]+)\),?\s*'  # Details in parens
        r'(.+?)$',       # Location
        line
    )
    
    if not match:
        return None
    
    job_title = match.group(1).strip()
    company_name = match.group(2).strip()
    details = match.group(3).strip()
    location = match.group(4).strip()
    
    # Filter out non-job content
    if len(company_name) < 2 or len(company_name) > 100:
        return None
    if len(location) > 100:
        return None
    if any(skip in company_name.lower() for skip in ['subscribe', 'click here', 'fill out']):
        return None
    
    # Parse details (Industry, Stage, sometimes investor info)
    details_parts = [p.strip() for p in details.split(',')]
    
    industry = details_parts[0] if details_parts else ""
    stage = ""
    investors = ""
    
    for part in details_parts[1:]:
        part_lower = part.lower()
        if any(s in part_lower for s in ['series', 'seed', 'public', 'early-stage', 'early stage', 'late-stage', 'acquired']):
            stage = part.strip()
        elif 'backed' in part_lower:
            investors = part.strip()
    
    # Clean up
    company_name = re.sub(r'[,\s]+$', '', company_name)
    location = re.sub(r'^[/\s]+|[/\s]+$', '', location)
    
    return {
        'company': company_name,
        'industry': industry.strip(),
        'stage': stage.strip(),
        'location': location.strip(),
        'investors': investors.strip(),
        'job_title': job_title.strip()
    }


def parse_edition_html(html: str, edition_num: int, date: str) -> list[dict]:
    """Parse edition HTML and extract company listings."""
    companies = []
    current_category = None
    
    # Split by lines and find job listings
    lines = html.split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check for section headers
        lower = line.lower()
        if 'chief of staff' in lower and 'role' in lower:
            current_category = 'Chief of Staff'
            continue
        elif 'bizops' in lower and 'role' in lower:
            current_category = 'BizOps'
            continue
        elif 'vc' in lower and 'role' in lower:
            current_category = 'VC'
            continue
        
        # Try to parse as job listing
        parsed = parse_job_line(line)
        if parsed:
            parsed['edition'] = edition_num
            parsed['date'] = date
            parsed['role_category'] = current_category
            companies.append(parsed)
    
    return companies


def deduplicate_companies(companies: list[dict]) -> list[dict]:
    """Deduplicate companies, keeping the most recent appearance."""
    company_map = {}
    
    for c in companies:
        key = c['company'].lower().strip()
        
        if key not in company_map:
            company_map[key] = {
                'company': c['company'],
                'industry': c['industry'],
                'stage': c['stage'],
                'location': c['location'],
                'investors': c.get('investors', ''),
                'editions': [c['edition']],
                'latest_edition': c['edition'],
                'latest_date': c.get('date', ''),
                'role_categories': [c.get('role_category')] if c.get('role_category') else []
            }
        else:
            existing = company_map[key]
            if c['edition'] not in existing['editions']:
                existing['editions'].append(c['edition'])
            if c['edition'] > existing['latest_edition']:
                existing['latest_edition'] = c['edition']
                existing['latest_date'] = c.get('date', '')
                # Update fields if empty
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
    """Load cached descriptions."""
    if DESCRIPTIONS_CACHE_FILE.exists():
        with open(DESCRIPTIONS_CACHE_FILE) as f:
            return json.load(f)
    return {}


def save_descriptions_cache(cache: dict):
    """Save descriptions cache."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(DESCRIPTIONS_CACHE_FILE, 'w') as f:
        json.dump(cache, f, indent=2)


def generate_description_with_claude(company: dict, api_key: str) -> str:
    """Generate a company description using Claude API."""
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=api_key)
        
        prompt = f"""Generate a concise 1-2 sentence description of what this company does. Be factual and brief.

Company: {company['company']}
Industry: {company['industry']}
Stage: {company['stage']}
Location: {company['location']}
{f"Investors: {company['investors']}" if company.get('investors') else ""}

If you don't have enough information, make a reasonable inference based on the industry and company name. Don't mention funding stage or location in the description."""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=150,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text.strip()
    except Exception as e:
        print(f"Error generating description for {company['company']}: {e}")
        return ""


def add_descriptions(companies: list[dict], api_key: str = None) -> list[dict]:
    """Add descriptions to companies, using cache."""
    cache = load_descriptions_cache()
    updated = 0
    
    for i, company in enumerate(companies):
        key = company['company'].lower().strip()
        
        if key in cache and cache[key]:
            company['description'] = cache[key]
        elif api_key:
            print(f"Generating description for {company['company']} ({i+1}/{len(companies)})...")
            description = generate_description_with_claude(company, api_key)
            company['description'] = description
            cache[key] = description
            updated += 1
            
            if updated % 10 == 0:
                save_descriptions_cache(cache)
        else:
            company['description'] = ""
    
    if updated > 0:
        save_descriptions_cache(cache)
        print(f"Generated {updated} new descriptions")
    
    return companies


def save_companies(companies: list[dict]):
    """Save companies to JSON file."""
    DATA_DIR.mkdir(exist_ok=True)
    with open(COMPANIES_FILE, 'w') as f:
        json.dump({
            'last_updated': datetime.now().isoformat(),
            'total_companies': len(companies),
            'companies': companies
        }, f, indent=2)
    print(f"Saved {len(companies)} companies to {COMPANIES_FILE}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='Build companies.json from raw HTML files')
    parser.add_argument('--api-key', type=str, help='Anthropic API key for descriptions')
    args = parser.parse_args()
    
    api_key = args.api_key or os.environ.get('ANTHROPIC_API_KEY')
    
    # Load all raw edition files
    all_companies = []
    
    if RAW_DIR.exists():
        for html_file in sorted(RAW_DIR.glob('*.html')):
            # Extract edition number from filename (e.g., edition_241.html)
            match = re.search(r'edition_(\d+)', html_file.stem)
            if match:
                edition_num = int(match.group(1))
                with open(html_file) as f:
                    html = f.read()
                
                # Try to extract date from content
                date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d+,?\s*\d{4}', html)
                date = date_match.group(0) if date_match else ""
                
                companies = parse_edition_html(html, edition_num, date)
                all_companies.extend(companies)
                print(f"Parsed edition {edition_num}: {len(companies)} listings")
    
    if not all_companies:
        print("No data found. Run scraper.py first to fetch editions.")
        return
    
    # Deduplicate
    companies = deduplicate_companies(all_companies)
    print(f"Deduplicated to {len(companies)} unique companies")
    
    # Add descriptions
    if api_key:
        companies = add_descriptions(companies, api_key)
    
    # Save
    save_companies(companies)


if __name__ == '__main__':
    main()
