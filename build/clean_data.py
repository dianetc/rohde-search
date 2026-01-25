#!/usr/bin/env python3
"""
Clean up the scraped companies data without re-scraping.
Fixes data quality issues, normalizes variations, and removes garbage entries.
"""

import json
import re
from pathlib import Path
from collections import defaultdict

def is_valid_industry(industry):
    """Check if industry looks valid."""
    if not industry or len(industry) < 2:
        return False

    industry_lower = industry.lower()

    # Filter out obvious garbage
    garbage_patterns = [
        r'^\d{4}$',  # Years like 2024, 2025
        r'^@',  # Twitter handles
        r'^it was',
        r'^she is',
        r'^he is',
        r'^up to',
        r'^applications',
        r'^I (added|invest)',
        r'^backed by',
        r'^\(',  # Starts with parenthesis
        r'^\.$',  # Just a period
        r'^h/t ',  # Hat tip
        r'respond to',
        r'check out',
        r'acq\s+by',  # Acquired by X
        r'acquired by',
        r'acq\.',  # Acq.
        r'just raised',  # Funding announcements
        r'raised \$',  # Funding amounts
        r'series [a-h]',  # Series A, B, C, etc
        r'seed.?stage',  # Seed stage, Seed-stage
        r'pre.?seed',  # Pre-seed
        r'yc/',  # YC/Series A
        r'hiring for',  # Job postings
        r'backgrounds welcome',
        r'always free',
        r'preferred',
        r"'s (new )?fund",  # Someone's fund
        r"'s (chocolate|aerospace|social impact)",  # Descriptions
        r'spin-off',
        r'division',
        r'unit within',
        r'arm of',
        r'incubation',
        r'new body',
        r'make binding',
        r'venture fund$',  # Just "venture fund"
        r'^holding company',
        r'^startup studio',
        r'^accelerator$',
        r'^incubator$',
        r'fka ',  # formerly known as
        r'former ',
        r'fund led by',
        r'^era$',  # Accelerator name
        r'^wil$',  # VC name
        r'^iqt$',  # VC name
        r'^czi$',  # Chan Zuckerberg Initiative
        r'^scf$',  # Some VC
        r'^usdr$',  # US Digital Response
        r'nonprofit that provides',
        r'company covering the',
    ]

    for pattern in garbage_patterns:
        if re.search(pattern, industry_lower):
            return False

    # Filter out if too long (probably a description)
    if len(industry) > 50:
        return False

    return True

def is_valid_location(location):
    """Check if location looks valid."""
    if not location or len(location) < 2:
        return False

    location_lower = location.lower()

    # Filter out obvious garbage
    garbage_patterns = [
        r'^\.',  # Starts with period
        r'^@',  # Twitter handles
        r'^\(',  # Starts with parenthesis
        r'\($',  # Ends with parenthesis
        r'^it was',
        r'^she is',
        r'^he is',
        r'capital',  # VC names
        r'ventures',  # VC names
        r'partners',  # VC names
        r'collective',  # VC names
        r'group$',  # VC names
        r'next$',  # Samsung Next
        r'fund',  # Investment funds
        r'respond to',
        r'check out',
        r'^and ',
        r'^or ',
        r'ceo ',  # CEO names
        r'founder',
        r'announcement',
        r'how it works',
        r'jobs!',
        r'for grabs',
        r'series [a-f]',  # Series A, B, C etc
        r'stage$',  # early-stage, etc
        r'seed',
        r'acquired',
        r'public\)',
        r'healthcare\)',
        r'fintech\)',
        r'saas\)',
        r'edtech\)',
        r'language learning',
        r'contract\)',
        r'manager',
        r'associate',
        r'relations',
        r'holdings',
        r'institute',
        r'company',
    ]

    for pattern in garbage_patterns:
        if re.search(pattern, location_lower):
            return False

    # Filter out if it's too long (probably not a location)
    if len(location) > 60:
        return False

    # Filter out 2-letter state codes that aren't in our known list
    if len(location) == 2 and location.upper() not in ['SF', 'LA', 'NY', 'DC', 'UK']:
        return False

    return True

def is_valid_company(company):
    """Check if company name looks valid."""
    if not company or len(company) < 2:
        return False

    # Filter out obvious garbage
    garbage_patterns = [
        r'^@',
        r'^\d{4}$',
        r'^it was',
        r'^she is',
        r'^he is',
        r'respond to this email',
        r'check out my friend',
        r'hosted earlier',
        r'which teaches',
    ]

    for pattern in garbage_patterns:
        if re.search(pattern, company, re.IGNORECASE):
            return False

    return True

def normalize_industry(industry):
    """Normalize industry names to canonical forms."""
    if not industry:
        return industry

    industry = industry.strip()
    industry_lower = industry.lower()

    # Pattern-based consolidation (more aggressive)
    # Healthcare variations
    if any(term in industry_lower for term in ['health', 'medtech', 'medical', 'clinical', 'behavioral health', 'wellness', 'fitness', 'hospital', 'pharma', 'therapeutic', 'biopharma', 'drug development']):
        # Specific exceptions
        if 'mental health' in industry_lower:
            return 'Mental Health'
        if 'dental' in industry_lower or 'dentis' in industry_lower:
            return 'Healthcare'
        if 'veterinary' in industry_lower or 'vet ' in industry_lower:
            return 'Healthcare'
        return 'Healthcare'

    # AI variations
    if re.search(r'\bai\b|artificial intelligence|machine learning|deep learning|llm|\bml\b|computer vision|nlp', industry_lower):
        # Keep robotics separate
        if 'robot' in industry_lower:
            return 'Robotics'
        return 'AI'

    # Fintech variations
    if any(term in industry_lower for term in ['fintech', 'financial', 'banking', 'payment', 'wealth', 'crypto', 'bitcoin', 'trading', 'investing']):
        if 'crypto' in industry_lower or 'blockchain' in industry_lower or 'web3' in industry_lower or 'web 3' in industry_lower:
            return 'Web3'
        return 'Fintech'

    # Biotech variations
    if any(term in industry_lower for term in ['biotech', 'bio tech', 'life science', 'bioinformatics', 'genetic', 'biopharm']):
        return 'Biotech'

    # E-commerce variations
    if 'commerce' in industry_lower or 'retail' in industry_lower or 'marketplace' in industry_lower:
        return 'E-commerce'

    # Education variations
    if 'edtech' in industry_lower or 'education' in industry_lower or 'e-learning' in industry_lower or 'learning' in industry_lower:
        return 'Edtech'

    # Insurance variations
    if 'insur' in industry_lower:
        return 'Insurtech'

    # Security variations
    if 'security' in industry_lower or 'cybersec' in industry_lower or 'cyber' == industry_lower:
        return 'Cybersecurity'

    # Marketing variations
    if 'marketing' in industry_lower or 'martech' in industry_lower or 'adtech' in industry_lower or 'advertising' in industry_lower:
        return 'Marketing'

    # Logistics variations
    if any(term in industry_lower for term in ['logistic', 'supply chain', 'shipping', 'delivery', 'fleet']):
        return 'Logistics'

    # Real estate variations
    if 'real estate' in industry_lower or 'proptech' in industry_lower or 'property' in industry_lower:
        return 'Real Estate'

    # HR/Recruiting variations
    if any(term in industry_lower for term in ['hrtech', 'hr tech', 'recruiting', 'recruitment', 'talent', 'human resources', 'hiring', 'staffing']):
        return 'HRTech'

    # Legal variations
    if 'legal' in industry_lower or 'legaltech' in industry_lower or 'law' in industry_lower:
        return 'Legal'

    # Climate/Energy variations
    if any(term in industry_lower for term in ['climate', 'clean energy', 'cleantech', 'greentech', 'renewable', 'solar', 'sustainable energy']):
        return 'Climate'

    # Energy variations (non-renewable)
    if 'energy' in industry_lower and 'renewable' not in industry_lower and 'clean' not in industry_lower:
        return 'Energy'

    # Food variations
    if any(term in industry_lower for term in ['food', 'restaurant', 'beverage', 'meal', 'catering', 'grocery']):
        return 'Food & Beverage'

    # Defense variations
    if any(term in industry_lower for term in ['defense', 'aerospace', 'space', 'satellite']):
        if 'space' in industry_lower or 'satellite' in industry_lower or 'aerospace' in industry_lower:
            return 'Aerospace'
        return 'Defense'

    # Travel/Hospitality variations
    if any(term in industry_lower for term in ['travel', 'hospitality', 'hotel', 'ride', 'mobility', 'transportation']):
        return 'Travel'

    # Media/Entertainment variations
    if any(term in industry_lower for term in ['media', 'entertainment', 'video', 'streaming', 'content', 'publishing', 'podcast']):
        if 'gaming' in industry_lower or 'game' in industry_lower or 'esport' in industry_lower:
            return 'Gaming'
        if 'social' in industry_lower:
            return 'Social Media'
        return 'Media'

    # Developer Tools variations
    if any(term in industry_lower for term in ['developer', 'devtools', 'dev tools', 'devops', 'api']):
        return 'Developer Tools'

    # Infrastructure/Cloud variations
    if any(term in industry_lower for term in ['infrastructure', 'cloud', 'data center', 'datacenter', 'edge computing']):
        return 'Infrastructure'

    # Hardware/IoT variations
    if any(term in industry_lower for term in ['hardware', 'iot', 'semiconductor', 'electronics', 'sensor', 'wearable']):
        return 'Hardware'

    # Robotics/Automation variations
    if 'automat' in industry_lower or 'robot' in industry_lower or 'autonomous' in industry_lower or 'drone' in industry_lower:
        return 'Robotics'

    # Manufacturing/Construction variations
    if 'manufacturing' in industry_lower or 'construction' in industry_lower or 'industrial' in industry_lower:
        return 'Manufacturing'

    # Government/Nonprofit variations
    if 'government' in industry_lower or 'govtech' in industry_lower or 'civic' in industry_lower or 'public' == industry_lower:
        return 'Government'

    if 'nonprofit' in industry_lower or 'non-profit' in industry_lower or 'philanthrop' in industry_lower or 'charity' in industry_lower:
        return 'Nonprofit'

    # Agriculture variations
    if 'agri' in industry_lower or 'agtech' in industry_lower or 'farm' in industry_lower:
        return 'Agriculture'

    # Future of Work/Collaboration variations
    if 'future of work' in industry_lower or 'collaboration' in industry_lower or 'productivity' in industry_lower or 'workflow' in industry_lower:
        return 'Productivity'

    # Quantum Computing variations
    if 'quantum' in industry_lower:
        return 'Quantum Computing'

    # Exact mappings for remaining cases
    mappings = {
        # Tech categories
        'web3': 'Web3',
        'web 3': 'Web3',
        'blockchain': 'Web3',
        'blockchains': 'Web3',
        'crypto': 'Web3',
        'cryptocurrency': 'Web3',
        'nft': 'Web3',
        'dao': 'Web3',
        'saas': 'SaaS',
        'software': 'SaaS',
        'b2b saas': 'SaaS',
        'enterprise saas': 'SaaS',
        'enterprise software': 'SaaS',
        'software development': 'Developer Tools',
        'data': 'Data',
        'analytics': 'Data',
        'big data': 'Data',
        'data analytics': 'Data',
        'business intelligence': 'Data',
        'infrastructure': 'Infrastructure',
        'devtools': 'Developer Tools',
        'developer tools': 'Developer Tools',
        'dev tools': 'Developer Tools',
        'api': 'Developer Tools',
        'apis': 'Developer Tools',
        'robotics': 'Robotics',
        'hardware': 'Hardware',
        'iot': 'Hardware',
        'semiconductor': 'Hardware',
        'semiconductors': 'Hardware',
        'climate': 'Climate',
        'climate tech': 'Climate',
        'climatetech': 'Climate',
        'cleantech': 'Climate',
        'greentech': 'Climate',
        'sustainability': 'Climate',
        'energy': 'Energy',
        'marketplace': 'E-commerce',
        'marketplaces': 'E-commerce',
        'consumer': 'Consumer',
        'consumer goods': 'Consumer',
        'cpg': 'Consumer',
        'social': 'Social Media',
        'social media': 'Social Media',
        'social network': 'Social Media',
        'media': 'Media',
        'digital media': 'Media',
        'entertainment': 'Media',
        'gaming': 'Gaming',
        'games': 'Gaming',
        'esports': 'Gaming',
        'e-sports': 'Gaming',
        'sports': 'Sports',
        'food': 'Food & Beverage',
        'food tech': 'Food & Beverage',
        'foodtech': 'Food & Beverage',
        'agriculture': 'Agriculture',
        'agtech': 'Agriculture',
        'construction': 'Manufacturing',
        'manufacturing': 'Manufacturing',
        'automotive': 'Automotive',
        'electric vehicle': 'Automotive',
        'electric vehicles': 'Automotive',
        'ev': 'Automotive',
        'transportation': 'Travel',
        'mobility': 'Travel',
        'travel': 'Travel',
        'hospitality': 'Travel',
        'government': 'Government',
        'govtech': 'Government',
        'nonprofit': 'Nonprofit',
        'non-profit': 'Nonprofit',
        'venture capital': 'VC',
        'vc': 'VC',
        'venture fund': 'VC',
        'investing': 'VC',
        'investments': 'VC',
        'pet': 'Consumer',
        'pets': 'Consumer',
        'pet care': 'Consumer',
        'fashion': 'Consumer',
        'apparel': 'Consumer',
        'beauty': 'Consumer',
        'wellness': 'Healthcare',
        'fitness': 'Healthcare',
        'mental health': 'Mental Health',
        'dental': 'Healthcare',
        'veterinary': 'Healthcare',
        'legal': 'Legal',
        'legaltech': 'Legal',
        'sales': 'SaaS',
        'crm': 'SaaS',
        'database': 'Data',
        'databases': 'Data',
        'deeptech': 'Hardware',
        'deep tech': 'Hardware',
        'quantum computing': 'Quantum Computing',
        'quantum': 'Quantum Computing',
        'design': 'Consumer',
        'community': 'Social Media',
        'communities': 'Social Media',
        'messaging': 'Social Media',
        'dating': 'Social Media',
        'creator economy': 'Media',
        'enterprise': 'SaaS',
        'b2b': 'SaaS',
        'consulting': 'Services',
        'agency': 'Services',
        'services': 'Services',
        'operations': 'SaaS',
        'compliance': 'SaaS',
        'procurement': 'SaaS',
        'accounting': 'SaaS',
        'home services': 'Services',
        'home': 'Consumer',
        'internet': 'SaaS',
        'platform': 'SaaS',
        'tech': 'SaaS',
        'technology': 'SaaS',
        'information technology': 'SaaS',
        'it services': 'SaaS',
        'voice': 'AI',
        'speech': 'AI',
        'vr': 'Hardware',
        'ar': 'Hardware',
        'augmented reality': 'Hardware',
        'virtual reality': 'Hardware',
        'ar/vr': 'Hardware',
        'vr/xr': 'Hardware',
        'mobile': 'SaaS',
        'mobile apps': 'SaaS',
        'apps': 'SaaS',
        'website': 'SaaS',
        'websites': 'SaaS',

        # Filter out stage/funding/company types
        'seed': '',
        'pre-seed': '',
        'pre seed': '',
        'seed stage': '',
        'seed-stage': '',
        'series a': '',
        'series b': '',
        'series c': '',
        'series d': '',
        'series e': '',
        'series f': '',
        'series g': '',
        'series h': '',
        'early-stage': '',
        'early stage': '',
        'late-stage': '',
        'late stage': '',
        'later-stage': '',
        'yc': '',
        'y combinator': '',
        'public': '',
        'acquired': '',
        'pe-backed': '',
        'startup studio': '',
        'accelerator': '',
        'incubator': '',
        'holding company': '',
        'remote': '',
    }

    # Check for exact match (case-insensitive)
    if industry_lower in mappings:
        normalized = mappings[industry_lower]
        return normalized if normalized else None

    # Return original with standardized capitalization
    industry = re.sub(r'\bAI\b', 'AI', industry, flags=re.IGNORECASE)
    industry = re.sub(r'\bSaaS\b', 'SaaS', industry, flags=re.IGNORECASE)
    industry = re.sub(r'\bWeb3\b', 'Web3', industry, flags=re.IGNORECASE)
    industry = re.sub(r'\bB2B\b', 'B2B', industry, flags=re.IGNORECASE)
    industry = re.sub(r'\bB2C\b', 'B2C', industry, flags=re.IGNORECASE)

    return industry

def normalize_location(location):
    """Normalize location names to canonical forms."""
    if not location:
        return location

    location = location.strip()
    location = re.sub(r'\s+', ' ', location)  # Collapse multiple spaces
    location = re.sub(r'^[/\s]+|[/\s]+$', '', location)  # Trim slashes
    location = re.sub(r'\.$', '', location)  # Remove trailing period
    location = re.sub(r'\s*,\s*$', '', location)  # Remove trailing comma

    # Handle multiple locations separated by /
    parts = [p.strip() for p in location.split('/')]
    normalized_parts = []

    for part in parts:
        # Clean up first
        part = part.split(',')[0].strip()  # Remove country/state after comma
        part = re.sub(r'\(.*?\)', '', part).strip()  # Remove parentheses content
        part = re.sub(r'\s+', ' ', part).strip()  # Collapse spaces

        if not part or len(part) > 30:
            continue

        part_lower = part.lower()

        # Pattern-based normalization
        if 'remote' in part_lower or 'anywhere' in part_lower or 'global' in part_lower or 'worldwide' in part_lower or 'various' in part_lower:
            normalized_parts.append('Remote')
            continue
        if 'hybrid' in part_lower:
            # Skip "hybrid" as it's usually combined with a city
            continue
        if 'us states' in part_lower or 'multiple states' in part_lower or 'united states' == part_lower:
            normalized_parts.append('Remote')
            continue
        if 'east coast' in part_lower or 'west coast' in part_lower:
            normalized_parts.append('Remote')
            continue

        # Exact mappings - SF Bay Area cities
        sf_area = ['sf', 'san francisco', 'sf bay area', 'bay area', 'palo alto', 'menlo park',
                   'mountain view', 'san jose', 'san mateo', 'redwood city', 'oakland',
                   'sunnyvale', 'santa clara', 'cupertino', 'fremont', 'burlingame',
                   'san bruno', 'san carlos', 'foster city', 'daly city', 'millbrae',
                   'berkeley', 'emeryville', 'alameda', 'hayward', 'san leandro',
                   'milpitas', 'santa cruz', 'sausalito', 'scotts valley', 'pleasanton',
                   'los altos']

        # NYC area
        nyc_area = ['ny', 'nyc', 'new york', 'new york city', 'brooklyn', 'manhattan',
                    'queens', 'jersey city', 'hoboken', 'williamsburg', 'secaucus']

        # LA area
        la_area = ['la', 'los angeles', 'santa monica', 'culver city', 'pasadena',
                   'venice', 'playa vista', 'long beach', 'glendale', 'burbank',
                   'beverly hills', 'west hollywood', 'el segundo', 'torrance',
                   'sherman oaks', 'costa mesa', 'irvine']

        # Boston area
        boston_area = ['boston', 'cambridge', 'somerville', 'waltham', 'needham',
                      'newton', 'quincy', 'natick', 'woburn']

        # DC area
        dc_area = ['dc', 'washington', 'washington dc', 'mclean', 'bethesda',
                   'arlington', 'alexandria']

        # Denver area
        denver_area = ['denver', 'boulder', 'broomfield', 'arvada', 'englewood']

        # Seattle area
        seattle_area = ['seattle', 'bellevue', 'redmond', 'kirkland', 'everett', 'woodinville']

        # Miami area
        miami_area = ['miami', 'fort lauderdale', 'boca raton', 'aventura', 'west palm beach', 'plantation']

        # Chicago area
        chicago_area = ['chicago', 'evanston', 'oak park']

        # Check major metro areas
        if part_lower in sf_area:
            normalized_parts.append('SF')
        elif part_lower in nyc_area:
            normalized_parts.append('NYC')
        elif part_lower in la_area:
            normalized_parts.append('Los Angeles')
        elif part_lower in boston_area:
            normalized_parts.append('Boston')
        elif part_lower in dc_area:
            normalized_parts.append('DC')
        elif part_lower in denver_area:
            normalized_parts.append('Denver')
        elif part_lower in seattle_area:
            normalized_parts.append('Seattle')
        elif part_lower in miami_area:
            normalized_parts.append('Miami')
        elif part_lower in chicago_area:
            normalized_parts.append('Chicago')
        # Other major cities
        elif part_lower in ['austin']:
            normalized_parts.append('Austin')
        elif part_lower in ['atlanta', 'marietta', 'norcross']:
            normalized_parts.append('Atlanta')
        elif part_lower in ['philadelphia', 'philly']:
            normalized_parts.append('Philadelphia')
        elif part_lower in ['portland']:
            normalized_parts.append('Portland')
        elif part_lower in ['phoenix', 'scottsdale', 'tempe']:
            normalized_parts.append('Phoenix')
        elif part_lower in ['san diego', 'carlsbad']:
            normalized_parts.append('San Diego')
        elif part_lower in ['dallas', 'plano', 'coppell']:
            normalized_parts.append('Dallas')
        elif part_lower in ['houston']:
            normalized_parts.append('Houston')
        elif part_lower in ['nashville']:
            normalized_parts.append('Nashville')
        elif part_lower in ['salt lake city', 'lehi', 'provo', 'south jordan', 'lindon']:
            normalized_parts.append('Salt Lake City')
        elif part_lower in ['raleigh', 'durham', 'morrisville']:
            normalized_parts.append('Raleigh')
        elif part_lower in ['detroit', 'novi', 'troy']:
            normalized_parts.append('Detroit')
        elif part_lower in ['minneapolis']:
            normalized_parts.append('Minneapolis')
        elif part_lower in ['pittsburgh']:
            normalized_parts.append('Pittsburgh')
        elif part_lower in ['columbus']:
            normalized_parts.append('Columbus')
        elif part_lower in ['charlotte']:
            normalized_parts.append('Charlotte')
        elif part_lower in ['baltimore']:
            normalized_parts.append('Baltimore')
        elif part_lower in ['milwaukee']:
            normalized_parts.append('Milwaukee')
        elif part_lower in ['st. louis', 'st louis']:
            normalized_parts.append('St. Louis')
        elif part_lower in ['richmond']:
            normalized_parts.append('Richmond')
        elif part_lower in ['omaha']:
            normalized_parts.append('Omaha')
        elif part_lower in ['reno', 'sparks']:
            normalized_parts.append('Reno')
        # International
        elif part_lower in ['toronto']:
            normalized_parts.append('Toronto')
        elif part_lower in ['montreal']:
            normalized_parts.append('Montreal')
        elif part_lower in ['vancouver']:
            normalized_parts.append('Vancouver')
        elif part_lower in ['calgary']:
            normalized_parts.append('Calgary')
        elif part_lower in ['ottawa']:
            normalized_parts.append('Ottawa')
        elif part_lower in ['canada', 'canada)']:
            normalized_parts.append('Canada')
        elif part_lower in ['london']:
            normalized_parts.append('London')
        elif part_lower in ['paris']:
            normalized_parts.append('Paris')
        elif part_lower in ['berlin']:
            normalized_parts.append('Berlin')
        elif part_lower in ['singapore']:
            normalized_parts.append('Singapore')
        elif part_lower in ['stockholm']:
            normalized_parts.append('Stockholm')
        elif part_lower in ['dublin', 'ireland']:
            normalized_parts.append('Dublin')
        elif part_lower in ['uk', 'united kingdom)']:
            normalized_parts.append('UK')
        elif part_lower in ['australia']:
            normalized_parts.append('Australia')
        else:
            # Unknown location - skip it to keep the list clean
            continue

    # Remove duplicates while preserving order
    seen = set()
    unique_parts = []
    for part in normalized_parts:
        if part and part not in seen:
            seen.add(part)
            unique_parts.append(part)

    return ' / '.join(unique_parts) if unique_parts else 'Remote'

def clean_companies_data(input_file, output_file):
    """Clean the companies data and save to output file."""

    print(f"Loading data from {input_file}...")
    with open(input_file, 'r') as f:
        data = json.load(f)

    print(f"Original: {len(data['companies'])} companies")

    # Track what we're removing
    removed_invalid = 0
    removed_duplicates = 0
    cleaned_companies = []
    seen_companies = {}  # Track duplicates by (company, edition)

    for company in data['companies']:
        # Validate fields
        if not is_valid_company(company.get('company', '')):
            removed_invalid += 1
            continue

        if not is_valid_industry(company.get('industry', '')):
            removed_invalid += 1
            continue

        if not is_valid_location(company.get('location', '')):
            removed_invalid += 1
            continue

        # Normalize fields
        company['industry'] = normalize_industry(company.get('industry', ''))
        company['location'] = normalize_location(company.get('location', ''))

        # Skip if industry was normalized to empty (was actually a stage/etc)
        if not company['industry']:
            removed_invalid += 1
            continue

        # Check for duplicates (same company in same edition)
        key = (company['company'].lower(), company.get('latest_edition'))
        if key in seen_companies:
            removed_duplicates += 1
            continue

        seen_companies[key] = True
        cleaned_companies.append(company)

    # Update data
    data['companies'] = cleaned_companies
    data['total_companies'] = len(cleaned_companies)

    # Calculate real unique counts
    unique_industries = len(set(c['industry'] for c in cleaned_companies if c.get('industry')))
    unique_locations = len(set(
        loc.strip()
        for c in cleaned_companies
        for loc in c.get('location', '').split('/')
        if loc.strip() and len(loc.strip()) < 30
    ))

    print(f"\nCleaning results:")
    print(f"  Removed {removed_invalid} invalid entries")
    print(f"  Removed {removed_duplicates} duplicates")
    print(f"  Kept {len(cleaned_companies)} companies")
    print(f"\nUnique stats:")
    print(f"  {unique_industries} unique industries")
    print(f"  {unique_locations} unique locations")

    # Show top industries and locations
    industry_counts = defaultdict(int)
    location_counts = defaultdict(int)

    for c in cleaned_companies:
        if c.get('industry'):
            industry_counts[c['industry']] += 1
        if c.get('location'):
            for loc in c['location'].split('/'):
                loc = loc.strip().split(',')[0].strip()
                if loc and len(loc) < 30:
                    location_counts[loc] += 1

    print(f"\nTop 15 industries:")
    for industry, count in sorted(industry_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {industry}: {count}")

    print(f"\nTop 15 locations:")
    for location, count in sorted(location_counts.items(), key=lambda x: -x[1])[:15]:
        print(f"  {location}: {count}")

    # Save cleaned data
    print(f"\nSaving cleaned data to {output_file}...")
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)

    print("Done!")

if __name__ == '__main__':
    input_file = Path('data/companies.json')
    output_file = Path('data/companies.json.cleaned')

    # Create backup
    backup_file = Path('data/companies.json.backup')
    print(f"Creating backup at {backup_file}...")
    with open(input_file) as f_in, open(backup_file, 'w') as f_out:
        f_out.write(f_in.read())

    clean_companies_data(input_file, output_file)

    print(f"\nBackup saved to: {backup_file}")
    print(f"Cleaned data saved to: {output_file}")
    print(f"\nTo use the cleaned data, run:")
    print(f"  mv data/companies.json.cleaned data/companies.json")
