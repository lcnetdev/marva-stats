#!/usr/bin/env python3
"""
Extract published records from BSON files within a date range.

Usage: python3 extract_published.py <start_date> <end_date>
Date format: yyyy-mm-dd

Outputs:
- output/records_by_day.json: count of published records per day
- output/records_by_user_month.json: count of published records by user by month (normalized by userid)
- xml_data/*.xml: sample XML files containing "n 2025" or "n2025"
"""

import sys
import json
import re
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import bson
from bson.codec_options import CodecOptions


def extract_userid(user_str):
    """Extract the parenthetical userid from a user string.

    Examples:
        "Pamela Perry (fg10)" -> "fg10"
        "pper (fg10)" -> "fg10"
        "unknown" -> "unknown"
    """
    match = re.search(r'\(([^)]+)\)', user_str)
    if match:
        return match.group(1)
    return user_str


def extract_display_name(user_str):
    """Extract the display name (part before parentheses) from a user string.

    Examples:
        "Pamela Perry (fg10)" -> "Pamela Perry"
        "pper (fg10)" -> "pper"
        "unknown" -> "unknown"
    """
    match = re.match(r'^([^(]+)\s*\(', user_str)
    if match:
        return match.group(1).strip()
    return user_str


def extract_nar_ids(content):
    """Extract NAR IDs (n20255...) from XML content.

    Looks for patterns like http://id.loc.gov/rwo/agents/n20255014191
    The pattern is n20255 (year 2025 + 5) followed by more digits.
    Returns a set of unique NAR IDs found.
    """
    if not content:
        return set()
    # Match n20255 followed by digits in URI context
    pattern = r'id\.loc\.gov/[^"]*/(n20255\d+)'
    matches = re.findall(pattern, content)
    return set(matches)


def bson_file_iter(filepath):
    """Iterate over BSON documents in a file with manual size handling."""
    with open(filepath, 'rb') as f:
        while True:
            # Read the document size (first 4 bytes, little-endian int32)
            size_data = f.read(4)
            if not size_data or len(size_data) < 4:
                break

            doc_size = int.from_bytes(size_data, 'little', signed=True)

            # Skip invalid/corrupted documents
            if doc_size <= 4 or doc_size > 100 * 1024 * 1024:  # 100MB max
                print(f"  Warning: Invalid document size ({doc_size}), attempting recovery...")
                # Try to find next valid document by scanning
                continue

            # Read the rest of the document
            remaining = f.read(doc_size - 4)
            if len(remaining) < doc_size - 4:
                print(f"  Warning: Unexpected EOF, got {len(remaining)} bytes, expected {doc_size - 4}")
                break

            doc_data = size_data + remaining
            try:
                yield bson.decode(doc_data)
            except bson.errors.InvalidBSON as e:
                print(f"  Warning: Failed to decode document ({doc_size} bytes): {e}")
                continue
            except Exception as e:
                print(f"  Warning: Unexpected error decoding document: {e}")
                continue


def parse_date(date_str):
    """Parse ISO date string to date object."""
    # Handle format like '2025-11-13T15:24:31.862Z'
    return datetime.fromisoformat(date_str.replace('Z', '+00:00')).date()


def get_last_version_content(versions):
    """Get the content from the last (most recent) version."""
    if not versions:
        return None
    # Versions appear to be ordered, last one is most recent
    return versions[-1].get('content')


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 extract_published.py <start_date> <end_date>")
        print("Date format: yyyy-mm-dd")
        sys.exit(1)

    start_date = datetime.strptime(sys.argv[1], '%Y-%m-%d').date()
    end_date = datetime.strptime(sys.argv[2], '%Y-%m-%d').date()

    print(f"Filtering records from {start_date} to {end_date}")

    bson_dir = Path(__file__).parent / "bson_data"
    bson_files = sorted(bson_dir.glob("*.bson"))

    if not bson_files:
        print("No BSON files found in bson_data/")
        sys.exit(1)

    print(f"Found {len(bson_files)} BSON file(s)")

    # Track seen URIs to avoid duplicates
    seen_uris = set()
    duplicate_count = 0

    # Stats aggregation
    records_by_day = defaultdict(int)
    records_by_month = defaultdict(int)  # Total records by month (all users)
    records_by_user_month = defaultdict(lambda: defaultdict(int))

    # Track display names for each userid
    user_display_names = defaultdict(set)

    # For preview
    sample_contents = []
    total_matched = 0

    # XML samples containing "n20255" (NAR IDs)
    xml_samples_2025 = []
    max_xml_samples = 10

    # NAR tracking (n2025 IDs)
    seen_nar_ids = set()
    nars_by_day = defaultdict(int)
    nars_by_month = defaultdict(int)
    nars_by_user_month = defaultdict(lambda: defaultdict(int))

    for filepath in bson_files:
        print(f"\nProcessing: {filepath.name}")
        file_matched = 0
        file_duplicates = 0

        for doc in bson_file_iter(filepath):
            uri = doc.get('uri')
            index = doc.get('index', {})
            status = index.get('status')
            modified_str = doc.get('modified')

            # Skip if not published
            if status != 'published':
                continue

            # Skip if no modified date
            if not modified_str:
                continue

            # Parse modified date
            try:
                modified_date = parse_date(modified_str)
            except (ValueError, AttributeError):
                continue

            # Check date range
            if not (start_date <= modified_date <= end_date):
                continue

            # Check for duplicates
            if uri in seen_uris:
                file_duplicates += 1
                duplicate_count += 1
                continue

            seen_uris.add(uri)
            file_matched += 1
            total_matched += 1

            # Get user and normalize by userid
            user_raw = index.get('user', 'unknown')
            user = extract_userid(user_raw)
            display_name = extract_display_name(user_raw)
            user_display_names[user].add(display_name)

            # Aggregate by day
            day_key = modified_date.isoformat()
            records_by_day[day_key] += 1

            # Aggregate by month (all users)
            month_key = modified_date.strftime('%Y-%m')
            records_by_month[month_key] += 1

            # Aggregate by user and month
            records_by_user_month[user][month_key] += 1

            # Get content for checks
            content = get_last_version_content(doc.get('versions', []))

            # Collect sample content for preview
            if len(sample_contents) < 3:
                if content:
                    sample_contents.append({
                        'uri': uri,
                        'title': index.get('title', 'N/A'),
                        'user': user,
                        'modified': modified_str,
                        'content_preview': content[:2000] if len(content) > 2000 else content
                    })

            # Check for "n20255" in XML content (NAR IDs)
            if content and len(xml_samples_2025) < max_xml_samples:
                if 'n20255' in content:
                    xml_samples_2025.append({
                        'uri': uri,
                        'title': index.get('title', 'N/A'),
                        'user': user,
                        'modified': modified_str,
                        'content': content
                    })

            # Extract and track NAR IDs (n2025...)
            if content:
                nar_ids = extract_nar_ids(content)
                for nar_id in nar_ids:
                    if nar_id not in seen_nar_ids:
                        seen_nar_ids.add(nar_id)
                        nars_by_day[day_key] += 1
                        nars_by_month[month_key] += 1
                        nars_by_user_month[user][month_key] += 1

        print(f"  Matched: {file_matched}, Duplicates skipped: {file_duplicates}")

    print(f"\n{'='*60}")
    print(f"Total matched records: {total_matched}")
    print(f"Total duplicates skipped: {duplicate_count}")
    print(f"Total unique NAR IDs (n2025...): {len(seen_nar_ids)}")
    print(f"{'='*60}")

    # Print sample XML content
    print("\n--- Sample XML Content ---")
    for i, sample in enumerate(sample_contents, 1):
        print(f"\n[{i}] URI: {sample['uri']}")
        print(f"    Title: {sample['title']}")
        print(f"    User: {sample['user']}")
        print(f"    Modified: {sample['modified']}")
        print(f"    Content preview:\n{sample['content_preview'][:1000]}...")

    # Output JSON files
    output_dir = Path(__file__).parent / "output"
    output_dir.mkdir(exist_ok=True)

    # Records by day
    by_day_file = output_dir / "records_by_day.json"
    with open(by_day_file, 'w') as f:
        json.dump(dict(sorted(records_by_day.items())), f, indent=2)
    print(f"\nWrote: {by_day_file}")

    # Records by month (all users)
    by_month_file = output_dir / "records_by_month.json"
    with open(by_month_file, 'w') as f:
        json.dump(dict(sorted(records_by_month.items())), f, indent=2)
    print(f"Wrote: {by_month_file}")

    # Records by user by month
    by_user_month_file = output_dir / "records_by_user_month.json"
    # Convert defaultdict to regular dict for JSON, adding display names
    user_month_data = {}
    for user, months in records_by_user_month.items():
        names = sorted(user_display_names.get(user, set()))
        if names and names != [user]:
            key = f"{user} ({', '.join(names)})"
        else:
            key = user
        user_month_data[key] = dict(months)
    with open(by_user_month_file, 'w') as f:
        json.dump(user_month_data, f, indent=2)
    print(f"Wrote: {by_user_month_file}")

    # NAR stats by day
    nars_by_day_file = output_dir / "nars_by_day.json"
    with open(nars_by_day_file, 'w') as f:
        json.dump(dict(sorted(nars_by_day.items())), f, indent=2)
    print(f"Wrote: {nars_by_day_file}")

    # NAR stats by month
    nars_by_month_file = output_dir / "nars_by_month.json"
    with open(nars_by_month_file, 'w') as f:
        json.dump(dict(sorted(nars_by_month.items())), f, indent=2)
    print(f"Wrote: {nars_by_month_file}")

    # NAR stats by user by month
    nars_by_user_month_file = output_dir / "nars_by_user_month.json"
    # Add display names to NAR user data too
    nar_user_month_data = {}
    for user, months in nars_by_user_month.items():
        names = sorted(user_display_names.get(user, set()))
        if names and names != [user]:
            key = f"{user} ({', '.join(names)})"
        else:
            key = user
        nar_user_month_data[key] = dict(months)
    with open(nars_by_user_month_file, 'w') as f:
        json.dump(nar_user_month_data, f, indent=2)
    print(f"Wrote: {nars_by_user_month_file}")

    # All NAR IDs as full URLs
    nar_urls = [f"http://id.loc.gov/authorities/names/{nar_id}" for nar_id in sorted(seen_nar_ids)]
    nars_list_file = output_dir / "nars_list.json"
    with open(nars_list_file, 'w') as f:
        json.dump(nar_urls, f, indent=2)
    print(f"Wrote: {nars_list_file} ({len(nar_urls)} NAR URLs)")

    # Output XML samples containing "n 2025" or "n2025"
    xml_dir = Path(__file__).parent / "xml_data"
    xml_dir.mkdir(exist_ok=True)

    print(f"\nFound {len(xml_samples_2025)} XML files containing 'n20255'")
    for i, sample in enumerate(xml_samples_2025):
        # Create safe filename from URI with date
        safe_name = sample['uri'].replace(':', '_').replace('/', '_')
        # Extract date from modified timestamp (e.g., "2025-11-13T15:24:31.862Z" -> "2025-11-13")
        modified_date = sample['modified'][:10] if sample['modified'] else 'unknown'
        xml_file = xml_dir / f"{modified_date}_{safe_name}.xml"
        with open(xml_file, 'w') as f:
            f.write(sample['content'])
        print(f"  Wrote: {xml_file.name} ({sample['title'][:50]}...)")


if __name__ == "__main__":
    main()
