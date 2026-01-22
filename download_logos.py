import requests
import os
from pathlib import Path

# Team ID to school name mapping (ESPN NCAA team IDs)
TEAM_ID_MAP = {
    '120': 'New Mexico',
    '127': 'Kansas',
    '130': 'Kansas State',
    '145': 'Louisville',
    '150': 'Maryland',
    '153': 'Michigan',
    '158': 'Michigan State',
    '194': 'Notre Dame',
    '197': 'Ohio State',
    '201': 'Oklahoma',
    '2294': 'Georgetown',
    '238': 'Stanford',
    '239': 'Syracuse',
    '257': 'Texas',
    '2579': 'Texas Tech',
    '26': 'Baylor',
    '2628': 'TCU',
    '2633': 'UNLV',  # _ncw in the URL
    '264': 'UCLA',
    '2641': 'Utah',
    '277': 'USC',
    '30': 'Duke',
    '41': 'Georgia',
    '66': 'Indiana',
    '87': 'Kentucky',
    '96': 'Tennessee',
    '97': 'Texas A&M',
    '99': 'Virginia'
}

# List of URLs
urls = [
    "https://a.espncdn.com/i/teamlogos/ncaa/500/120.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/127.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/130.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/145.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/150.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/153.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/158.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/194.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/197.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/201.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/2294.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/238.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/239.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/257.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/2579.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/26.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/2628.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/2633_ncw.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/264.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/2641.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/277.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/30.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/41.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/66.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/87.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/96.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/96.png",  # Duplicate
    "https://a.espncdn.com/i/teamlogos/ncaa/500/97.png",
    "https://a.espncdn.com/i/teamlogos/ncaa/500/99.png"
]

# Create assets directory if it doesn't exist
assets_dir = Path('/home/user/wbb_polls_pbp_2026/data/assets')
assets_dir.mkdir(parents=True, exist_ok=True)

downloaded_schools = []
downloaded_files = set()  # Track downloaded files to avoid duplicates

for url in urls:
    try:
        # Extract team ID from URL
        filename = url.split('/')[-1]  # e.g., "120.png" or "2633_ncw.png"
        team_id = filename.replace('.png', '').replace('_ncw', '')

        # Skip if we already downloaded this file
        if filename in downloaded_files:
            print(f"Skipping duplicate: {filename}")
            continue

        # Get school name if available
        school_name = TEAM_ID_MAP.get(team_id)

        # Determine output filename
        if school_name:
            output_filename = f"{school_name}.png"
        else:
            output_filename = filename

        output_path = assets_dir / output_filename

        # Download the image
        print(f"Downloading {url}...")
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        # Save the image
        with open(output_path, 'wb') as f:
            f.write(response.content)

        print(f"✓ Saved as: {output_filename}")

        if school_name:
            downloaded_schools.append(school_name)

        downloaded_files.add(filename)

    except Exception as e:
        print(f"✗ Error downloading {url}: {e}")

print(f"\n{'='*50}")
print(f"Successfully downloaded {len(downloaded_schools)} logos")
print(f"{'='*50}")

# Print schools in alphabetical order
if downloaded_schools:
    print("\nSchools added to assets folder:")
    for school in sorted(downloaded_schools):
        print(f"  - {school}")
