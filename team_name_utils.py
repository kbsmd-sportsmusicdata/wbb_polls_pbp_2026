"""
Centralized team name standardization utilities.

This module provides a single source of truth for team name mappings across all scripts.
All scripts should import and use these functions to ensure consistency.
"""

import logging
from pathlib import Path
from typing import Dict
import pandas as pd

# Configure logging
logger = logging.getLogger(__name__)

# Path to standardization mapping file
STANDARDIZATION_FILE = Path(__file__).parent / "data" / "raw" / "team_name_standardization.csv"

# Fallback mappings if CSV file is unavailable or malformed
FALLBACK_MAPPINGS = {
    'Connecticut': 'UConn',
    'Louisiana State': 'LSU',
    'Southern California': 'USC',
    'Mississippi': 'Ole Miss',
    'North Carolina': 'UNC',
}


def load_team_name_mappings() -> Dict[str, str]:
    """
    Loads team name standardization mappings from CSV file.

    Handles mixed format CSV with both:
    - source_name,canonical_name (variations -> standard)
    - standardized_team_name,possible_team_name (standard -> variations, inverted)

    Returns:
        Dictionary mapping all variations -> canonical_name
    """
    if not STANDARDIZATION_FILE.exists():
        logger.warning(f"Standardization file not found: {STANDARDIZATION_FILE}. Using fallback mappings.")
        return FALLBACK_MAPPINGS.copy()

    try:
        df = pd.read_csv(STANDARDIZATION_FILE)
        mappings = {}
        used_names = set()  # Track all names we've used (as source OR target)

        # Process rows with source_name -> canonical_name format
        if 'source_name' in df.columns and 'canonical_name' in df.columns:
            # Filter out rows where source_name looks like a header or is the embedded header row
            valid_rows = df[
                (df['source_name'].notna()) &
                (df['canonical_name'].notna()) &
                (df['source_name'] != 'standardized_team_name') &  # Skip embedded header row
                (df['canonical_name'] != 'possible_team_name')     # Skip embedded header row
            ]

            for _, row in valid_rows.iterrows():
                source = str(row['source_name']).strip()
                canonical = str(row['canonical_name']).strip()

                if source and canonical and source != canonical:
                    # Skip if either name has already been used (prevents duplicate/reverse mappings)
                    if source in used_names or canonical in used_names:
                        logger.debug(f"Skipping duplicate mapping: '{source}' → '{canonical}'")
                        continue

                    # Determine if we need to swap the direction
                    # Swap if: source has keywords AND target has abbreviated form (e.g., "St." instead of "State")
                    source_has_keywords = ('State' in source or 'University' in source or 'College' in source)
                    target_is_abbreviated = ('St.' in canonical or 'Univ' in canonical)

                    if (len(source) > len(canonical) and source_has_keywords and target_is_abbreviated):
                        # Swap: abbreviated form -> full form (e.g., "Michigan St." -> "Michigan State")
                        mappings[canonical] = source
                        used_names.add(canonical)
                        used_names.add(source)
                    else:
                        # Normal direction: variant -> canonical (e.g., "Louisiana State" -> "LSU")
                        mappings[source] = canonical
                        used_names.add(source)
                        used_names.add(canonical)

        # No need for additional cleaning - we already filtered duplicates and identity mappings

        if not mappings:
            logger.warning(f"No valid mappings found in {STANDARDIZATION_FILE}. Using fallback mappings.")
            return FALLBACK_MAPPINGS.copy()

        logger.debug(f"Loaded {len(mappings)} team name mappings from {STANDARDIZATION_FILE}")
        return mappings

    except Exception as e:
        logger.error(f"Error parsing {STANDARDIZATION_FILE}: {e}. Using fallback mappings.")
        return FALLBACK_MAPPINGS.copy()


def standardize_team_names(df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Standardizes team names in specified DataFrame columns.

    Args:
        df: DataFrame containing team name columns
        columns: List of column names to standardize

    Returns:
        DataFrame with standardized team names
    """
    mappings = load_team_name_mappings()

    for col in columns:
        if col in df.columns:
            df[col] = df[col].replace(mappings)

    return df


# Pre-load mappings as module constant for backward compatibility
TEAM_NAME_MAPPINGS = load_team_name_mappings()
