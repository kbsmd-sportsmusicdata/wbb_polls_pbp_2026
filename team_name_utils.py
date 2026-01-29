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
        canonical_names = set()  # Track standardized names to avoid reverse mappings

        # Process rows with source_name -> canonical_name format
        if 'source_name' in df.columns and 'canonical_name' in df.columns:
            # Filter out rows where source_name looks like a header
            valid_rows = df[
                (df['source_name'].notna()) &
                (df['canonical_name'].notna()) &
                (df['source_name'] != 'standardized_team_name')  # Skip header rows
            ]

            for _, row in valid_rows.iterrows():
                source = str(row['source_name']).strip()
                canonical = str(row['canonical_name']).strip()
                if source and canonical:
                    mappings[source] = canonical
                    canonical_names.add(canonical)  # Track canonical names

        # Process rows with standardized_team_name,possible_team_name format
        if 'standardized_team_name' in df.columns and 'possible_team_name' in df.columns:
            # Column order: standardized_team_name, possible_team_name
            # Mapping direction: possible_team_name -> standardized_team_name
            valid_rows = df[
                (df['standardized_team_name'].notna()) &
                (df['possible_team_name'].notna()) &
                (df['standardized_team_name'] != 'source_name')  # Skip header rows
            ]

            for _, row in valid_rows.iterrows():
                standardized = str(row['standardized_team_name']).strip()
                possible = str(row['possible_team_name']).strip()
                # Map possible name (variant) -> standardized name (canonical)
                if possible and standardized and possible != '':
                    # Don't create reverse mappings (standardized -> something else)
                    if standardized not in mappings:  # Don't overwrite if standardized name is already a source
                        canonical_names.add(standardized)
                    mappings[possible] = standardized

        # Remove any reverse mappings where a canonical name maps to something else
        # This handles cases where CSV has both "UConn -> Connecticut" and "Connecticut -> UConn"
        cleaned_mappings = {
            source: target for source, target in mappings.items()
            if source not in canonical_names or source == target  # Keep identity mappings
        }

        if not cleaned_mappings:
            logger.warning(f"No valid mappings found in {STANDARDIZATION_FILE}. Using fallback mappings.")
            return FALLBACK_MAPPINGS.copy()

        logger.debug(f"Loaded {len(cleaned_mappings)} team name mappings from {STANDARDIZATION_FILE}")
        return cleaned_mappings

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
