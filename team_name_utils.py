"""
Centralized team name standardization utilities.

This module provides a single source of truth for team name mappings across all scripts.
All scripts should import and use these functions to ensure consistency.
"""

from pathlib import Path
from typing import Dict
import pandas as pd

# Path to standardization mapping file
STANDARDIZATION_FILE = Path(__file__).parent / "data" / "raw" / "team_name_standardization.csv"


def load_team_name_mappings() -> Dict[str, str]:
    """
    Loads team name standardization mappings from CSV file.

    Returns:
        Dictionary mapping source_name -> canonical_name
    """
    try:
        if STANDARDIZATION_FILE.exists():
            df = pd.read_csv(STANDARDIZATION_FILE)
            return dict(zip(df['source_name'], df['canonical_name']))
        else:
            # Fallback to hardcoded mappings if file not found
            return {
                'Connecticut': 'UConn',
                'Louisiana State': 'LSU',
                'Southern California': 'USC',
                'Mississippi': 'Ole Miss',
                'North Carolina': 'UNC',
            }
    except Exception:
        # Fallback to hardcoded mappings on any error
        return {
            'Connecticut': 'UConn',
            'Louisiana State': 'LSU',
            'Southern California': 'USC',
            'Mississippi': 'Ole Miss',
            'North Carolina': 'UNC',
        }


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
