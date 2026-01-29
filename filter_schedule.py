#!/usr/bin/env python3
"""
Filters WBB schedule data to include only games involving Top 50 teams (completed games only).

This script creates a Tableau-ready dataset by:
1. Identifying Top 50 teams from NET rankings + current polls
2. Filtering schedule to only completed games with Top 50 teams
3. Transforming to long format (one row per team perspective)
4. Keeping only user-specified columns for analysis

Output:
    - data/wbb_schedule/schedule_filtered.csv (Tableau-ready dataset)

Usage:
    python filter_schedule.py
"""

import json
import logging
from pathlib import Path
from typing import Set

import ast
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
SCHEDULE_PATH = Path("data/raw/schedule_2026.csv")
POLLS_PATH = Path("data/polls_analytics.csv")
NET_RANKINGS_PATH = Path("data/net_rankings/net_rankings_master.csv")
OUTPUT_PATH = Path("data/wbb_schedule/schedule_filtered.csv")
TOP_N_TEAMS = 50  # Include Top 50 teams


def get_top_50_teams() -> Set[str]:
    """
    Identifies Top 50 teams from NET rankings and current polls.

    Returns:
        Set of team names to include in filtered schedule
    """
    top_teams = set()

    # Source 1: NET Rankings (Top 50)
    if NET_RANKINGS_PATH.exists():
        try:
            net_df = pd.read_csv(NET_RANKINGS_PATH)

            # Get latest NET rankings
            if 'run_date' in net_df.columns:
                latest_date = net_df['run_date'].max()
                net_df = net_df[net_df['run_date'] == latest_date]

            # Get Top 50 from NET rankings
            if 'net_rank' in net_df.columns and 'team' in net_df.columns:
                # Convert rank to numeric (handle any strings)
                net_df['net_rank'] = pd.to_numeric(net_df['net_rank'], errors='coerce')
                top_50_net = net_df.nsmallest(TOP_N_TEAMS, 'net_rank')['team'].tolist()
                top_teams.update(top_50_net)
                logger.info(f"Added {len(top_50_net)} teams from NET rankings (Top 50)")
            else:
                logger.warning(f"NET rankings file missing required columns: {net_df.columns.tolist()}")

        except Exception as e:
            logger.warning(f"Could not load NET rankings: {e}")
    else:
        logger.warning(f"NET rankings file not found at {NET_RANKINGS_PATH}")

    # Source 2: Current AP/Coaches Polls (all ranked teams)
    if POLLS_PATH.exists():
        try:
            polls_df = pd.read_csv(POLLS_PATH)

            # Get teams that are currently ranked (rank_numeric <= 25)
            if 'rank_numeric' in polls_df.columns and 'team' in polls_df.columns:
                # Get latest poll week
                if 'week_number' in polls_df.columns:
                    latest_week = polls_df['week_number'].max()
                    polls_df = polls_df[polls_df['week_number'] == latest_week]

                ranked_teams = polls_df[polls_df['rank_numeric'] <= 25]['team'].unique().tolist()
                top_teams.update(ranked_teams)
                logger.info(f"Added {len(ranked_teams)} teams from current polls (ranked 1-25)")
            else:
                logger.warning(f"Polls file missing required columns: {polls_df.columns.tolist()}")

        except Exception as e:
            logger.warning(f"Could not load polls data: {e}")
    else:
        logger.warning(f"Polls file not found at {POLLS_PATH}")

    logger.info(f"Total unique teams in Top 50 list: {len(top_teams)}")

    if not top_teams:
        logger.error("No teams identified for filtering - check data sources")

    return top_teams


def parse_records(records_str: str) -> tuple[str, str]:
    """
    Parses the records string to extract overall and conference records.

    Args:
        records_str: String representation like "[{'name': 'overall', 'summary': '15-3'}, ...]"

    Returns:
        Tuple of (overall_record, conf_record)
    """

    try:
        if pd.isna(records_str) or not records_str:
            return '', ''

        records = ast.literal_eval(records_str)

        overall = ''
        conf = ''

        for record in records:
            if record.get('name') == 'overall':
                overall = record.get('summary', '')
            elif record.get('name') == 'conf' or record.get('type') == 'vsconf':
                conf = record.get('summary', '')

        return overall, conf

    except (ValueError, SyntaxError) as e:
        logger.debug(f"Could not parse records: {records_str[:100]} - {e}")
        return '', ''


def filter_schedule(top_teams: Set[str]) -> pd.DataFrame:
    """
    Filters schedule to completed games involving Top 50 teams.

    Args:
        top_teams: Set of team names to include

    Returns:
        Filtered DataFrame with selected columns
    """
    logger.info(f"Loading schedule from {SCHEDULE_PATH}")
    df = pd.read_csv(SCHEDULE_PATH)
    logger.info(f"Loaded {len(df)} total games")

    # Filter 1: Only completed games
    if 'status_type_completed' in df.columns:
        df = df[df['status_type_completed'] == True].copy()
        logger.info(f"After filtering to completed games: {len(df)} games")
    else:
        logger.warning("Column 'status_type_completed' not found - cannot filter by completion status")

    # Filter 2: Only games involving Top 50 teams
    if top_teams:
        # Check if either home or away team is in Top 50
        # Use home_location/away_location to match team names from polls
        home_in_top50 = df['home_location'].isin(top_teams)
        away_in_top50 = df['away_location'].isin(top_teams)
        df = df[home_in_top50 | away_in_top50].copy()
        logger.info(f"After filtering to Top 50 teams: {len(df)} games")

    if df.empty:
        logger.warning("No games remaining after filtering")
        return pd.DataFrame()

    # Parse records for both teams
    logger.info("Parsing team records...")
    df[['home_overall_record', 'home_conf_record']] = df['home_records'].apply(
        lambda x: pd.Series(parse_records(x))
    )
    df[['away_overall_record', 'away_conf_record']] = df['away_records'].apply(
        lambda x: pd.Series(parse_records(x))
    )

    # Create long format: one row per team perspective
    logger.info("Transforming to long format (one row per team)...")

    # Home team perspective
    home_df = df.copy()
    home_df['Team'] = home_df['home_location']  # Use location to match polls
    home_df['Opponent'] = home_df['away_location']
    home_df['Location'] = home_df['neutral_site'].apply(lambda x: 'Neutral' if x else 'Home')
    home_df['Team Score'] = home_df['home_score']
    home_df['Opponent Score'] = home_df['away_score']
    home_df['Game Result'] = home_df['home_winner'].apply(lambda x: 'W' if x else 'L')
    home_df['Team Logo'] = home_df['home_logo']
    home_df['Opponent Logo'] = home_df['away_logo']
    home_df['Team Rank'] = home_df['home_current_rank']
    home_df['Opponent Rank'] = home_df['away_current_rank']
    home_df['Team Conf ID'] = home_df['home_conference_id']
    home_df['Opponent Conf ID'] = home_df['away_conference_id']
    home_df['Overall Record'] = home_df['home_overall_record']
    home_df['Conf Record'] = home_df['home_conf_record']

    # Away team perspective
    away_df = df.copy()
    away_df['Team'] = away_df['away_location']  # Use location to match polls
    away_df['Opponent'] = away_df['home_location']
    away_df['Location'] = away_df['neutral_site'].apply(lambda x: 'Neutral' if x else 'Away')
    away_df['Team Score'] = away_df['away_score']
    away_df['Opponent Score'] = away_df['home_score']
    away_df['Game Result'] = away_df['away_winner'].apply(lambda x: 'W' if x else 'L')
    away_df['Team Logo'] = away_df['away_logo']
    away_df['Opponent Logo'] = away_df['home_logo']
    away_df['Team Rank'] = away_df['away_current_rank']
    away_df['Opponent Rank'] = away_df['home_current_rank']
    away_df['Team Conf ID'] = away_df['away_conference_id']
    away_df['Opponent Conf ID'] = away_df['home_conference_id']
    away_df['Overall Record'] = away_df['away_overall_record']
    away_df['Conf Record'] = away_df['away_conf_record']

    # Combine both perspectives
    combined_df = pd.concat([home_df, away_df], ignore_index=True)

    # Rename conference_competition to is_conf_game for clarity
    combined_df = combined_df.rename(columns={'conference_competition': 'is_conf_game'})

    # Define the final columns for the output
    final_columns = [
        'id', 'date', 'Team', 'Opponent', 'Location', 'Game Result',
        'Team Score', 'Opponent Score', 'Team Logo', 'Opponent Logo',
        'Team Rank', 'Opponent Rank', 'Team Conf ID', 'Opponent Conf ID',
        'Overall Record', 'Conf Record', 'is_conf_game',
        'type_id', 'notes_headline', 'status_type_name',
        'home_conference_id', 'home_winner', 'home_current_rank', 'home_records',
        'away_conference_id', 'away_winner', 'away_current_rank', 'away_records',
        'groups_id', 'groups_short_name'
    ]

    # Ensure all requested columns exist (fill missing with None)
    for col in final_columns:
        if col not in combined_df.columns:
            combined_df[col] = None

    # Select final columns in the specified order
    result_df = combined_df[final_columns].copy()

    # Filter to only rows where Team is in Top 50 (we want their perspective)
    if top_teams:
        result_df = result_df[result_df['Team'].isin(top_teams)].copy()
        logger.info(f"After filtering to Top 50 team perspectives: {len(result_df)} rows")

    # Sort by date descending (most recent first)
    result_df = result_df.sort_values('date', ascending=False)

    logger.info(f"Final dataset: {len(result_df)} rows, {result_df['Team'].nunique()} unique teams")

    return result_df


def save_filtered_schedule(df: pd.DataFrame) -> bool:
    """
    Saves filtered schedule to CSV.

    Args:
        df: Filtered schedule DataFrame

    Returns:
        True if save successful, False otherwise
    """
    try:
        # Create output directory
        OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

        # Save to CSV
        df.to_csv(OUTPUT_PATH, index=False)
        logger.info(f"Saved filtered schedule to {OUTPUT_PATH}")

        # Display summary statistics
        if not df.empty:
            logger.info("\n=== Filtered Schedule Summary ===")
            logger.info(f"Total games (team perspective): {len(df)}")
            logger.info(f"Unique teams: {df['Team'].nunique()}")
            logger.info(f"Date range: {df['date'].min()} to {df['date'].max()}")
            logger.info(f"Conference games: {df['is_conf_game'].sum()}")
            logger.info(f"Non-conference games: {(~df['is_conf_game']).sum()}")

            # Top 10 teams by game count
            logger.info("\nTop 10 teams by game count:")
            top_teams_games = df['Team'].value_counts().head(10)
            for team, count in top_teams_games.items():
                logger.info(f"  {team}: {count} games")

        return True

    except Exception as e:
        logger.error(f"Error saving filtered schedule: {e}")
        return False


def main():
    """Main execution function."""
    logger.info("Starting schedule filter")

    # Step 1: Identify Top 50 teams
    top_teams = get_top_50_teams()

    if not top_teams:
        logger.error("No teams identified - cannot filter schedule")
        return False

    # Step 2: Filter schedule
    filtered_df = filter_schedule(top_teams)

    if filtered_df.empty:
        logger.error("Filtered schedule is empty")
        return False

    # Step 3: Save filtered schedule
    if save_filtered_schedule(filtered_df):
        logger.info("Schedule filtering completed successfully")
        return True
    else:
        logger.error("Failed to save filtered schedule")
        return False


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
