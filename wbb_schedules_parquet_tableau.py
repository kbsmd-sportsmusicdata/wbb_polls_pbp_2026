import pandas as pd
import numpy as np
from datetime import datetime

# ==========================================
# CONFIGURATION & COLUMN MAPPING
# ==========================================
# Update these values if your column names differ
INPUT_FILE = 'wbb_schedule_2026.parquet'
OUTPUT_FILE = 'wbb_tableau_data.csv'

# Dictionary mapping logical names to your actual file columns.
COLS = {
    'GAME_ID': 'id',
    'DATE': 'date',
    'STATUS': 'status_type_name',
    'SEASON_TYPE': 'type_id',
    'NOTES': 'notes_headline',
    'CONF_GAME': 'conference_competition',
    
    # Home Team Data
    'HOME_TEAM': 'home_location',
    'HOME_SCORE': 'home_score',
    'HOME_LOGO': 'home_logo',
    'HOME_CONF_ID': 'home_conference_id',
    'HOME_WINNER': 'home_winner',
    'HOME_RANK': 'home_current_rank',
    'HOME_RECORDS': 'home_records',

    # Away Team Data
    'AWAY_TEAM': 'away_location',
    'AWAY_SCORE': 'away_score',
    'AWAY_LOGO': 'away_logo',
    'AWAY_CONF_ID': 'away_conference_id',
    'AWAY_WINNER': 'away_winner',
    'AWAY_RANK': 'away_current_rank',
    'AWAY_RECORDS': 'away_records',

    # Group/Tournament Info
    'GROUP_ID': 'groups_id',
    'GROUP_NAME': 'groups_name',
    'GROUP_SHORT': 'groups_short_name'
}

def load_and_inspect(filepath):
    """Loads the file and prints columns to help with mapping."""
    try:
        df = pd.read_parquet(filepath)
        print(f"✅ Successfully loaded {filepath}")
        print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")
        return df
    except Exception as e:
        print(f"❌ Error loading file: {e}")
        return None

def process_game_data(df):
    """Reshapes and calculates metrics."""
    
    # 1. Filter for completed games
    df['is_completed'] = df[COLS['STATUS']].str.contains('FINAL', case=False, na=False)

    # 2. Reshape Data (The "Tableau Pivot")
    # We map specific team attributes (Logo, Rank) to 'Team' vs 'Opponent' 
    # so they align with the row's perspective.
    
    # --- Home Perspective ---
    home_df = df.copy()
    home_df['Team'] = home_df[COLS['HOME_TEAM']]
    home_df['Team Score'] = home_df[COLS['HOME_SCORE']]
    home_df['Team Logo'] = home_df[COLS['HOME_LOGO']]
    home_df['Team Rank'] = home_df[COLS['HOME_RANK']]
    home_df['Team Conf ID'] = home_df[COLS['HOME_CONF_ID']]
    
    home_df['Opponent'] = home_df[COLS['AWAY_TEAM']]
    home_df['Opponent Score'] = home_df[COLS['AWAY_SCORE']]
    home_df['Opponent Logo'] = home_df[COLS['AWAY_LOGO']]
    home_df['Opponent Rank'] = home_df[COLS['AWAY_RANK']]
    home_df['Opponent Conf ID'] = home_df[COLS['AWAY_CONF_ID']]
    
    home_df['Location'] = 'Home'
    
    # --- Away Perspective ---
    away_df = df.copy()
    away_df['Team'] = away_df[COLS['AWAY_TEAM']]
    away_df['Team Score'] = away_df[COLS['AWAY_SCORE']]
    away_df['Team Logo'] = away_df[COLS['AWAY_LOGO']]
    away_df['Team Rank'] = away_df[COLS['AWAY_RANK']]
    away_df['Team Conf ID'] = away_df[COLS['AWAY_CONF_ID']]

    away_df['Opponent'] = away_df[COLS['HOME_TEAM']]
    away_df['Opponent Score'] = away_df[COLS['HOME_SCORE']]
    away_df['Opponent Logo'] = away_df[COLS['HOME_LOGO']]
    away_df['Opponent Rank'] = away_df[COLS['HOME_RANK']]
    away_df['Opponent Conf ID'] = away_df[COLS['HOME_CONF_ID']]
    
    away_df['Location'] = 'Away'
    
    # Combine them
    full_df = pd.concat([home_df, away_df], ignore_index=True)
    
    # 3. Calculate Game Results (Win/Loss)
    full_df['Win'] = np.where(
        (full_df['is_completed']) & (full_df['Team Score'] > full_df['Opponent Score']), 1, 0
    )
    full_df['Loss'] = np.where(
        (full_df['is_completed']) & (full_df['Team Score'] < full_df['Opponent Score']), 1, 0
    )
    
    # 4. Identify Conference Games
    if COLS['CONF_GAME'] in df.columns:
         full_df['is_conf_game'] = full_df[COLS['CONF_GAME']]
    else:
        full_df['is_conf_game'] = full_df[COLS['NOTES']].str.contains('Conference', case=False, na=False)

    # 5. Calculate Aggregated Records per Team
    
    # Overall Record
    team_stats = full_df.groupby('Team').agg(
        Total_Wins=('Win', 'sum'),
        Total_Losses=('Loss', 'sum')
    ).reset_index()
    
    # Conference Record
    conf_stats = full_df[full_df['is_conf_game'] == True].groupby('Team').agg(
        Conf_Wins=('Win', 'sum'),
        Conf_Losses=('Loss', 'sum')
    ).reset_index()
    
    # Merge stats back
    full_df = full_df.merge(team_stats, on='Team', how='left')
    full_df = full_df.merge(conf_stats, on='Team', how='left')
    
    # Fill NaN records with 0
    cols_to_fix = ['Total_Wins', 'Total_Losses', 'Conf_Wins', 'Conf_Losses']
    full_df[cols_to_fix] = full_df[cols_to_fix].fillna(0).astype(int)
    
    # Create formatted Record Strings
    full_df['Overall Record'] = full_df['Total_Wins'].astype(str) + "-" + full_df['Total_Losses'].astype(str)
    full_df['Conf Record'] = full_df['Conf_Wins'].astype(str) + "-" + full_df['Conf_Losses'].astype(str)

    # 6. Result Label for Tableau
    conditions = [
        (full_df['is_completed'] == False),
        (full_df['Win'] == 1),
        (full_df['Loss'] == 1)
    ]
    choices = ['Pending', 'W', 'L']
    full_df['Game Result'] = np.select(conditions, choices, default='Tie/Unknown')

    return full_df

def main():
    print("🏀 Starting WBB Schedule Processing...")
    
    # Load
    df = load_and_inspect(INPUT_FILE)
    if df is None:
        return

    # Check for missing columns (excluding optional ones might be safer, but we'll check all mapped ones)
    missing_cols = [c for key, c in COLS.items() if c not in df.columns]
    # Note: 'conference_competition' might be missing in some datasets, but we handle it in logic.
    if missing_cols:
        print(f"⚠️  WARNING: The following mapped columns were not found: {missing_cols}")
        # We proceed anyway, but those columns will fail if accessed directly. 
        # For safety in this script, we only output what exists.
    
    # Process
    processed_df = process_game_data(df)
    
    # Define Final Output Columns
    # We include:
    # 1. Standard Identifiers & Time
    # 2. Team Perspective Columns (Calculated/Mapped)
    # 3. Aggregated Records
    # 4. Raw Original Columns (Requested explicitly)
    
    standard_cols = [
        COLS['GAME_ID'], COLS['DATE'], 
        'Team', 'Opponent', 'Location', 'Game Result',
        'Team Score', 'Opponent Score',
        'Team Logo', 'Opponent Logo',
        'Team Rank', 'Opponent Rank',
        'Team Conf ID', 'Opponent Conf ID',
        'Overall Record', 'Conf Record', 'is_conf_game',
        COLS['SEASON_TYPE'], COLS['NOTES'], COLS['STATUS']
    ]
    
    # Add the raw requested columns to the list
    raw_requested = [
        COLS['HOME_LOGO'], COLS['HOME_CONF_ID'], COLS['HOME_WINNER'], 
        COLS['HOME_RANK'], COLS['HOME_RECORDS'],
        COLS['AWAY_LOGO'], COLS['AWAY_CONF_ID'], COLS['AWAY_WINNER'], 
        COLS['AWAY_RANK'], COLS['AWAY_RECORDS'],
        COLS['GROUP_ID'], COLS['GROUP_NAME'], COLS['GROUP_SHORT']
    ]
    
    all_target_columns = standard_cols + raw_requested
    
    # Filter to ensure we only ask for columns that actually exist in the processed dataframe
    final_output_cols = [c for c in all_target_columns if c in processed_df.columns]
    
    final_output = processed_df[final_output_cols]
    
    # Export
    final_output.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Processing Complete!")
    print(f"📄 Data saved to: {OUTPUT_FILE}")
    print(f"📊 Total Rows: {len(final_output)}")

if __name__ == "__main__":
    main()
