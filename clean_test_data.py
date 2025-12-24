"""
Test Data Cleaner
Clean and validate the 2020-2025 test dataset before full historical scrape
"""
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

class TestDataCleaner:
    def __init__(self, test_dir="data/polls_test"):
        self.test_dir = Path(test_dir)
        self.raw_dir = self.test_dir / "raw"
        self.cleaned_dir = self.test_dir / "cleaned"
        self.logs_dir = self.test_dir / "logs"
        
        # Create directories
        self.cleaned_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        self.cleaning_log = {
            "timestamp": datetime.now().isoformat(),
            "years_cleaned": [],
            "issues_found": [],
            "team_names_standardized": {}
        }
    
    def explore_structure(self):
        """Quick structure analysis of test data"""
        print("="*80)
        print("TEST DATA STRUCTURE ANALYSIS")
        print("="*80 + "\n")
        
        year_dirs = sorted([d for d in self.raw_dir.iterdir() if d.is_dir()])
        
        if not year_dirs:
            print("❌ No data found in", self.raw_dir)
            return None
        
        print(f"Found {len(year_dirs)} years: {[d.name for d in year_dirs]}\n")
        
        structure_info = {}
        
        for year_dir in year_dirs:
            year = year_dir.name
            csv_files = list(year_dir.glob("polls_*.csv"))
            
            if not csv_files:
                print(f"⚠️  {year}: No CSV files")
                continue
            
            # Analyze first CSV
            df = pd.read_csv(csv_files[0])
            
            structure_info[year] = {
                "file": csv_files[0].name,
                "rows": len(df),
                "columns": list(df.columns),
                "first_col": df.columns[0],
                "week_cols": self._identify_week_columns(df.columns)
            }
            
            print(f"📅 {year}:")
            print(f"   Rows: {len(df)}")
            print(f"   Columns: {len(df.columns)}")
            print(f"   First column: '{df.columns[0]}'")
            print(f"   Week columns: {len(structure_info[year]['week_cols'])}")
            
            # Show sample
            if len(df) > 0:
                print(f"   Sample team: {df[df.columns[0]].iloc[0]}")
            print()
        
        return structure_info
    
    def _identify_week_columns(self, columns):
        """Identify week columns"""
        week_cols = []
        for col in columns:
            col_str = str(col).lower()
            if any(w in col_str for w in ['wk', 'week', 'pre', 'final']):
                week_cols.append(col)
        return week_cols
    
    def extract_all_team_names(self):
        """Extract all unique team names from test data"""
        print("="*80)
        print("EXTRACTING TEAM NAMES")
        print("="*80 + "\n")
        
        all_teams = set()
        
        for year_dir in sorted(self.raw_dir.iterdir()):
            if not year_dir.is_dir():
                continue
            
            year = year_dir.name
            csv_files = list(year_dir.glob("polls_*.csv"))
            
            for csv_file in csv_files:
                df = pd.read_csv(csv_file)
                team_col = df.columns[0]
                teams = df[team_col].dropna().unique()
                all_teams.update(teams)
                print(f"   {year}: {len(teams)} teams")
        
        print(f"\n✅ Total unique team names: {len(all_teams)}")
        
        # Save team list
        teams_file = self.logs_dir / "unique_team_names.txt"
        with open(teams_file, 'w') as f:
            for team in sorted(all_teams):
                f.write(f"{team}\n")
        
        print(f"💾 Saved to: {teams_file}")
        
        return sorted(all_teams)
    
    def clean_year(self, year, team_mapping=None):
        """
        Clean a single year of test data
        Convert from wide format to long format
        """
        year_dir = self.raw_dir / str(year)
        csv_files = list(year_dir.glob("polls_*.csv"))
        
        if not csv_files:
            raise FileNotFoundError(f"No CSV files for {year}")
        
        # Read first table
        df = pd.read_csv(csv_files[0])
        
        # Identify columns
        team_col = df.columns[0]
        week_cols = self._identify_week_columns(df.columns)
        
        # Convert to long format
        records = []
        
        for idx, row in df.iterrows():
            team_name = row[team_col]
            
            if pd.isna(team_name) or team_name == '':
                continue
            
            # Standardize team name if mapping provided
            standardized_name = team_name
            if team_mapping and team_name in team_mapping:
                standardized_name = team_mapping[team_name]
            
            for week_col in week_cols:
                rank_val = row[week_col]
                
                if pd.isna(rank_val) or rank_val == '':
                    continue
                
                # Parse week number
                week_num = self._parse_week_number(week_col)
                
                # Parse rank
                rank_pos, receiving_votes = self._parse_rank_value(rank_val)
                
                records.append({
                    'season_year': f"{year-1}-{year}",
                    'year': year,
                    'week_number': week_num,
                    'week_label': week_col,
                    'team_name': standardized_name,
                    'original_team_name': team_name,
                    'rank_position': rank_pos,
                    'receiving_votes': receiving_votes
                })
        
        return pd.DataFrame(records)
    
    def _parse_week_number(self, week_col):
        """Extract week number from column name"""
        col_lower = str(week_col).lower()
        
        if 'pre' in col_lower:
            return 0
        if 'final' in col_lower:
            return 99
        
        import re
        match = re.search(r'\d+', week_col)
        if match:
            return int(match.group())
        
        return -1
    
    def _parse_rank_value(self, rank_val):
        """Parse ranking value"""
        if pd.isna(rank_val):
            return None, False
        
        rank_str = str(rank_val).strip()
        
        if rank_str.upper() == 'RV':
            return None, True
        
        try:
            rank = int(float(rank_str))
            return rank, False
        except ValueError:
            return None, False
    
    def clean_all_test_years(self):
        """Clean all years in test dataset"""
        print("\n" + "="*80)
        print("CLEANING TEST DATA")
        print("="*80 + "\n")
        
        year_dirs = sorted([d for d in self.raw_dir.iterdir() if d.is_dir()])
        
        for year_dir in year_dirs:
            year = int(year_dir.name)
            
            try:
                print(f"Cleaning {year}...", end=' ')
                cleaned_df = self.clean_year(year)
                
                # Save
                output_file = self.cleaned_dir / f"{year}_cleaned.csv"
                cleaned_df.to_csv(output_file, index=False)
                
                self.cleaning_log["years_cleaned"].append(year)
                
                print(f"✅ {len(cleaned_df)} records")
                
            except Exception as e:
                print(f"❌ Error: {e}")
                self.cleaning_log["issues_found"].append({
                    "year": year,
                    "error": str(e)
                })
        
        print(f"\n✅ Cleaned {len(self.cleaning_log['years_cleaned'])} years")
        
        # Save cleaning log
        log_file = self.logs_dir / f"cleaning_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(log_file, 'w') as f:
            json.dump(self.cleaning_log, f, indent=2)
        
        print(f"💾 Log saved: {log_file}")
    
    def combine_test_years(self):
        """Combine all cleaned test years"""
        print("\n" + "="*80)
        print("COMBINING TEST DATA")
        print("="*80 + "\n")
        
        cleaned_files = sorted(self.cleaned_dir.glob("*_cleaned.csv"))
        
        if not cleaned_files:
            print("❌ No cleaned files found")
            return None
        
        all_data = []
        
        for csv_file in cleaned_files:
            df = pd.read_csv(csv_file)
            all_data.append(df)
            print(f"   {csv_file.stem}: {len(df)} records")
        
        # Combine
        combined_df = pd.concat(all_data, ignore_index=True)
        combined_df.sort_values(['year', 'week_number', 'rank_position'], inplace=True)
        
        # Save
        combined_dir = self.test_dir / "combined"
        combined_dir.mkdir(exist_ok=True)
        
        output_file = combined_dir / "test_all_years_combined.csv"
        combined_df.to_csv(output_file, index=False)
        
        print(f"\n✅ Combined dataset:")
        print(f"   Total records: {len(combined_df):,}")
        print(f"   Years: {combined_df['year'].min()}-{combined_df['year'].max()}")
        print(f"   Unique teams: {combined_df['team_name'].nunique()}")
        print(f"   Output: {output_file}")
        
        return combined_df
    
    def validate_cleaned_data(self, combined_df):
        """Quick validation of cleaned data"""
        print("\n" + "="*80)
        print("VALIDATING CLEANED DATA")
        print("="*80 + "\n")
        
        issues = []
        
        # Check 1: Missing weeks
        print("✓ Check 1: Week sequence")
        for year in combined_df['year'].unique():
            year_data = combined_df[combined_df['year'] == year]
            weeks = sorted(year_data['week_number'].unique())
            
            # Remove special weeks (0=pre, 99=final)
            regular_weeks = [w for w in weeks if 0 < w < 99]
            
            if regular_weeks:
                expected = range(min(regular_weeks), max(regular_weeks) + 1)
                missing = [w for w in expected if w not in regular_weeks]
                
                if missing:
                    issues.append(f"{year}: Missing weeks {missing}")
                    print(f"   ⚠️  {year}: Missing weeks {missing}")
                else:
                    print(f"   ✅ {year}: Weeks {min(regular_weeks)}-{max(regular_weeks)}")
        
        # Check 2: Ranking values
        print("\n✓ Check 2: Ranking values")
        invalid_ranks = combined_df[
            (combined_df['rank_position'].notna()) & 
            ((combined_df['rank_position'] < 1) | (combined_df['rank_position'] > 25))
        ]
        
        if len(invalid_ranks) > 0:
            issues.append(f"Invalid ranks: {len(invalid_ranks)}")
            print(f"   ⚠️  Found {len(invalid_ranks)} invalid rankings")
        else:
            print(f"   ✅ All rankings in valid range (1-25)")
        
        # Check 3: Team names
        print("\n✓ Check 3: Team names")
        unique_teams = combined_df['team_name'].nunique()
        print(f"   ✅ {unique_teams} unique team names")
        
        # Check for potential duplicates
        team_counts = combined_df.groupby('team_name').size().sort_values(ascending=False)
        print(f"   Most frequent teams:")
        for team, count in team_counts.head(5).items():
            print(f"      - {team}: {count} appearances")
        
        # Check 4: Data completeness
        print("\n✓ Check 4: Data completeness")
        print(f"   Total records: {len(combined_df):,}")
        print(f"   Records with rankings: {combined_df['rank_position'].notna().sum():,}")
        print(f"   Records receiving votes only: {combined_df['receiving_votes'].sum():,}")
        
        print("\n" + "="*80)
        
        if issues:
            print("⚠️  Found issues (review above)")
            return False
        else:
            print("✅ All validation checks passed!")
            return True
    
    def run_full_cleaning(self):
        """Run complete cleaning pipeline"""
        print("="*80)
        print("TEST DATA CLEANING PIPELINE")
        print("="*80 + "\n")
        
        # Step 1: Explore
        structure = self.explore_structure()
        if not structure:
            return 1
        
        # Step 2: Extract team names
        team_names = self.extract_all_team_names()
        
        # Step 3: Clean
        self.clean_all_test_years()
        
        # Step 4: Combine
        combined = self.combine_test_years()
        
        if combined is None:
            return 1
        
        # Step 5: Validate
        validation_passed = self.validate_cleaned_data(combined)
        
        # Summary
        print("\n" + "="*80)
        print("CLEANING COMPLETE!")
        print("="*80)
        
        if validation_passed:
            print("\n✅ Test data successfully cleaned and validated!")
            print("\nNext steps:")
            print("1. Review combined file: data/polls_test/combined/test_all_years_combined.csv")
            print("2. If satisfied → proceed to full 43-year scrape")
            print("3. Run: python run_historical_scrape.py --yes")
        else:
            print("\n⚠️  Cleaning completed with warnings")
            print("Review issues above before proceeding to full scrape")
        
        return 0 if validation_passed else 1

def main():
    """Run test data cleaning"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Clean test polls data (2020-2025)')
    parser.add_argument('--test-dir', default='data/polls_test',
                       help='Test data directory')
    
    args = parser.parse_args()
    
    cleaner = TestDataCleaner(test_dir=args.test_dir)
    return cleaner.run_full_cleaning()

if __name__ == "__main__":
    exit(main())
