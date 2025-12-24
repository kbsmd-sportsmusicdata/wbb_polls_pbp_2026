"""
Data Structure Explorer
Analyzes raw scraped data to understand format variations across years
Run this BEFORE cleaning to identify patterns and issues
"""
import pandas as pd
from pathlib import Path
import json
from collections import defaultdict, Counter
from datetime import datetime

class PollsDataExplorer:
    def __init__(self, raw_data_dir="data/polls/raw"):
        self.raw_dir = Path(raw_data_dir)
        self.exploration_results = {
            "timestamp": datetime.now().isoformat(),
            "years_analyzed": [],
            "column_patterns": defaultdict(list),
            "team_names_by_year": {},
            "data_quality_issues": [],
            "summary": {}
        }
    
    def explore_all_years(self):
        """Explore all years in raw data directory"""
        print("="*80)
        print("POLLS DATA STRUCTURE EXPLORER")
        print("="*80 + "\n")
        
        year_dirs = sorted([d for d in self.raw_dir.iterdir() if d.is_dir()])
        
        if not year_dirs:
            print("❌ No year directories found in", self.raw_dir)
            return
        
        print(f"Found {len(year_dirs)} year directories\n")
        
        for year_dir in year_dirs:
            year = year_dir.name
            print(f"📅 Analyzing {year}...")
            
            try:
                year_analysis = self.analyze_year(year_dir)
                self.exploration_results["years_analyzed"].append(year)
                
                # Track column patterns
                for table_info in year_analysis["tables"]:
                    pattern = self.get_column_pattern(table_info["columns"])
                    self.exploration_results["column_patterns"][pattern].append(year)
                
                # Track team names
                if year_analysis["team_names"]:
                    self.exploration_results["team_names_by_year"][year] = year_analysis["team_names"]
                
                print(f"   ✅ Analyzed {len(year_analysis['tables'])} table(s)\n")
                
            except Exception as e:
                print(f"   ❌ Error: {e}\n")
                self.exploration_results["data_quality_issues"].append({
                    "year": year,
                    "error": str(e)
                })
        
        self.generate_summary()
        self.save_results()
    
    def analyze_year(self, year_dir):
        """Analyze structure of data for one year"""
        csv_files = list(year_dir.glob("polls_*.csv"))
        
        year_analysis = {
            "year": year_dir.name,
            "num_files": len(csv_files),
            "tables": [],
            "team_names": []
        }
        
        for csv_file in csv_files:
            df = pd.read_csv(csv_file)
            
            # Identify column structure
            first_col = df.columns[0]
            week_cols = self.identify_week_columns(df.columns)
            
            # Get sample team names
            team_names = df[first_col].dropna().head(10).tolist()
            year_analysis["team_names"].extend(team_names)
            
            # Check data quality
            quality_issues = self.check_data_quality(df, first_col, week_cols)
            
            table_info = {
                "filename": csv_file.name,
                "rows": len(df),
                "columns": list(df.columns),
                "first_column_name": first_col,
                "week_columns": week_cols,
                "week_count": len(week_cols),
                "sample_teams": team_names[:5],
                "quality_issues": quality_issues
            }
            
            year_analysis["tables"].append(table_info)
        
        return year_analysis
    
    def identify_week_columns(self, columns):
        """Identify which columns represent weeks"""
        week_cols = []
        
        for col in columns:
            col_str = str(col).lower()
            
            # Common patterns
            if any(pattern in col_str for pattern in ['wk', 'week', 'pre', 'final']):
                week_cols.append(col)
            # Numeric columns might be weeks
            elif col_str.isdigit():
                week_cols.append(col)
        
        return week_cols
    
    def get_column_pattern(self, columns):
        """Generate a pattern signature for column structure"""
        # Create a simplified pattern
        pattern_parts = []
        
        for col in columns[:10]:  # First 10 columns
            col_str = str(col).lower()
            if 'school' in col_str or 'team' in col_str:
                pattern_parts.append('TEAM')
            elif any(w in col_str for w in ['wk', 'week']):
                pattern_parts.append('WK')
            elif 'pre' in col_str:
                pattern_parts.append('PRE')
            elif 'final' in col_str:
                pattern_parts.append('FINAL')
            else:
                pattern_parts.append('OTHER')
        
        return '|'.join(pattern_parts)
    
    def check_data_quality(self, df, team_col, week_cols):
        """Check for data quality issues in this table"""
        issues = []
        
        # 1. Check for empty team names
        empty_teams = df[team_col].isna().sum()
        if empty_teams > 0:
            issues.append(f"{empty_teams} empty team names")
        
        # 2. Check for unexpected values in ranking columns
        for week_col in week_cols[:5]:  # Check first 5 weeks
            unique_vals = df[week_col].dropna().unique()
            # Should be mostly 1-25 or 'RV'
            numeric_vals = [v for v in unique_vals if str(v).replace('.','').isdigit()]
            non_numeric = [v for v in unique_vals if v not in numeric_vals and str(v).upper() != 'RV']
            
            if len(non_numeric) > 2:
                issues.append(f"Unexpected values in {week_col}: {non_numeric[:3]}")
        
        # 3. Check row count (should be ~25-30 teams)
        if len(df) > 50:
            issues.append(f"Unusually many rows: {len(df)}")
        elif len(df) < 10:
            issues.append(f"Very few rows: {len(df)}")
        
        return issues
    
    def generate_summary(self):
        """Generate summary statistics"""
        print("\n" + "="*80)
        print("EXPLORATION SUMMARY")
        print("="*80 + "\n")
        
        # Years analyzed
        years = self.exploration_results["years_analyzed"]
        print(f"📊 Years analyzed: {len(years)}")
        if years:
            print(f"   Range: {min(years)} - {max(years)}")
        
        # Column patterns
        print(f"\n📋 Unique column patterns found: {len(self.exploration_results['column_patterns'])}")
        for pattern, years_list in sorted(self.exploration_results['column_patterns'].items()):
            if len(years_list) > 3:
                print(f"   Pattern: {pattern}")
                print(f"   Used in: {len(years_list)} years ({min(years_list)}-{max(years_list)})")
        
        # Team name analysis
        all_team_names = set()
        for year, teams in self.exploration_results["team_names_by_year"].items():
            all_team_names.update(teams)
        
        print(f"\n🏀 Unique team names across all years: {len(all_team_names)}")
        
        # Common team name variations
        team_name_counts = Counter()
        for teams in self.exploration_results["team_names_by_year"].values():
            team_name_counts.update(teams)
        
        print("\n   Most frequently appearing teams:")
        for team, count in team_name_counts.most_common(10):
            print(f"   - {team}: {count} years")
        
        # Data quality issues
        if self.exploration_results["data_quality_issues"]:
            print(f"\n⚠️  Data quality issues: {len(self.exploration_results['data_quality_issues'])}")
            for issue in self.exploration_results["data_quality_issues"][:5]:
                print(f"   - {issue['year']}: {issue['error']}")
        else:
            print("\n✅ No critical data quality issues detected")
        
        # Save summary stats
        self.exploration_results["summary"] = {
            "total_years": len(years),
            "year_range": f"{min(years)}-{max(years)}" if years else "N/A",
            "unique_column_patterns": len(self.exploration_results['column_patterns']),
            "unique_teams": len(all_team_names),
            "data_quality_issues": len(self.exploration_results["data_quality_issues"])
        }
    
    def save_results(self):
        """Save exploration results to JSON"""
        output_dir = Path("data/polls/logs")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "data_exploration.json"
        
        with open(output_file, 'w') as f:
            json.dump(self.exploration_results, f, indent=2)
        
        print(f"\n💾 Detailed results saved to: {output_file}")
    
    def generate_team_name_mapping_template(self):
        """Generate initial team name mapping template"""
        print("\n📝 Generating team name mapping template...")
        
        all_team_names = set()
        for teams in self.exploration_results["team_names_by_year"].values():
            all_team_names.update(teams)
        
        # Create mapping dataframe
        mapping_data = []
        for team_name in sorted(all_team_names):
            mapping_data.append({
                'raw_name': team_name,
                'standardized_name': team_name,  # Default to same
                'needs_review': True,
                'notes': ''
            })
        
        mapping_df = pd.DataFrame(mapping_data)
        
        # Save template
        output_dir = Path("data/polls/reference")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / "team_name_mappings_template.csv"
        mapping_df.to_csv(output_file, index=False)
        
        print(f"✅ Template saved: {output_file}")
        print(f"   Contains {len(mapping_df)} unique team names")
        print(f"   ⚠️  REVIEW NEEDED: Manually edit to standardize variations")
    
    def identify_structural_changes(self):
        """Identify years where data structure changed significantly"""
        print("\n🔍 Identifying structural changes...\n")
        
        patterns_by_year = {}
        for pattern, years_list in self.exploration_results["column_patterns"].items():
            for year in years_list:
                patterns_by_year[year] = pattern
        
        # Look for pattern changes
        sorted_years = sorted(patterns_by_year.keys())
        changes = []
        
        prev_pattern = None
        for year in sorted_years:
            pattern = patterns_by_year[year]
            if prev_pattern and pattern != prev_pattern:
                changes.append({
                    "year": year,
                    "old_pattern": prev_pattern,
                    "new_pattern": pattern
                })
                print(f"   📌 {year}: Structure changed")
                print(f"      From: {prev_pattern}")
                print(f"      To:   {pattern}\n")
            prev_pattern = pattern
        
        if not changes:
            print("   ✅ No significant structural changes detected")
        
        return changes

def main():
    """Run data exploration"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Explore raw polls data structure')
    parser.add_argument('--raw-dir', default='data/polls/raw',
                       help='Path to raw data directory')
    parser.add_argument('--generate-mapping', action='store_true',
                       help='Generate team name mapping template')
    
    args = parser.parse_args()
    
    explorer = PollsDataExplorer(raw_data_dir=args.raw_dir)
    
    # Run exploration
    explorer.explore_all_years()
    
    # Identify structural changes
    explorer.identify_structural_changes()
    
    # Optionally generate mapping template
    if args.generate_mapping:
        explorer.generate_team_name_mapping_template()
    else:
        print("\n💡 TIP: Run with --generate-mapping to create team name template")
    
    print("\n" + "="*80)
    print("EXPLORATION COMPLETE!")
    print("="*80)
    print("\nNext steps:")
    print("1. Review: data/polls/logs/data_exploration.json")
    print("2. Generate mapping: python explore_raw_data.py --generate-mapping")
    print("3. Follow DATA_CLEANING_GUIDE.md for cleaning instructions")

if __name__ == "__main__":
    main()
