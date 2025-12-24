#!/usr/bin/env python3
"""
TEST PIPELINE: WBB Polls Scraper (2020-2025 Only)
==================================================

Quick validation of the complete pipeline before running full 43-year scrape.

PURPOSE:
- Test scraping logic on recent years
- Validate data structure consistency
- Verify cleaning process works
- Confirm folder organization
- Complete in ~20-30 minutes

USAGE:
    python test_pipeline_scrape.py

This will scrape 6 years (2020-2025) and run initial validation.

IF SUCCESSFUL → Proceed to full 43-year scrape
IF ISSUES → Fix problems on small dataset first
"""

import sys
from pathlib import Path
from datetime import datetime
import time

# Import the core scraper
sys.path.insert(0, str(Path(__file__).parent))
from scrape_historical_polls import HistoricalPollsScraper

class TestPipelineScraper:
    def __init__(self):
        # Use separate test directory
        self.base_dir = Path("data/polls_test")
        self.scraper = HistoricalPollsScraper(base_dir=str(self.base_dir))
        self.scraper.delay_seconds = 3
        
        # Test parameters
        self.test_start_year = 2020
        self.test_end_year = 2025
        self.num_years = self.test_end_year - self.test_start_year + 1
    
    def print_header(self):
        """Print test pipeline header"""
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                          TEST PIPELINE SCRAPER                               ║
║                          Years: 2020-2025 (6 years)                          ║
║                                                                              ║
║  This quick test validates your pipeline before the full 43-year scrape     ║
╚══════════════════════════════════════════════════════════════════════════════╝
        """)
    
    def estimate_time(self):
        """Estimate test time"""
        total_seconds = self.num_years * self.scraper.delay_seconds
        minutes = total_seconds / 60
        
        print(f"📊 TEST PLAN:")
        print(f"   Years to scrape: {self.num_years} ({self.test_start_year}-{self.test_end_year})")
        print(f"   Delay per year: {self.scraper.delay_seconds} seconds")
        print(f"   Estimated time: ~{minutes:.0f} minutes (including processing)")
        print(f"   Test data directory: {self.base_dir}")
        print()
    
    def pre_flight_checks(self):
        """Check prerequisites before starting"""
        print("🔍 PRE-FLIGHT CHECKS:")
        
        checks_passed = True
        
        # 1. Check imports
        try:
            import pandas
            import requests
            print("   ✅ Required packages installed (pandas, requests)")
        except ImportError as e:
            print(f"   ❌ Missing package: {e}")
            checks_passed = False
        
        # 2. Check network
        try:
            import requests
            resp = requests.get("https://www.sports-reference.com", timeout=5)
            print("   ✅ Network connection to Sports-Reference")
        except Exception as e:
            print(f"   ⚠️  Network issue: {e}")
            print("      (May still work, continuing...)")
        
        # 3. Check directory permissions
        try:
            self.base_dir.mkdir(parents=True, exist_ok=True)
            print(f"   ✅ Can write to {self.base_dir}")
        except Exception as e:
            print(f"   ❌ Cannot create directory: {e}")
            checks_passed = False
        
        print()
        return checks_passed
    
    def run_test_scrape(self):
        """Run the test scrape"""
        print("="*80)
        print("STARTING TEST SCRAPE")
        print("="*80 + "\n")
        
        start_time = time.time()
        
        try:
            # Run scraper for test years only
            self.scraper.scrape_all_years(
                start_year=self.test_start_year,
                end_year=self.test_end_year
            )
            
            # Generate manifest
            print("\n📋 Generating manifest...")
            manifest = self.scraper.generate_manifest()
            
            elapsed = time.time() - start_time
            
            print("\n" + "="*80)
            print("TEST SCRAPE COMPLETE!")
            print("="*80)
            print(f"⏱️  Time taken: {elapsed/60:.1f} minutes")
            
            return True, manifest
            
        except KeyboardInterrupt:
            print("\n\n⚠️  Test interrupted by user")
            return False, None
        except Exception as e:
            print(f"\n\n❌ Test failed: {e}")
            import traceback
            traceback.print_exc()
            return False, None
    
    def validate_test_results(self, manifest):
        """Quick validation of test results"""
        print("\n" + "="*80)
        print("VALIDATING TEST RESULTS")
        print("="*80 + "\n")
        
        validation_passed = True
        
        # Check 1: All years scraped?
        years_scraped = list(manifest.get('years', {}).keys())
        expected_years = [str(y) for y in range(self.test_start_year, self.test_end_year + 1)]
        
        print(f"✓ Check 1: Years scraped")
        if set(years_scraped) == set(expected_years):
            print(f"   ✅ All {len(expected_years)} years present: {min(years_scraped)}-{max(years_scraped)}")
        else:
            missing = set(expected_years) - set(years_scraped)
            print(f"   ❌ Missing years: {missing}")
            validation_passed = False
        
        # Check 2: CSV files exist?
        print(f"\n✓ Check 2: CSV files")
        total_csv = 0
        for year, year_info in manifest['years'].items():
            csv_count = len(year_info.get('csv_files', []))
            total_csv += csv_count
            if csv_count == 0:
                print(f"   ⚠️  {year}: No CSV files found")
                validation_passed = False
        
        if total_csv > 0:
            print(f"   ✅ Found {total_csv} CSV files total")
        
        # Check 3: Metadata files exist?
        print(f"\n✓ Check 3: Metadata files")
        years_with_metadata = sum(1 for y in manifest['years'].values() if y.get('has_metadata'))
        if years_with_metadata == len(years_scraped):
            print(f"   ✅ All {years_with_metadata} years have metadata.json")
        else:
            print(f"   ⚠️  Only {years_with_metadata}/{len(years_scraped)} years have metadata")
        
        # Check 4: Data structure consistency
        print(f"\n✓ Check 4: Data structure")
        structures = {}
        for year, year_info in manifest['years'].items():
            for csv_info in year_info.get('csv_files', []):
                cols = csv_info.get('columns')
                rows = csv_info.get('rows')
                if cols and rows:
                    structure_key = f"{cols}cols_{rows}rows"
                    if structure_key not in structures:
                        structures[structure_key] = []
                    structures[structure_key].append(year)
        
        print(f"   Found {len(structures)} unique structure pattern(s)")
        for structure, years in structures.items():
            print(f"   - {structure}: {len(years)} years")
        
        if len(structures) == 1:
            print(f"   ✅ Consistent structure across all years")
        else:
            print(f"   ⚠️  Structure varies (may need different cleaning approaches)")
        
        print("\n" + "="*80)
        
        return validation_passed
    
    def explore_test_data(self):
        """Quick exploration of test data"""
        print("\n" + "="*80)
        print("EXPLORING TEST DATA STRUCTURE")
        print("="*80 + "\n")
        
        import pandas as pd
        
        # Find first CSV file
        raw_dir = self.base_dir / "raw"
        first_csv = None
        
        for year_dir in sorted(raw_dir.iterdir()):
            if year_dir.is_dir():
                csv_files = list(year_dir.glob("polls_*.csv"))
                if csv_files:
                    first_csv = csv_files[0]
                    break
        
        if not first_csv:
            print("❌ No CSV files found to explore")
            return
        
        print(f"📄 Sample file: {first_csv.relative_to(self.base_dir)}\n")
        
        try:
            df = pd.read_csv(first_csv)
            
            print(f"📊 Structure:")
            print(f"   Rows: {len(df)}")
            print(f"   Columns: {len(df.columns)}")
            print(f"\n   Column names:")
            for i, col in enumerate(df.columns, 1):
                print(f"   {i:2d}. {col}")
            
            print(f"\n📋 First 5 rows:")
            print(df.head().to_string())
            
            print(f"\n🔍 Analysis:")
            first_col = df.columns[0]
            week_cols = [c for c in df.columns if 
                        any(w in str(c).lower() for w in ['wk', 'week', 'pre', 'final'])]
            
            print(f"   - First column (teams): '{first_col}'")
            print(f"   - Week columns found: {len(week_cols)}")
            print(f"   - Sample weeks: {week_cols[:5]}")
            
            # Sample rankings
            if len(df) > 0 and len(week_cols) > 0:
                sample_team = df[first_col].iloc[0]
                sample_week = week_cols[0]
                sample_rank = df[sample_week].iloc[0]
                print(f"\n   Example: {sample_team} in {sample_week} = {sample_rank}")
            
        except Exception as e:
            print(f"❌ Error exploring data: {e}")
    
    def generate_next_steps(self, validation_passed):
        """Generate recommended next steps"""
        print("\n" + "="*80)
        print("RECOMMENDED NEXT STEPS")
        print("="*80 + "\n")
        
        if validation_passed:
            print("✅ TEST PIPELINE SUCCESSFUL!\n")
            print("Your scraping logic works correctly. You can now:")
            print()
            print("📍 OPTION 1: Proceed to full 43-year scrape")
            print("   python run_historical_scrape.py --yes")
            print("   (Estimated time: 2-3 hours)")
            print()
            print("📍 OPTION 2: Test cleaning process first")
            print("   1. python explore_raw_data.py --raw-dir data/polls_test/raw")
            print("   2. Review structure analysis")
            print("   3. Build cleaning script for test data")
            print("   4. If cleaning works → full scrape + clean all 43 years")
            print()
            print("📍 OPTION 3: Test validation")
            print("   1. Create test database from test data")
            print("   2. python validate_polls_data.py")
            print("   3. Verify all checks pass")
            print()
        else:
            print("⚠️  TEST REVEALED ISSUES\n")
            print("Please review the validation warnings above.")
            print()
            print("Common issues and fixes:")
            print("   - Missing years: Check if Sports-Reference has data")
            print("   - No CSV files: Check scraping logs for errors")
            print("   - Structure varies: Expected - will handle in cleaning")
            print()
            print("After fixing issues:")
            print("   1. Delete test directory: rm -rf data/polls_test")
            print("   2. Re-run: python test_pipeline_scrape.py")
        
        print("\n" + "="*80)
        print("Test data saved in: data/polls_test/")
        print("(Safe to delete after validation)")
        print("="*80)
    
    def run_full_test(self):
        """Run complete test pipeline"""
        self.print_header()
        self.estimate_time()
        
        # Pre-flight checks
        if not self.pre_flight_checks():
            print("\n❌ Pre-flight checks failed. Aborting test.")
            return 1
        
        # Confirm
        print("Ready to start test scrape?")
        response = input("Continue? (yes/no): ")
        if response.lower() not in ['yes', 'y']:
            print("Test cancelled.")
            return 0
        
        print()
        
        # Run scrape
        success, manifest = self.run_test_scrape()
        
        if not success or not manifest:
            print("\n❌ Test scrape failed. Check errors above.")
            return 1
        
        # Validate results
        validation_passed = self.validate_test_results(manifest)
        
        # Explore structure
        self.explore_test_data()
        
        # Next steps
        self.generate_next_steps(validation_passed)
        
        return 0 if validation_passed else 1

def main():
    """Main execution"""
    test_scraper = TestPipelineScraper()
    return test_scraper.run_full_test()

if __name__ == "__main__":
    exit(main())
