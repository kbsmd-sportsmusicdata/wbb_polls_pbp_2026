#!/usr/bin/env python3
"""
ONE-COMMAND TEST PIPELINE
=========================

Run the complete test pipeline (scrape → explore → clean → validate) in one command.

USAGE:
    python run_complete_test.py

This will:
1. Scrape 2020-2025 data (~20 min)
2. Explore data structure (~2 min)
3. Clean and standardize (~5 min)
4. Validate results (~1 min)
5. Generate report

Total time: ~30 minutes

WHAT YOU GET:
- Test data in data/polls_test/
- Cleaned and combined dataset
- Validation report
- Go/No-Go decision for full scrape
"""

import sys
import subprocess
from pathlib import Path
from datetime import datetime
import json

class MasterTestRunner:
    def __init__(self):
        self.start_time = datetime.now()
        self.test_dir = Path("data/polls_test")
        self.results = {
            "timestamp": self.start_time.isoformat(),
            "steps_completed": [],
            "steps_failed": [],
            "overall_success": False
        }
    
    def print_banner(self):
        """Print master banner"""
        print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║                     MASTER TEST PIPELINE RUNNER                              ║
║                                                                              ║
║  Complete validation of scraping → cleaning → analysis pipeline             ║
║  Test dataset: 2020-2025 (6 years)                                          ║
║  Estimated time: ~30 minutes                                                 ║
╚══════════════════════════════════════════════════════════════════════════════╝

Starting test pipeline at {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}
        """)
    
    def run_step(self, step_name, command, description):
        """Run a pipeline step"""
        print("\n" + "="*80)
        print(f"STEP: {step_name}")
        print("="*80)
        print(f"Description: {description}")
        print(f"Command: {command}")
        print()
        
        try:
            result = subprocess.run(
                command,
                shell=True,
                check=True,
                capture_output=False,
                text=True
            )
            
            self.results["steps_completed"].append({
                "step": step_name,
                "command": command,
                "success": True
            })
            
            print(f"\n✅ {step_name} completed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            print(f"\n❌ {step_name} failed")
            print(f"Error: {e}")
            
            self.results["steps_failed"].append({
                "step": step_name,
                "command": command,
                "error": str(e)
            })
            
            return False
    
    def check_files_exist(self, step_name, file_patterns):
        """Check if expected output files exist"""
        print(f"\n🔍 Checking {step_name} outputs...")
        
        all_exist = True
        for pattern in file_patterns:
            files = list(Path(".").glob(pattern))
            if files:
                print(f"   ✅ Found: {pattern} ({len(files)} file(s))")
            else:
                print(f"   ❌ Missing: {pattern}")
                all_exist = False
        
        return all_exist
    
    def run_pipeline(self):
        """Run complete test pipeline"""
        self.print_banner()
        
        # Step 1: Scrape test data
        step1_success = self.run_step(
            "1. SCRAPE TEST DATA",
            "python test_pipeline_scrape.py",
            "Scrape 2020-2025 polls data from Sports-Reference"
        )
        
        if not step1_success:
            print("\n❌ Test pipeline failed at Step 1: Scraping")
            print("Cannot proceed without data. Please fix errors and retry.")
            return False
        
        # Verify scrape outputs
        if not self.check_files_exist(
            "scrape",
            ["data/polls_test/raw/*/polls_*.csv"]
        ):
            print("\n⚠️  Expected files not found after scraping")
            print("Continuing anyway...")
        
        # Step 2: Explore data structure
        step2_success = self.run_step(
            "2. EXPLORE DATA STRUCTURE",
            "python explore_raw_data.py --raw-dir data/polls_test/raw --generate-mapping",
            "Analyze structure and generate team name mapping"
        )
        
        if not step2_success:
            print("\n⚠️  Exploration failed, but continuing...")
        
        # Step 3: Clean data
        step3_success = self.run_step(
            "3. CLEAN AND STANDARDIZE DATA",
            "python clean_test_data.py",
            "Convert to long format and standardize"
        )
        
        if not step3_success:
            print("\n❌ Test pipeline failed at Step 3: Cleaning")
            print("Cannot validate without cleaned data.")
            return False
        
        # Verify cleaning outputs
        if not self.check_files_exist(
            "cleaning",
            ["data/polls_test/combined/test_all_years_combined.csv"]
        ):
            print("\n❌ Combined dataset not found")
            return False
        
        # Step 4: Generate summary statistics
        print("\n" + "="*80)
        print("STEP: 4. GENERATE SUMMARY STATISTICS")
        print("="*80)
        
        try:
            import pandas as pd
            
            df = pd.read_csv("data/polls_test/combined/test_all_years_combined.csv")
            
            print(f"\n📊 Dataset Summary:")
            print(f"   Total records: {len(df):,}")
            print(f"   Years: {df['year'].min()}-{df['year'].max()}")
            print(f"   Unique teams: {df['team_name'].nunique()}")
            print(f"   Weeks per year:")
            weeks_per_year = df.groupby('year')['week_number'].nunique()
            for year, count in weeks_per_year.items():
                print(f"      {year}: {count} weeks")
            
            print(f"\n🏀 Top 10 most frequently ranked teams:")
            top_teams = df['team_name'].value_counts().head(10)
            for i, (team, count) in enumerate(top_teams.items(), 1):
                print(f"      {i:2d}. {team}: {count} times")
            
            print(f"\n✅ Summary statistics generated successfully")
            
            self.results["steps_completed"].append({
                "step": "4. GENERATE SUMMARY STATISTICS",
                "success": True
            })
            
        except Exception as e:
            print(f"\n❌ Failed to generate statistics: {e}")
            self.results["steps_failed"].append({
                "step": "4. GENERATE SUMMARY STATISTICS",
                "error": str(e)
            })
        
        return True
    
    def generate_final_report(self, pipeline_success):
        """Generate final test report"""
        elapsed = datetime.now() - self.start_time
        elapsed_minutes = elapsed.total_seconds() / 60
        
        print("\n" + "="*80)
        print("FINAL TEST REPORT")
        print("="*80)
        
        print(f"\n⏱️  Total time: {elapsed_minutes:.1f} minutes")
        print(f"\n✅ Steps completed: {len(self.results['steps_completed'])}")
        print(f"❌ Steps failed: {len(self.results['steps_failed'])}")
        
        if self.results["steps_failed"]:
            print(f"\nFailed steps:")
            for failure in self.results["steps_failed"]:
                print(f"   - {failure['step']}")
        
        # Overall assessment
        print("\n" + "="*80)
        
        if pipeline_success and len(self.results["steps_failed"]) == 0:
            self.results["overall_success"] = True
            print("🎉 TEST PIPELINE SUCCESSFUL!")
            print("="*80)
            print("\n✅ All validation checks passed")
            print("\nYou're ready to proceed to the full 43-year scrape!")
            print("\nNext steps:")
            print("1. Review test data: data/polls_test/combined/test_all_years_combined.csv")
            print("2. Examine structure: data/polls_test/logs/data_exploration.json")
            print("3. Check team names: data/polls_test/reference/team_name_mappings_template.csv")
            print("4. When satisfied, run: python run_historical_scrape.py --yes")
            print("\n📚 See TEST_WORKFLOW_GUIDE.md for detailed next steps")
            
        else:
            print("⚠️  TEST PIPELINE COMPLETED WITH ISSUES")
            print("="*80)
            print("\nSome steps failed or had warnings.")
            print("Review the output above for details.")
            print("\nRecommendations:")
            print("1. Fix issues identified")
            print("2. Clean up test data: rm -rf data/polls_test")
            print("3. Re-run test: python run_complete_test.py")
            print("\nDo NOT proceed to full 43-year scrape until test passes.")
        
        # Save report
        report_file = self.test_dir / "logs" / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(report_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\n💾 Full report saved to: {report_file}")
        print("="*80)
        
        return self.results["overall_success"]
    
    def run(self):
        """Main execution"""
        pipeline_success = self.run_pipeline()
        final_success = self.generate_final_report(pipeline_success)
        
        return 0 if final_success else 1

def main():
    """Entry point"""
    print("Checking prerequisites...")
    
    # Check if required scripts exist
    required_scripts = [
        "test_pipeline_scrape.py",
        "explore_raw_data.py",
        "clean_test_data.py"
    ]
    
    missing_scripts = [s for s in required_scripts if not Path(s).exists()]
    
    if missing_scripts:
        print(f"\n❌ Missing required scripts: {missing_scripts}")
        print("Please ensure all test scripts are in the current directory.")
        return 1
    
    # Check Python packages
    try:
        import pandas
        import requests
    except ImportError as e:
        print(f"\n❌ Missing Python package: {e}")
        print("Install with: pip install pandas requests beautifulsoup4 lxml")
        return 1
    
    print("✅ All prerequisites met\n")
    
    # Run test
    runner = MasterTestRunner()
    return runner.run()

if __name__ == "__main__":
    exit(main())
