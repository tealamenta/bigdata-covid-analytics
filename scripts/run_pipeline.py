"""COVID-19 Analytics Pipeline - Run All In Once."""

import sys
import time
from pathlib import Path
from datetime import datetime

def print_header(title):
    print("\n" + "=" * 60)
    print(f"  {title}")
    print("=" * 60)

def print_step(step, total, name):
    print(f"\n[{step}/{total}] {name}...")
    print("-" * 40)

def main():
    print_header("COVID-19 ANALYTICS PIPELINE")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    start_time = time.time()
    results = {}

    # Step 1: Ingestion France
    print_step(1, 5, "Ingestion France")
    try:
        from src.ingestion.france_covid import FranceCovidIngestion
        ingestion = FranceCovidIngestion()
        results['ingestion_france'] = ingestion.run()
        print("OK - France ingestion completed" if results['ingestion_france'] else "FAILED")
    except Exception as e:
        print(f"ERROR: {e}")
        results['ingestion_france'] = False

    # Step 2: Ingestion Colombia
    print_step(2, 5, "Ingestion Colombia")
    try:
        from src.ingestion.colombia_covid import ColombiaCovidIngestion
        ingestion = ColombiaCovidIngestion()
        results['ingestion_colombia'] = ingestion.run()
        print("OK - Colombia ingestion completed" if results['ingestion_colombia'] else "FAILED")
    except Exception as e:
        print(f"ERROR: {e}")
        results['ingestion_colombia'] = False

    # Step 3: Formatting France
    print_step(3, 5, "Formatting France (Spark)")
    try:
        from src.formatting.format_france import FranceCovidFormatter
        formatter = FranceCovidFormatter()
        results['formatting_france'] = formatter.run()
        print("OK - France formatting completed" if results['formatting_france'] else "FAILED")
    except Exception as e:
        print(f"ERROR: {e}")
        results['formatting_france'] = False

    # Step 4: Formatting Colombia
    print_step(4, 5, "Formatting Colombia (Spark)")
    try:
        from src.formatting.format_colombia import ColombiaCovidFormatter
        formatter = ColombiaCovidFormatter()
        results['formatting_colombia'] = formatter.run()
        print("OK - Colombia formatting completed" if results['formatting_colombia'] else "FAILED")
    except Exception as e:
        print(f"ERROR: {e}")
        results['formatting_colombia'] = False

    # Step 5: Combination
    print_step(5, 5, "Combination")
    try:
        from src.combination.combine_data import CovidDataCombiner
        combiner = CovidDataCombiner()
        results['combination'] = combiner.run()
        print("OK - Combination completed" if results['combination'] else "FAILED")
    except Exception as e:
        print(f"ERROR: {e}")
        results['combination'] = False

    # Summary
    elapsed = time.time() - start_time
    
    print_header("PIPELINE SUMMARY")
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total time: {elapsed:.2f} seconds\n")
    
    print("Results:")
    all_success = True
    for step, success in results.items():
        status = "[OK]" if success else "[FAILED]"
        print(f"  {status} {step}")
        if not success:
            all_success = False
    
    print()
    if all_success:
        print("SUCCESS - PIPELINE COMPLETED")
        print("\nOutput files:")
        print("  - data/raw/france/<date>/covid_hospitalizations.csv")
        print("  - data/raw/colombia/<date>/covid_cases.csv")
        print("  - data/formatted/france/covid_hospitalizations.parquet/")
        print("  - data/formatted/colombia/covid_cases.parquet/")
        print("  - data/usage/covid_comparison/daily_comparison.csv")
        print("  - data/usage/covid_comparison/summary_stats.csv")
    else:
        print("WARNING - Pipeline completed with errors")
    
    return all_success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
