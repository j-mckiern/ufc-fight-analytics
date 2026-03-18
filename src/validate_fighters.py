#!/usr/bin/env python3
"""
Validate fighter ID coverage across CSV files.

Checks that all fighter_ids in fight_stats.csv exist in fighters.csv
and reports coverage statistics.
"""

import csv
import sys
from datetime import datetime
from pathlib import Path


def load_ids(filepath: Path, column: str) -> set[str]:
    """Load IDs from a CSV file."""
    ids = set()
    if filepath.exists():
        try:
            with open(filepath, 'r', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if column in row:
                        ids.add(row[column])
        except Exception as e:
            print(f"[!] Error reading {filepath}: {e}")
            return ids
    return ids


def main():
    data_dir = Path(__file__).resolve().parent.parent / "data"
    
    # Use today's date by default, but check if it exists
    today = datetime.now().strftime("%Y-%m-%d")
    target_dir = data_dir / today
    
    if not target_dir.exists():
        # List available directories
        dirs = sorted([d.name for d in data_dir.iterdir() if d.is_dir()])
        if dirs:
            print(f"[*] Data directory for today ({today}) not found.")
            print(f"    Available dates: {', '.join(dirs[-5:])}")  # Show last 5
            print(f"\n[*] Using most recent: {dirs[-1]}")
            target_dir = data_dir / dirs[-1]
        else:
            print(f"[!] No data directory found. Have you run the scrapers?")
            sys.exit(1)
    
    fight_stats_file = target_dir / "fight_stats.csv"
    fighters_file = target_dir / "fighters.csv"
    
    print(f"\nValidating fighter coverage in {target_dir}/")
    print("=" * 60)
    
    if not fight_stats_file.exists():
        print(f"[!] fight_stats.csv not found in {target_dir}")
        return
    
    if not fighters_file.exists():
        print(f"[!] fighters.csv not found in {target_dir}")
        return
    
    # Load IDs
    fighter_ids_in_fights = load_ids(fight_stats_file, "fighter_id")
    fighter_ids_in_db = load_ids(fighters_file, "fighter_id")
    
    print(f"\nFighter ID Count:")
    print(f"  In fight_stats.csv:  {len(fighter_ids_in_fights)} unique fighters")
    print(f"  In fighters.csv:     {len(fighter_ids_in_db)} unique fighters")
    
    missing = fighter_ids_in_fights - fighter_ids_in_db
    coverage = (len(fighter_ids_in_db) / len(fighter_ids_in_fights) * 100) if fighter_ids_in_fights else 0
    
    print(f"\nCoverage:")
    print(f"  Missing from fighters.csv: {len(missing)} ({100 - coverage:.1f}%)")
    print(f"  Coverage Rate: {coverage:.1f}% ✓")
    
    # Report
    if missing:
        print(f"\n[!] Missing {len(missing)} fighter(s):")
        for fid in sorted(missing)[:20]:  # Show first 20
            print(f"      {fid}")
        if len(missing) > 20:
            print(f"      ... and {len(missing) - 20} more")
        
        # Save to file for reference
        missing_file = target_dir / "missing_fighters.csv"
        try:
            with open(missing_file, 'w', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=["fighter_id"])
                writer.writeheader()
                for fid in sorted(missing):
                    writer.writerow({"fighter_id": fid})
            print(f"\n[*] Full list saved to: {missing_file}")
        except Exception as e:
            print(f"[!] Error saving missing fighters list: {e}")
    else:
        print(f"\n[✓] All fighters have stats data!")
    
    # Check for failed fighters
    failed_file = target_dir / "failed_fighters.csv"
    if failed_file.exists():
        failed_count = sum(1 for _ in csv.DictReader(open(failed_file))) - 1  # -1 for header
        print(f"\n[*] Note: {failed_count} fighters failed to scrape (see failed_fighters.csv)")
    
    print("\n" + "=" * 60)
    if coverage >= 95:
        print("[✓] Coverage is excellent (>= 95%)")
    elif coverage >= 90:
        print("[~] Coverage is good (>= 90%)")
    elif coverage >= 80:
        print("[!] Coverage could be improved (< 90%)")
    else:
        print("[!] Coverage is low (< 80%). Check logs for errors.")


if __name__ == "__main__":
    main()
