"""
Rebuild Script for Finance Modernization Platform

Regenerates all data and rebuilds the entire pipeline.
Use this when you need a fresh start or after making changes.

Usage:
    python scripts/rebuild_all.py
"""

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_command(cmd: str, cwd: Path = None) -> bool:
    """Run a shell command and return success status."""
    print(f"  Running: {cmd}")
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            cwd=cwd or PROJECT_ROOT,
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"  ❌ Error: {result.stderr[:200]}")
            return False
        return True
    except Exception as e:
        print(f"  ❌ Exception: {e}")
        return False


def main():
    print("=" * 60)
    print("FINANCE MODERNIZATION - REBUILD ALL")
    print("=" * 60)
    print()
    
    steps = []
    
    # Step 1: Generate seed data
    print("[1/4] Generating seed data...")
    success = run_command("python scripts/generate_seed_data.py")
    steps.append(("Generate Seed Data", success))
    print()
    
    # Step 2: Run dbt seed
    print("[2/4] Loading seeds into database...")
    success = run_command("dbt seed", cwd=PROJECT_ROOT / "dbt_project")
    steps.append(("dbt seed", success))
    print()
    
    # Step 3: Run dbt models
    print("[3/4] Running dbt models...")
    success = run_command("dbt run", cwd=PROJECT_ROOT / "dbt_project")
    steps.append(("dbt run", success))
    print()
    
    # Step 4: Run tests
    print("[4/4] Running tests...")
    success = run_command("python scripts/run_tests.py")
    steps.append(("Tests", success))
    print()
    
    # Summary
    print("=" * 60)
    print("REBUILD SUMMARY")
    print("=" * 60)
    
    all_passed = True
    for step_name, success in steps:
        status = "✅" if success else "❌"
        print(f"  {status} {step_name}")
        if not success:
            all_passed = False
    
    print("=" * 60)
    
    if all_passed:
        print("\n🎉 Rebuild complete! You can now run:")
        print("   streamlit run streamlit_app/app.py")
        return 0
    else:
        print("\n⚠️  Some steps failed. Check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())