"""
Test Runner for Finance Modernization Platform

Runs all tests and generates a report.

Usage:
    python scripts/run_tests.py
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestResult:
    """Container for test results."""
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.message = ""
        self.details = ""
    
    def pass_test(self, message: str = "", details: str = ""):
        self.passed = True
        self.message = message
        self.details = details
    
    def fail_test(self, message: str, details: str = ""):
        self.passed = False
        self.message = message
        self.details = details


def test_data_files() -> TestResult:
    """Test 1: Verify AS400 data files exist."""
    result = TestResult("AS400 Data Files")
    
    try:
        files_dir = PROJECT_ROOT / "data" / "mock_as400" / "physical_files"
        files = list(files_dir.glob("*.txt"))
        
        required_files = ["CUSMAS.txt", "ARMAS.txt", "PAYTRAN.txt", "GLJRN.txt"]
        found_files = [f.name for f in files]
        
        missing = [f for f in required_files if f not in found_files]
        
        if missing:
            result.fail_test(f"Missing files: {missing}")
        else:
            result.pass_test(f"{len(files)} files found", ", ".join(found_files))
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_parser() -> TestResult:
    """Test 2: Verify AS400 parser works."""
    result = TestResult("AS400 Parser")
    
    try:
        from src.ingestion.as400_parser import AS400Parser
        from src.utils.config import PHYSICAL_FILES_DIR
        
        parser = AS400Parser(PHYSICAL_FILES_DIR)
        df = parser.parse_file("CUSMAS.txt")
        
        if len(df) == 0:
            result.fail_test("Parser returned empty DataFrame")
        elif "customer_id" not in df.columns:
            result.fail_test("Missing customer_id column")
        else:
            result.pass_test(f"Parsed {len(df)} records", f"Columns: {len(df.columns)}")
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_date_conversion() -> TestResult:
    """Test 3: Verify CYYMMDD date conversion."""
    result = TestResult("Date Conversion (CYYMMDD)")
    
    try:
        from src.utils.date_utils import cyymmdd_to_date, date_to_cyymmdd
        from datetime import date
        
        # Test cases
        tests = [
            (1240115, date(2024, 1, 15)),
            (1230701, date(2023, 7, 1)),
            (1251231, date(2025, 12, 31)),
        ]
        
        for cyymmdd, expected in tests:
            converted = cyymmdd_to_date(cyymmdd)
            if converted != expected:
                result.fail_test(f"Expected {expected}, got {converted}")
                return result
            
            # Test reverse
            back = date_to_cyymmdd(converted)
            if back != cyymmdd:
                result.fail_test(f"Reverse conversion failed: {back} != {cyymmdd}")
                return result
        
        result.pass_test("All conversions correct", f"Tested {len(tests)} cases")
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_database() -> TestResult:
    """Test 4: Verify database connection and tables."""
    result = TestResult("Database Connection")
    
    try:
        import duckdb
        
        db_path = PROJECT_ROOT / "data" / "finance.duckdb"
        conn = duckdb.connect(str(db_path))
        
        # Check tables exist
        tables = conn.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema IN ('main_raw', 'main_staging', 'main_marts')
        """).fetchdf()
        
        conn.close()
        
        if len(tables) == 0:
            result.fail_test("No tables found in database")
        else:
            result.pass_test(f"{len(tables)} tables found", tables['table_name'].tolist()[:5])
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_dbt_models() -> TestResult:
    """Test 5: Verify dbt models are populated."""
    result = TestResult("dbt Models")
    
    try:
        import duckdb
        
        db_path = PROJECT_ROOT / "data" / "finance.duckdb"
        conn = duckdb.connect(str(db_path))
        
        # Check key metrics
        summary = conn.execute("""
            SELECT total_ar_balance, open_invoice_count 
            FROM main_marts.metrics_ar_summary
        """).fetchdf()
        
        if len(summary) == 0:
            result.fail_test("metrics_ar_summary is empty")
            return result
        
        ar_balance = summary['total_ar_balance'].iloc[0]
        invoice_count = summary['open_invoice_count'].iloc[0]
        
        conn.close()
        
        if ar_balance <= 0:
            result.fail_test(f"AR balance is {ar_balance}")
        else:
            result.pass_test(
                f"AR: ${ar_balance:,.2f}", 
                f"Open Invoices: {int(invoice_count)}"
            )
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_aging_distribution() -> TestResult:
    """Test 6: Verify aging buckets have data."""
    result = TestResult("Aging Distribution")
    
    try:
        import duckdb
        
        db_path = PROJECT_ROOT / "data" / "finance.duckdb"
        conn = duckdb.connect(str(db_path))
        
        aging = conn.execute("""
            SELECT aging_bucket, COUNT(*) as cnt
            FROM main_staging.stg_invoices
            WHERE current_balance > 0
            GROUP BY aging_bucket
        """).fetchdf()
        
        conn.close()
        
        if len(aging) <= 1:
            result.fail_test(f"Only {len(aging)} aging bucket(s) - data may need regeneration")
        else:
            buckets = aging['aging_bucket'].tolist()
            result.pass_test(f"{len(aging)} buckets", ", ".join(buckets))
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_risk_distribution() -> TestResult:
    """Test 7: Verify risk categories have data."""
    result = TestResult("Risk Distribution")
    
    try:
        import duckdb
        
        db_path = PROJECT_ROOT / "data" / "finance.duckdb"
        conn = duckdb.connect(str(db_path))
        
        risk = conn.execute("""
            SELECT risk_category, COUNT(*) as cnt
            FROM main_marts.dim_customers
            WHERE total_ar_balance > 0
            GROUP BY risk_category
        """).fetchdf()
        
        conn.close()
        
        categories = risk['risk_category'].tolist()
        
        if len(risk) <= 1:
            result.fail_test(f"Only {len(risk)} risk category - data may need regeneration")
        else:
            result.pass_test(f"{len(risk)} categories", ", ".join(categories))
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_ml_pipeline() -> TestResult:
    """Test 8: Verify ML models work."""
    result = TestResult("ML Pipeline")
    
    try:
        from src.ml.models.collection_scorer import CollectionPriorityScorer
        
        scorer = CollectionPriorityScorer()
        worklist = scorer.generate_worklist()
        
        if len(worklist) == 0:
            result.fail_test("Worklist is empty")
        else:
            top_customer = worklist.iloc[0]["customer_name"]
            top_score = worklist.iloc[0]["priority_score"]
            result.pass_test(
                f"{len(worklist)} accounts scored",
                f"Top: {top_customer} (Score: {top_score})"
            )
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_llm_agent() -> TestResult:
    """Test 9: Verify LLM agent works."""
    result = TestResult("LLM Agent (Groq)")
    
    try:
        from src.llm_agents.agents.ar_query_agent import ARQueryAgent
        
        agent = ARQueryAgent()
        mode = "API" if not agent.demo_mode else "Demo"
        
        query_result = agent.ask("What is total AR?")
        agent.close()
        
        if query_result.get("error"):
            result.fail_test(query_result["answer"][:50])
        elif not query_result.get("answer"):
            result.fail_test("No answer returned")
        else:
            result.pass_test(
                f"Mode: {mode}",
                query_result["answer"][:60] + "..."
            )
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def test_collections_agent() -> TestResult:
    """Test 10: Verify Collections agent works."""
    result = TestResult("Collections Agent")
    
    try:
        from src.llm_agents.agents.collections_agent import CollectionsAgent, CustomerAccount
        
        agent = CollectionsAgent()
        
        account = CustomerAccount(
            customer_id="TEST001",
            customer_name="Test Company",
            contact_name="John Doe",
            email="john@test.com",
            total_balance=50000.00,
            days_past_due=45,
            invoice_count=3,
            payment_history_score=65,
            segment="Mid-Market"
        )
        
        email = agent.generate_email(account)
        
        if not email.subject or not email.body:
            result.fail_test("Email generation failed")
        else:
            result.pass_test(
                f"Tone: {email.tone.name}",
                f"Subject: {email.subject[:40]}..."
            )
    except Exception as e:
        result.fail_test(str(e))
    
    return result


def run_all_tests():
    """Run all tests and print report."""
    print("=" * 70)
    print("FINANCE MODERNIZATION PLATFORM - TEST SUITE")
    print("=" * 70)
    print(f"Run Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    print()
    
    # Define all tests
    tests = [
        ("Data Files", test_data_files),
        ("AS400 Parser", test_parser),
        ("Date Conversion", test_date_conversion),
        ("Database", test_database),
        ("dbt Models", test_dbt_models),
        ("Aging Distribution", test_aging_distribution),
        ("Risk Distribution", test_risk_distribution),
        ("ML Pipeline", test_ml_pipeline),
        ("LLM Agent", test_llm_agent),
        ("Collections Agent", test_collections_agent),
    ]
    
    results = []
    
    for i, (name, test_func) in enumerate(tests, 1):
        print(f"[{i}/{len(tests)}] Testing {name}...", end=" ")
        try:
            result = test_func()
            results.append(result)
            
            if result.passed:
                print(f"✅ PASS - {result.message}")
                if result.details:
                    print(f"         {result.details}")
            else:
                print(f"❌ FAIL - {result.message}")
                if result.details:
                    print(f"         {result.details}")
        except Exception as e:
            result = TestResult(name)
            result.fail_test(str(e))
            results.append(result)
            print(f"❌ ERROR - {e}")
        print()
    
    # Summary
    passed = sum(1 for r in results if r.passed)
    failed = len(results) - passed
    
    print("=" * 70)
    print("TEST SUMMARY")
    print("=" * 70)
    print(f"Total:  {len(results)}")
    print(f"Passed: {passed} ✅")
    print(f"Failed: {failed} ❌")
    print("=" * 70)
    
    if failed == 0:
        print("\n🎉 ALL TESTS PASSED! Your project is ready!")
        return 0
    else:
        print(f"\n⚠️  {failed} test(s) failed. Review the errors above.")
        return 1


if __name__ == "__main__":
    exit_code = run_all_tests()
    sys.exit(exit_code)