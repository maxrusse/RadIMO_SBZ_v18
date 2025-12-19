"""
Test script to verify Excel file validation functionality
"""
import pandas as pd
import os
import tempfile
from datetime import datetime

# Import validation function
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Create test Excel files with various errors
test_cases = [
    {
        "name": "Valid file",
        "data": {
            "PPL": ["Worker1 (W1)", "Worker2 (W2)"],
            "TIME": ["08:00-16:00", "09:00-17:00"],
            "Normal": [1, 2],
            "Notfall": [1, 0],
            "Modifier": [1.0, 1.2]
        },
        "should_pass": True
    },
    {
        "name": "Invalid time format - dots instead of colons",
        "data": {
            "PPL": ["Worker1 (W1)"],
            "TIME": ["11.15-16.00"],
            "Normal": [1]
        },
        "should_pass": False,
        "expected_error": "Verwenden Sie ':' statt '.'"
    },
    {
        "name": "Invalid time format - missing dash",
        "data": {
            "PPL": ["Worker1 (W1)"],
            "TIME": ["11:15"],
            "Normal": [1]
        },
        "should_pass": False,
        "expected_error": "Format muss 'HH:MM-HH:MM' sein"
    },
    {
        "name": "Missing required column TIME",
        "data": {
            "PPL": ["Worker1 (W1)"],
            "Normal": [1]
        },
        "should_pass": False,
        "expected_error": "Fehlende Spalten: TIME"
    },
    {
        "name": "Empty PPL cell",
        "data": {
            "PPL": ["Worker1 (W1)", None],
            "TIME": ["08:00-16:00", "09:00-17:00"],
            "Normal": [1, 1]
        },
        "should_pass": False,
        "expected_error": "enthält leere Zellen"
    },
    {
        "name": "Invalid skill value (3)",
        "data": {
            "PPL": ["Worker1 (W1)"],
            "TIME": ["08:00-16:00"],
            "Normal": [3]
        },
        "should_pass": False,
        "expected_error": "Wert muss 0, 1 oder 2 sein"
    },
    {
        "name": "Invalid hour (25)",
        "data": {
            "PPL": ["Worker1 (W1)"],
            "TIME": ["25:00-16:00"],
            "Normal": [1]
        },
        "should_pass": False,
        "expected_error": "muss zwischen 0-23 sein"
    }
]

def create_test_file(data):
    """Create a temporary Excel file with test data"""
    df = pd.DataFrame(data)
    temp_fd, temp_path = tempfile.mkstemp(suffix='.xlsx')
    os.close(temp_fd)

    with pd.ExcelWriter(temp_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Tabelle1', index=False)

    return temp_path

def run_tests():
    """Run all validation tests"""
    print("=" * 80)
    print("EXCEL FILE VALIDATION TESTS")
    print("=" * 80)
    print()

    # Import validation function
    from app import validate_uploaded_file

    passed = 0
    failed = 0

    for i, test_case in enumerate(test_cases, 1):
        print(f"Test {i}: {test_case['name']}")
        print("-" * 80)

        # Create test file
        temp_path = create_test_file(test_case['data'])

        try:
            # Run validation
            is_valid, error_msg = validate_uploaded_file(temp_path)

            # Check result
            if is_valid == test_case['should_pass']:
                if is_valid:
                    print("✓ PASS - File correctly validated as valid")
                    passed += 1
                else:
                    # Check if error message contains expected error
                    if 'expected_error' in test_case and test_case['expected_error'] in error_msg:
                        print(f"✓ PASS - File correctly rejected with expected error")
                        print(f"  Error: {error_msg[:100]}...")
                        passed += 1
                    elif 'expected_error' in test_case:
                        print(f"✗ FAIL - File rejected but with unexpected error")
                        print(f"  Expected: {test_case['expected_error']}")
                        print(f"  Got: {error_msg}")
                        failed += 1
                    else:
                        print(f"✓ PASS - File correctly rejected")
                        print(f"  Error: {error_msg[:100]}...")
                        passed += 1
            else:
                print(f"✗ FAIL - Expected {'PASS' if test_case['should_pass'] else 'FAIL'}, got {'PASS' if is_valid else 'FAIL'}")
                if not is_valid:
                    print(f"  Error: {error_msg}")
                failed += 1

        except Exception as e:
            print(f"✗ FAIL - Exception: {str(e)}")
            failed += 1
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.remove(temp_path)

        print()

    print("=" * 80)
    print(f"RESULTS: {passed} passed, {failed} failed out of {passed + failed} tests")
    print("=" * 80)

    return failed == 0

if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
