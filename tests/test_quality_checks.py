import pytest
from scripts.quality_checks.validate_data import calculate_quality_score

def test_quality_score_calculation():
    perfect = {"null_checks": {"null_violations": 0}, "referential_integrity": {"orphan_records": 0}}
    assert calculate_quality_score(perfect) == 100.0
    
    bad = {"null_checks": {"null_violations": 0}, "referential_integrity": {"orphan_records": 10}}
    assert calculate_quality_score(bad) == 50.0 # 100 - (10 * 5)
