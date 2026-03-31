import pytest
import pandas as pd
from sqlalchemy import text
from unittest.mock import MagicMock
from scripts.ingestion.ingest_to_staging import bulk_insert_data

def test_bulk_insert_data():
    # Mock connection and cursor
    mock_conn = MagicMock()
    df = pd.DataFrame({'col1': [1,2], 'col2': ['a','b']})
    
    rows = bulk_insert_data(df, 'test_table', mock_conn)
    assert rows == 2
    mock_conn.execute.assert_called()
