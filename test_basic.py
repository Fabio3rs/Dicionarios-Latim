#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Basic tests for the Dicionários Latim-Português project.
These are minimal tests to ensure core functionality works.

Run with: python test_basic.py
Or with pytest (if installed): python -m pytest test_basic.py -v
"""

import os
import sys
import tempfile
import sqlite3
from unittest.mock import patch

# Try to import pytest, but don't fail if it's not available
try:
    import pytest
    PYTEST_AVAILABLE = True
except ImportError:
    PYTEST_AVAILABLE = False
    
    # Mock pytest.raises for compatibility
    class MockPytest:
        @staticmethod
        def raises(exception_type, match=None):
            class ContextManager:
                def __enter__(self):
                    return self
                def __exit__(self, exc_type, exc_val, exc_tb):
                    if exc_type is None:
                        raise AssertionError(f"Expected {exception_type.__name__} but no exception was raised")
                    return isinstance(exc_val, exception_type) and (match is None or match in str(exc_val))
            return ContextManager()
        
        @staticmethod
        def skip(reason):
            print(f"SKIPPED: {reason}")
            return
    
    pytest = MockPytest()

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config import Config


class TestConfig:
    """Test configuration management."""
    
    def test_config_paths(self):
        """Test that config generates valid paths."""
        path = Config.get_path("test_file.txt")
        assert isinstance(path, str)
        assert "test_file.txt" in path
    
    def test_openai_key_missing(self):
        """Test error when OpenAI key is missing."""
        with patch.dict(os.environ, {}, clear=True):
            # Clear the class variable too
            original_key = Config.OPENAI_API_KEY
            Config.OPENAI_API_KEY = None
            
            with pytest.raises(ValueError, match="OPENAI_API_KEY not found"):
                Config.get_openai_key()
            
            # Restore
            Config.OPENAI_API_KEY = original_key
    
    def test_openai_key_present(self):
        """Test successful key retrieval."""
        test_key = "sk-test123"
        with patch.dict(os.environ, {"OPENAI_API_KEY": test_key}):
            Config.OPENAI_API_KEY = test_key
            assert Config.get_openai_key() == test_key


class TestDatabase:
    """Test database operations."""
    
    def test_schema_creation(self):
        """Test that schema can be applied successfully."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        try:
            conn = sqlite3.connect(db_path)
            
            # Apply basic schema (simplified)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS entry (
                    id INTEGER PRIMARY KEY,
                    doc_name TEXT NOT NULL,
                    page_num INTEGER,
                    morfologia TEXT,
                    definicao TEXT
                )
            """)
            
            # Test insertion
            conn.execute("""
                INSERT INTO entry (doc_name, page_num, morfologia, definicao)
                VALUES (?, ?, ?, ?)
            """, ("test.pdf", 1, "subst. f.", "palavra de teste"))
            
            conn.commit()
            
            # Test retrieval
            cursor = conn.execute("SELECT * FROM entry WHERE id = 1")
            result = cursor.fetchone()
            
            assert result is not None
            assert result[1] == "test.pdf"
            assert result[4] == "palavra de teste"
            
            conn.close()
            
        finally:
            if os.path.exists(db_path):
                os.unlink(db_path)


class TestImports:
    """Test that all required modules can be imported."""
    
    def test_core_imports(self):
        """Test core Python imports work."""
        import sqlite3
        import json
        import re
        import os
        import sys
        import argparse
        
        # Basic sanity checks
        assert hasattr(sqlite3, 'connect')
        assert hasattr(json, 'dumps')
        assert hasattr(re, 'compile')
    
    def test_optional_imports(self):
        """Test optional imports with graceful fallbacks."""
        # These might not be available in all environments
        optional_modules = [
            'openai', 
            'pdf2image', 
            'pdfminer.high_level',
            'ollama',
            'unidecode'
        ]
        
        for module_name in optional_modules:
            try:
                __import__(module_name)
            except ImportError:
                # This is expected in some environments
                pytest.skip(f"Optional module {module_name} not available")


@patch('config.Config.get_openai_key')
def test_script_imports(mock_get_key):
    """Test that scripts can be imported without OpenAI key."""
    mock_get_key.return_value = "sk-test123"
    
    # Test that we can import scripts without errors
    # (This mainly tests syntax and import structure)
    scripts_to_test = [
        'export_for_diff',
        'ingest_normalized', 
        'query_lexicon'
    ]
    
    for script_name in scripts_to_test:
        try:
            __import__(f'scripts.{script_name}')
        except ImportError as e:
            if "No module named 'scripts" in str(e):
                # Expected if scripts aren't in Python path
                continue
            else:
                raise


if __name__ == "__main__":
    # Simple test runner if pytest isn't available
    import unittest
    
    print("Running basic tests...")
    
    # Test config
    try:
        config_test = TestConfig()
        config_test.test_config_paths()
        print("✅ Config paths test passed")
    except Exception as e:
        print(f"❌ Config test failed: {e}")
    
    # Test database
    try:
        db_test = TestDatabase()
        db_test.test_schema_creation()
        print("✅ Database test passed")
    except Exception as e:
        print(f"❌ Database test failed: {e}")
    
    # Test imports
    try:
        import_test = TestImports()
        import_test.test_core_imports()
        print("✅ Core imports test passed")
    except Exception as e:
        print(f"❌ Import test failed: {e}")
    
    print("✅ Basic tests completed")