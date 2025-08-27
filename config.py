#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration management for Dicionários de Latim para Português.
Handles environment variables, default paths, and common settings.
"""

import os
from typing import Optional


class Config:
    """Central configuration class for the project."""
    
    # API Configuration
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    
    # Default file paths (can be overridden)
    DEFAULT_PDF_PATH = "Dicionário - Ernesto Faria.pdf"
    DEFAULT_OCR_DB = "ocr_results.db"
    DEFAULT_LS_DB = "ls_dict.sqlite"
    DEFAULT_UNIFIED_DB = "dicionarios_unificados.sqlite"
    DEFAULT_LEXICON_DB = "lexicon.db"
    DEFAULT_CHUNKS_JSON = "chunks_faria.json"
    DEFAULT_NORMALIZED_JSON = "normalized_results.json"
    DEFAULT_SCHEMA_SQL = "scripts/schema_normalizado.sql"
    
    # Processing configuration
    DEFAULT_START_PAGE = 7
    DEFAULT_BATCH_SIZE = 5000
    
    # Output directories
    RESULTADOS_DIR = "resultados"
    DICIONARIOS_DIR = "dicionarios"
    SCRIPTS_DIR = "scripts"
    
    @classmethod
    def get_openai_key(cls) -> str:
        """Get OpenAI API key, raising error if not found."""
        key = cls.OPENAI_API_KEY
        if not key:
            raise ValueError(
                "OPENAI_API_KEY not found in environment variables. "
                "Please set it with: export OPENAI_API_KEY='your_key_here'"
            )
        return key
    
    @classmethod
    def get_path(cls, relative_path: str) -> str:
        """Get absolute path relative to project root."""
        return os.path.join(os.path.dirname(os.path.dirname(__file__)), relative_path)
    
    @classmethod
    def ensure_dir(cls, path: str) -> str:
        """Ensure directory exists, create if not."""
        os.makedirs(path, exist_ok=True)
        return path


# Common abbreviations used throughout the project
COMMON_ABBREVIATIONS = {
    "abl.": "ablativo",
    "abs.": "absoluto",
    "absl.": "absolutamente", 
    "acus.": "acusativo",
    "adj.": "adjetivo",
    "adv.": "advérbio",
    "cf.": "confere",
    "comp.": "comparativo",
    "conj.": "conjunção",
    "dat.": "dativo",
    "dem.": "demonstrativo",
    "dep.": "deponente",
    "dim.": "diminutivo",
    "f.": "feminino",
    "m.": "masculino",
    "n.": "neutro",
    "pl.": "plural",
    "sing.": "singular",
    "subst.": "substantivo",
    "v.": "verbo",
}

# Logging configuration
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()