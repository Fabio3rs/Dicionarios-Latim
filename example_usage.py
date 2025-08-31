#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Exemplo básico de uso do sistema de dicionários Latim-Português.
Demonstra consultas simples ao banco de dados lexicon.

Usage:
    python example_usage.py
"""

import os
import sys
import sqlite3
from typing import List, Dict, Any

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import Config

def connect_to_lexicon() -> sqlite3.Connection:
    """Connect to the lexicon database."""
    db_path = Config.get_path(os.path.join(Config.RESULTADOS_DIR, Config.DEFAULT_LEXICON_DB))
    
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        print("🔧 Please run the full pipeline first or check the path.")
        sys.exit(1)
    
    return sqlite3.connect(db_path)


def search_entries(conn: sqlite3.Connection, query: str) -> List[Dict[str, Any]]:
    """Search for entries using FTS5."""
    cursor = conn.cursor()
    
    # Simple FTS5 search
    cursor.execute("""
        SELECT e.id, e.morfologia, e.definicao, 
               GROUP_CONCAT(l.forma, ' / ') as lemas
        FROM entry_fts fts
        JOIN entry e ON e.rowid = fts.rowid
        LEFT JOIN lemma l ON l.entry_id = e.id
        WHERE entry_fts MATCH ?
        GROUP BY e.id
        LIMIT 10
    """, (query,))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'id': row[0],
            'morfologia': row[1],
            'definicao': row[2],
            'lemas': row[3]
        })
    
    return results


def print_results(results: List[Dict[str, Any]]):
    """Print search results in a nice format."""
    if not results:
        print("❌ Nenhum resultado encontrado.")
        return
    
    print(f"✅ Encontrados {len(results)} resultado(s):\n")
    
    for i, result in enumerate(results, 1):
        print(f"[{i}] ID: {result['id']}")
        print(f"    Lemas: {result['lemas'] or 'N/A'}")
        print(f"    Morfologia: {result['morfologia'] or 'N/A'}")
        print(f"    Definição: {(result['definicao'] or 'N/A')[:200]}...")
        print()


def main():
    """Main demonstration function."""
    print("🏛️  Exemplo de uso - Dicionários Latim-Português")
    print("="*60)
    
    # Connect to database
    try:
        conn = connect_to_lexicon()
        print("✅ Conectado ao banco de dados.")
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")
        return
    
    # Example searches
    example_queries = [
        "puella",      # Search for "puella" (girl)
        "amare",       # Search for "amare" (to love) 
        "bellum",      # Search for "bellum" (war)
        "casa",        # Search for "casa" (house)
    ]
    
    for query in example_queries:
        print(f"🔍 Buscando por: '{query}'")
        print("-" * 40)
        
        try:
            results = search_entries(conn, query)
            print_results(results)
        except Exception as e:
            print(f"❌ Erro na busca: {e}")
        
        print()
    
    # Interactive search
    print("🎯 Busca interativa (digite 'quit' para sair):")
    while True:
        try:
            query = input("\n🔍 Digite sua busca: ").strip()
            if query.lower() in ['quit', 'sair', 'exit']:
                break
            if not query:
                continue
                
            results = search_entries(conn, query)
            print_results(results)
            
        except KeyboardInterrupt:
            print("\n👋 Saindo...")
            break
        except Exception as e:
            print(f"❌ Erro: {e}")
    
    conn.close()
    print("✅ Conexão fechada. Obrigado!")


if __name__ == "__main__":
    main()