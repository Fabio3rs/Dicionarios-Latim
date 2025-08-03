import sqlite3
import re

def unicode_fix(s):
    if not s: return s
    try:
        return bytes(s, 'utf-8').decode('unicode_escape')
    except Exception:
        return s

def extrair_prefixo(label):
    m = re.match(r'^(n\d+)', label)
    return m.group(1) if m else None

def coletar_verbetes(dbpath, max_verbetes=10):
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    c.execute("""
        SELECT DISTINCT dicionario, label
        FROM verbetes_util
        WHERE campo = 'ontolex#writtenRep'
        LIMIT ?
    """, (max_verbetes,))
    entradas = c.fetchall()

    for dicionario, label in entradas:
        prefixo = extrair_prefixo(label)
        if not prefixo:
            continue
        c.execute("""
            SELECT campo, valor FROM verbetes_util
            WHERE label LIKE ?
        """, (f"{prefixo}%",))
        campos = {}
        for campo, valor in c.fetchall():
            campos.setdefault(campo, []).append(unicode_fix(valor))

        print(f"Dicionário: {dicionario}")
        print("Lema:", ', '.join(campos.get('ontolex#writtenRep', ['(não encontrado)'])))
        defs_pt = [v for v in campos.get('skos#definition', []) if v.endswith('@pt')]
        defs_la = [v for v in campos.get('skos#definition', []) if v.endswith('@la')]
        print("Definição (PT):", '; '.join(defs_pt) if defs_pt else "(não disponível)")
        print("Definição (LA):", '; '.join(defs_la) if defs_la else "(não disponível)")
        notas = campos.get('skos#note', []) + campos.get('lexinfo#note', [])
        print("Notas:", '; '.join(notas) if notas else "(nenhuma)")
        exemplos = campos.get('lexicog#usageExample', [])
        if exemplos:
            print("Exemplo(s):")
            for e in exemplos:
                print(f"  - {e}")
        print('-'*40)
    conn.close()

# Uso:
coletar_verbetes('dicionarios_unificados.sqlite', max_verbetes=30)
