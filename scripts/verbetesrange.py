import sqlite3
import re
import sys

def unicode_fix(s):
    if not s: return s
    try:
        return bytes(s, 'utf-8').decode('unicode_escape')
    except Exception:
        return s

def extrair_prefixo(label):
    m = re.match(r'^(n\d+)', label)
    return m.group(1) if m else None

def clean_field_list(lst):
    return [x.strip(' "\'\n') for x in lst if x and x.strip()]

def clean_punctuation(s):
    return (
        s.replace(' . ;', ';')
         .replace(' .', '.')
         .replace(' ;', ';')
         .replace('  ', ' ')
         .replace(';;', ';')
         .strip()
    )

def verbetes_range_formatado(dbpath, dicionario, inicio, fim, limite=50):
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()

    # 1. Busca os labels e lemas no range
    if fim is not None:
        c.execute("""
            SELECT label, valor FROM verbetes_util
            WHERE dicionario = ?
            AND campo = 'ontolex#writtenRep'
            AND valor COLLATE NOCASE BETWEEN ? AND ?
            ORDER BY valor
            LIMIT ?
        """, (dicionario, inicio, fim, limite))
    else:
        c.execute("""
            SELECT label, valor FROM verbetes_util
            WHERE dicionario = ?
            AND campo = 'ontolex#writtenRep'
            AND valor COLLATE NOCASE >= ?
            ORDER BY valor
            LIMIT ?
        """, (dicionario, inicio, limite))
    results = c.fetchall()
    print(f"Total de lemas encontrados no range: {len(results)}\n")

    for label, lema in results:
        prefixo = extrair_prefixo(label)
        if not prefixo:
            continue
        # 2. Busca todos os campos do verbete pelo prefixo
        c.execute("""
            SELECT campo, valor FROM verbetes_util
            WHERE label LIKE ?
        """, (f"{prefixo}%",))
        campos = {}
        for campo, valor in c.fetchall():
            campos.setdefault(campo, []).append(unicode_fix(valor))

        print(f"Dicionário: {dicionario}")
        print("Lema:", ', '.join(clean_field_list(campos.get('ontolex#writtenRep', ['(não encontrado)']))))

        defs_pt = clean_field_list(
            [v.replace('@pt', '') for v in campos.get('skos#definition', []) if v.endswith('@pt')]
        )
        defs_la = clean_field_list(
            [v.replace('@la', '') for v in campos.get('skos#definition', []) if v.endswith('@la')]
        )
        pt_out = clean_punctuation('; '.join(defs_pt)) if defs_pt else "(não disponível)"
        la_out = clean_punctuation('; '.join(defs_la)) if defs_la else "(não disponível)"

        print("Definição (PT):", pt_out)
        print("Definição (LA):", la_out)

        notas = clean_field_list(campos.get('skos#note', []) + campos.get('lexinfo#note', []))
        notas_out = clean_punctuation('; '.join(notas)) if notas else "(nenhuma)"
        print("Notas:", notas_out)

        exemplos = clean_field_list(campos.get('lexicog#usageExample', []))
        if exemplos:
            print("Exemplo(s):")
            for e in exemplos:
                print(f"  - {e}")
        print('-'*40)
    conn.close()

# ---------- Exemplo de uso -----------
if __name__ == "__main__":
    # Troque aqui os parâmetros conforme necessário
    dbfile = 'dicionarios_unificados.sqlite'
    dicionario = 'Cardoso'
    inicio = 'velle'
    fim = None
    limite = 30

    verbetes_range_formatado(dbfile, dicionario, inicio, fim, limite)
