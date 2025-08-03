import sqlite3
import re
import os

# 1. Criação do banco e dos índices
def criar_banco(path):
    conn = sqlite3.connect(path)
    c = conn.cursor()
    c.execute("""
    CREATE TABLE IF NOT EXISTS dicionario (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dicionario TEXT,
        sujeito_uri TEXT,
        tipo TEXT,
        label TEXT,
        campo TEXT,
        valor TEXT,
        outros_uris TEXT
    )
    """)
    # Índices para pesquisa rápida
    c.execute("CREATE INDEX IF NOT EXISTS idx_dicionario ON dicionario (dicionario)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_label ON dicionario (label)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_valor ON dicionario (valor)")
    conn.commit()
    return conn

# 2. Extração de triple do formato N-Triples
def extrair_triple(linha):
    # Regex simples para <S> <P> <O> .
    m = re.match(r'\s*<([^>]+)>\s+<([^>]+)>\s+(.+?)\s*\.\s*$', linha)
    if not m:
        return None
    s, p, o = m.groups()
    # Verifica se o objeto é literal ou um outro URI
    if o.startswith('<') and o.endswith('>'):
        o_valor = None
        o_uri = o.strip('<>')
    else:
        # Tira aspas se houver
        o_valor = o.strip('"')
        o_uri = None
    return s, p, o_valor, o_uri

# 3. Inferir nome do dicionário pela URI
def inferir_dicionario(uri):
    if "Cardoso" in uri:
        return "Cardoso"
    elif "Fonseca" in uri:
        return "Fonseca"
    elif "Velez" in uri:
        return "Velez"
    else:
        return "Outro"

# 4. Opcional: tentar obter um label (lemma) do sujeito URI
def extrair_label(uri):
    # Exemplo: terminações após a última barra ou hash
    return uri.split('/')[-1].split('#')[-1]

# 5. Carregar cada .nt para o banco
def carregar_nt_para_sqlite(path_nt, conn):
    base = os.path.basename(path_nt)
    print(f"Iniciando ingestão de {base} ...")
    with open(path_nt, 'r', encoding='utf-8') as f:
        batch = []
        for linha in f:
            if not linha.strip() or not linha.strip().endswith('.'):
                continue
            t = extrair_triple(linha)
            if not t: continue
            s, p, o_valor, o_uri = t
            dicionario = inferir_dicionario(s)
            tipo = p.split('/')[-1]  # só o predicado final
            label = extrair_label(s)
            campo = tipo
            valor = o_valor
            outros_uris = o_uri
            batch.append((dicionario, s, tipo, label, campo, valor, outros_uris))
            # Salva em batches de 5000 para performance
            if len(batch) >= 5000:
                conn.executemany(
                    "INSERT INTO dicionario (dicionario, sujeito_uri, tipo, label, campo, valor, outros_uris) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    batch)
                conn.commit()
                batch = []
        # Resto
        if batch:
            conn.executemany(
                "INSERT INTO dicionario (dicionario, sujeito_uri, tipo, label, campo, valor, outros_uris) VALUES (?, ?, ?, ?, ?, ?, ?)",
                batch)
            conn.commit()
    print(f"{base} finalizado.")

# 6. MAIN: rodar tudo
if __name__ == "__main__":
    arquivos = [
        ('Cardoso.nt', 'Cardoso'),
        ('Fonseca.nt', 'Fonseca'),
        ('Velez.nt', 'Velez'),
    ]
    dbpath = "dicionarios_unificados.sqlite"
    conn = criar_banco(dbpath)
    for path_nt, _ in arquivos:
        if os.path.exists(path_nt):
            carregar_nt_para_sqlite(path_nt, conn)
        else:
            print(f"Arquivo não encontrado: {path_nt}")
    conn.close()
    print(f"Pronto! Banco salvo em {dbpath}")


