import sqlite3
import re


def unicode_fix(s):
    if not s:
        return s
    try:
        return bytes(s, "utf-8").decode("unicode_escape")
    except Exception:
        return s


def extrair_prefixo(label):
    m = re.match(r"^(n\d+)", label)
    return m.group(1) if m else None


def clean_field_list(lst):
    return [x.strip(" \"'\n") for x in lst if x and x.strip()]


def clean_punctuation(s):
    return (
        s.replace(" . ;", ";")
        .replace(" .", ".")
        .replace(" ;", ";")
        .replace("  ", " ")
        .replace(";;", ";")
        .strip()
    )


def verbetes_lista_formatado(dbpath, dicionario, lemas):
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    lemas_query = ",".join(["?"] * len(lemas))

    # 1. Busca os labels e lemas da lista
    if dicionario is not None:
        c.execute(
            f"""
            SELECT label, valor FROM verbetes_util
            WHERE dicionario = ?
            AND campo = 'ontolex#writtenRep'
            AND lower(valor) IN ({lemas_query})
            ORDER BY valor
        """,
            [dicionario] + [l.lower() for l in lemas],
        )
    else:
        c.execute(
            f"""
            SELECT label, valor FROM verbetes_util
            WHERE campo = 'ontolex#writtenRep'
            AND lower(valor) IN ({lemas_query})
            ORDER BY valor
        """,
            [l.lower() for l in lemas],
        )
    results = c.fetchall()
    print(f"Total de lemas encontrados na lista: {len(results)}\n")

    for label, lema in results:
        prefixo = extrair_prefixo(label)
        if not prefixo:
            continue
        # 2. Busca todos os campos do verbete pelo prefixo
        c.execute(
            """
            SELECT campo, valor FROM verbetes_util
            WHERE label LIKE ?
        """,
            (f"{prefixo}%",),
        )
        campos = {}
        for campo, valor in c.fetchall():
            campos.setdefault(campo, []).append(unicode_fix(valor))

        print(f"Dicionário: {dicionario}")
        print(
            "Lema:",
            ", ".join(
                clean_field_list(campos.get("ontolex#writtenRep", ["(não encontrado)"]))
            ),
        )

        defs_pt = clean_field_list(
            [
                v.replace("@pt", "")
                for v in campos.get("skos#definition", [])
                if v.endswith("@pt")
            ]
        )
        defs_la = clean_field_list(
            [
                v.replace("@la", "")
                for v in campos.get("skos#definition", [])
                if v.endswith("@la")
            ]
        )
        pt_out = (
            clean_punctuation("; ".join(defs_pt)) if defs_pt else "(não disponível)"
        )
        la_out = (
            clean_punctuation("; ".join(defs_la)) if defs_la else "(não disponível)"
        )

        print("Definição (PT):", pt_out)
        print("Definição (LA):", la_out)

        notas = clean_field_list(
            campos.get("skos#note", []) + campos.get("lexinfo#note", [])
        )
        notas_out = clean_punctuation("; ".join(notas)) if notas else "(nenhuma)"
        print("Notas:", notas_out)

        exemplos = clean_field_list(campos.get("lexicog#usageExample", []))
        if exemplos:
            print("Exemplo(s):")
            for e in exemplos:
                print(f"  - {e}")
        print("-" * 40)
    conn.close()


def verbetes_lista_todos_dicionarios(dbpath, lemas):
    conn = sqlite3.connect(dbpath)
    c = conn.cursor()
    encontrados = []
    for lema in lemas:
        # Busca tanto pelo lema exato quanto pelo lema no início (seguido de vírgula)
        c.execute(
            """
            SELECT dicionario, label, valor FROM verbetes_util
            WHERE campo = 'ontolex#writtenRep'
            AND (lower(valor) = ? OR lower(valor) LIKE ?)
            ORDER BY dicionario, valor
        """,
            (lema.lower(), f"{lema.lower()},%"),
        )
        results = c.fetchall()
        for dicionario, label, valor in results:
            prefixo = extrair_prefixo(label)
            if not prefixo:
                continue
            c.execute(
                """
                SELECT campo, valor FROM verbetes_util
                WHERE label LIKE ?
            """,
                (f"{prefixo}%",),
            )
            campos = {}
            for campo, valor in c.fetchall():
                campos.setdefault(campo, []).append(unicode_fix(valor))
            encontrados.append(
                {"lema": valor.strip(), "dicionario": dicionario, "campos": campos}
            )
    conn.close()
    return encontrados

    # Mostra resultados
    for (lema, dicionario), campos in encontrados.items():
        print(f"Dicionário: {dicionario}")
        print(
            "Lema:",
            ", ".join(
                clean_field_list(campos.get("ontolex#writtenRep", ["(não encontrado)"]))
            ),
        )

        defs_pt = clean_field_list(
            [
                v.replace("@pt", "")
                for v in campos.get("skos#definition", [])
                if v.endswith("@pt")
            ]
        )
        defs_la = clean_field_list(
            [
                v.replace("@la", "")
                for v in campos.get("skos#definition", [])
                if v.endswith("@la")
            ]
        )
        pt_out = (
            clean_punctuation("; ".join(defs_pt)) if defs_pt else "(não disponível)"
        )
        la_out = (
            clean_punctuation("; ".join(defs_la)) if defs_la else "(não disponível)"
        )

        print("Definição (PT):", pt_out)
        print("Definição (LA):", la_out)

        notas = clean_field_list(
            campos.get("skos#note", []) + campos.get("lexinfo#note", [])
        )
        notas_out = clean_punctuation("; ".join(notas)) if notas else "(nenhuma)"
        print("Notas:", notas_out)

        exemplos = clean_field_list(campos.get("lexicog#usageExample", []))
        if exemplos:
            print("Exemplo(s):")
            for e in exemplos:
                print(f"  - {e}")
        print("-" * 40)
    conn.close()

    return encontrados


def busca_todos_os_lemas(dbfile, termo, dicionarios=None, campos=None):
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    termo_lower = termo.lower()
    campos = campos or ["ontolex#writtenRep", "skos#definition", "rdf-schema#label"]
    campos_query = "('" + "', '".join(campos) + "')"
    params = []
    base_query = f"""
        SELECT dicionario, label, campo, valor
        FROM verbetes_util
        WHERE campo IN {campos_query}
          AND lower(valor) LIKE ?
    """
    params.append(f"%{termo_lower}%")
    if dicionarios:
        dics_query = "('" + "', '".join(dicionarios) + "')"
        base_query += f" AND dicionario IN {dics_query}"
    base_query += " ORDER BY dicionario, campo, valor"
    c.execute(base_query, params)
    resultados = c.fetchall()
    conn.close()
    return resultados


def busca_lemas_com_todos_campos(termo, dbfile, campos=None, dicionarios=None):
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    termo_lower = termo.lower()
    campos = campos or ["ontolex#writtenRep", "skos#definition", "rdf-schema#label"]
    campos_query = "('" + "', '".join(campos) + "')"
    params = [f"%{termo_lower}%"]

    base_query = f"""
        SELECT dicionario, label, campo, valor
        FROM verbetes_util
        WHERE campo IN {campos_query}
          AND lower(valor) LIKE ?
    """
    if dicionarios:
        dics_query = "('" + "', '".join(dicionarios) + "')"
        base_query += f" AND dicionario IN {dics_query}"

    base_query += " ORDER BY dicionario, campo, valor"
    c.execute(base_query, params)
    resultados = c.fetchall()

    # Organiza os resultados por (dicionario, label)
    verbetes = {}
    for dicionario, label, campo, valor in resultados:
        verbetes.setdefault((dicionario, label), {})[campo] = valor

    # Agora, para cada label encontrado, buscamos TODOS os campos desse label
    verbetes_completos = []
    for (dicionario, label), campos_encontrados in verbetes.items():
        c.execute(
            """
            SELECT campo, valor FROM verbetes_util
            WHERE label = ?
        """,
            (label,),
        )
        todos_campos = {}
        for campo, valor in c.fetchall():
            todos_campos.setdefault(campo, []).append(valor)
        resultado_final = {
            "dicionario": dicionario,
            "label": label,
            "campos_principais": campos_encontrados,
            "todos_os_campos": todos_campos,
        }
        verbetes_completos.append(resultado_final)

    conn.close()
    return verbetes_completos


# ---------- Exemplo de uso -----------
if __name__ == "__main__":
    dbfile = "dicionarios_unificados.sqlite"
    dicionario = "Cardoso"
    lemas = [
        "ab asino delapsus",
        "ab incunabulis",
        "abacus",
        "abaliēnātiō",  # use a grafia exata do lemma
        "abaliēnō",
        # adicione mais...
    ]

    verbetes_lista_formatado(dbfile, dicionario, lemas)
