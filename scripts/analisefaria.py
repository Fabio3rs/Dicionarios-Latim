import codecs
import json
import sqlite3, unicodedata
import re
import sys
import os
import difflib

DOC_NAME = "Dicionário - Ernesto Faria.pdf"
DB_FILE = "ocr_results.db"
START_PAGE = 7  # Verbete começa aqui

DB_LS = "ls_dict.sqlite"
DB_UNIFICADA = "dicionarios_unificados.sqlite"


def noaccents(a, b):
    def strip(s):
        return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")

    a_norm, b_norm = strip(a), strip(b)
    return (a_norm > b_norm) - (a_norm < b_norm)


def strip_accents(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


conn = sqlite3.connect(DB_LS)
conn.create_collation("NOACCENTS", noaccents)
"""ls_entries (
    id TEXT PRIMARY KEY,
    orths TEXT,
    itypes TEXT,
    explicit_forms TEXT,
    lemma TEXT,
    pos TEXT,
    definition TEXT,
    search_blob TEXT
)"""


conn_unificado = sqlite3.connect(DB_UNIFICADA)
conn_unificado.create_collation("NOACCENTS", noaccents)
# view dict_unified2


lemmas_ls_cache = set()
lemmas_unified_cache = set()


def find_lema_fuzzy(lema: str, cursor, cutoff=0.7):
    if len(lemmas_ls_cache) == 0:
        cursor.execute("SELECT lemma FROM ls_entries")
        todas = [row[0] for row in cursor.fetchall()]
        todas_norm = [strip_accents(x).lower() for x in todas]
        lemmas_ls_cache.update(todas_norm)

    match = difflib.get_close_matches(lema, lemmas_ls_cache, n=3, cutoff=cutoff)
    print(f"Fuzzy match para '{lema}': {match}")
    if match:
        return sorted(match)
    return []


def find_lema_in_ls_dict(lemas=[]):
    c = conn.cursor()
    print(f"Buscando lemas: {lemas}")

    lemas = [unicodedata.normalize("NFD", lema.strip().lower()) for lema in lemas]

    where_clause = " OR ".join(f"LOWER(lemma) = ? COLLATE NOACCENTS" for _ in lemas)
    if not where_clause:
        return []

    c.execute(
        f"SELECT * FROM ls_entries WHERE {where_clause}",
        [f"{lema}" for lema in lemas],
    )
    columns = [desc[0] for desc in c.description]
    result = c.fetchall()
    if result and len(result) > 0:
        return [dict(zip(columns, row)) for row in result]

    return []


def consulta_whitaker_words(palavras: list[str]):
    # path exec whitakers-words/bin/words
    # working dir whitakers-words
    import subprocess

    # Remover toda a acentuação das palavras
    palavras = [strip_accents(p) for p in palavras if p and p.lower().strip()]

    # Remover duplicatas
    palavras = set(palavras)

    cur_dir = os.getcwd()
    try:
        os.chdir("whitakers-words")
        # print(f"Consultando whitakers-words para: {palavras}")
        result = subprocess.run(
            [
                "bash",
                "-c",
                f"echo -e '\n' * {len(palavras)} | bin/words {' '.join(palavras)}",
            ],
            capture_output=True,
            text=True,
            check=True,
        )

        result_stdout = result.stdout
        result_stdout = result_stdout.replace("MORE - hit RETURN/ENTER to continue", "")

        # print("Resultado da consulta:", result_stdout)

        return result_stdout
    except subprocess.CalledProcessError as e:
        print("Erro ao executar o comando:", e)
        print("Saída do erro:", e.stderr)
    finally:
        os.chdir(cur_dir)

    return None


def find_lema_in_unificada_fuzzy(lemas: str):
    if len(lemmas_unified_cache) == 0:
        c = conn_unificado.cursor()
        c.execute("SELECT entry_label FROM dict_unified2")
        todas = [row[0] for row in c.fetchall()]
        # Extrai só o termo entre "lexical entry for '...'"
        todas_norm = []
        for x in todas:
            # Remove aspas e extrai o termo entre "lexical entry for '...'"
            m = x.find("lexical entry for")
            m = x[m:] if m != -1 else x
            m = m.strip("'").strip()

            todas_norm.append(strip_accents(m).lower())
        lemmas_unified_cache.update(todas_norm)

    match = difflib.get_close_matches(lemas, lemmas_unified_cache, n=3, cutoff=0.7)
    print(f"Fuzzy match para '{lemas}': {match}")
    if match:
        return sorted(match)
    return []


"""
dicionario  entry_uri                                                     entry_label                                  definicoes_pt                                                 notas                        
----------  ------------------------------------------------------------  -------------------------------------------  ------------------------------------------------------------  -----------------------------
Cardoso     http://lila-erc.eu/data/lexicalResources/LatinPortuguese/Car  Cardoso\u2019s lexical entry for 'A'         (significa. de)"@pt |  ( significa . de ) "@pt                praepositio c\u0169 ablatiuo.
            doso/id/LexicalEntry/n00001.e01                                                                                                                                                                       

Cardoso     http://lila-erc.eu/data/lexicalResources/LatinPortuguese/Car  Cardoso\u2019s lexical entry for 'A'                                                                                                    
            doso/id/LexicalEntry/n00002.e01                                                                                                                                                                       

Cardoso     http://lila-erc.eu/data/lexicalResources/LatinPortuguese/Car  Cardoso\u2019s lexical entry for 'Ab'        s. de. "@pt |  s . de . "@pt                                                               
            doso/id/LexicalEntry/n00003.e01                                                                                                                                                                       

Cardoso     http://lila-erc.eu/data/lexicalResources/LatinPortuguese/Car  Cardoso\u2019s lexical entry for 'Abacus'    A copeira. ou mesa de c\u00f5tar"@pt |  A copeira . ou mesa                                
            doso/id/LexicalEntry/n00004.e01                                                                            de c\u00f5tar "@pt                                                                         

Cardoso     http://lila-erc.eu/data/lexicalResources/LatinPortuguese/Car  Cardoso\u2019s lexical entry for 'Abaculus'  A pe\u00e7a do enxadrez"@pt |  A pe\u00e7a do enxadrez "@pt                                
            doso/id/LexicalEntry/n00005.e01                                                                                                                                                                       
sqlite> 
"""


def find_lema_in_unificada(lemas):
    c = conn_unificado.cursor()
    print(f"Buscando lema na unificada: {lemas}")

    lemas = [unicodedata.normalize("NFD", lema.strip().lower()) for lema in lemas]

    where_clause = " OR ".join(
        f"LOWER(entry_label) LIKE ? COLLATE NOACCENTS" for _ in lemas
    )
    if not where_clause:
        return []

    c.execute(
        f"SELECT * FROM dict_unified2 WHERE {where_clause}",
        [f"%'{lema}'%" for lema in lemas],
    )
    cols = [desc[0] for desc in c.description]
    rows = c.fetchall()

    def unescape(s):
        if isinstance(s, str):
            # decodifica \uXXXX e \' e \" etc.
            return codecs.decode(s, "unicode_escape")
        return s

    results = []
    for row in rows:
        d = dict(zip(cols, row))
        # campos que vêm escapados
        for fld in ("entry_label", "definicoes_pt", "notas"):
            d[fld] = unescape(d.get(fld, ""))
        results.append(d)

    return results


# Regex para detectar o cabeçalho (exemplo: ABAKIS — 12 — ABĒO)
HEADER_REGEX = re.compile(
    r"^([A-ZÁÉÍÓÚÂÊÎÔÛÃÕÇ][^\s]*)\s+—\s+\d+\s+—\s+([A-ZÁÉÍÓÚÂÊÎÔÛÃÕÇ][^\s]*)",
    re.UNICODE,
)

"""

ABL. = ablativo
ABS. = absoluto, ou em absoluto
ABSL. = absolutamente
ACUS. = acusativo
ADJ. = adjetivo
ADV. = advérbio
CF. = confere, compare
COMP. = comparativo
CONJ. = conjunção
DAT. = dativo
DEM. = demonstrativo
DEP. = deponente
DIM. = diminutivo
DISTRIB. = distributivo
F. = feminino
FREQ. = frequentativo
FUT. = futuro
GEN. = genitivo
IMPF. = imperfeito
IMPESS. = impessoal
INDECL. = indeclinável
INF. = infinitivo
INTERJ. = interjeição
INTERR. = interrogação, interrogativo
INTR. = intransitivo
LOC. = locativo

M. = masculino
N. = neutro
NOM. = nominativo
NUM. = numeral
ORD. = ordinal
PART. = particípio
PERF. = perfeito
PASS. = passado ou passivo
PES. = pessoa
PL. = plural
PREP. = preposição
PRÉS. = presente
PR. = próprio
PRON. = pronome
PREV. = provérbio
REFLEX. = reflexivo
SG. = singular
SENT. = sentido
SINC. = sincopado
SUBS. = substantivo
SUBJ. = subjuntivo
SUPERL. = superlativo
TR. = transitivo
V. = verbo
V. = veja
VOC. = vocativo.
"""
abrev = r"(subs\.|adj\.|v\.|adv\.|pron\.|prep\.|conj\.|interj\.|num\.|part\.|freq\.|dim\.|comp\.|superl\.|tr\.|intr\.|m\.|f\.|n\.|pl\.|sg\.)"

# Lema geralmente: palavra (talvez número, hífen, acentos), vírgula, terminações, abreviações (no início de linha!)
LEMA_RE = re.compile(
    rf"(?m)^([^\s].{{0,40}}?)\s*,\s*([^\n]*?)\s*{abrev}.*", re.IGNORECASE
)


# Regex simplificado para tentar separar verbetes (melhorar conforme estrutura real)
LEMA_REGEX = re.compile(
    r"(?m)"  # modo multiline
    r"^(?:\s*(\d+\.\s*)?)"  # (opcional) número + ponto
    r"([A-Z]?[a-zA-ZāēīōūȳăĕĭŏŭçâêîôûãõÁÉÍÓÚÂÊÎÔÛÃÕÇ\-]+(?:\s*\([^)]+\))?)"  # lema
    r",\s*(.*?)\."  # formas até primeiro ponto final
    r"(?:\s+)"  # espaço obrig.
    r"((?:(?!^(?:\d+\.\s*)?[A-Z]?[a-zA-ZāēīōūȳăĕĭŏŭÁÉÍÓÚÇ\-]+\s*\(?.*?\)?,).*\n?)*)",  # definição (todas linhas até o próximo lema ou fim)
    re.DOTALL | re.MULTILINE,
)

LEMA_SPLIT_REGEX = re.compile(
    r"""(?=             # Lookahead: NÃO consome, só aponta separação
        (?:^|[\n\.])    # Início de linha OU após ponto
        (?:\d+\.\s*)?   # Opcional: número+.
        [A-Za-zāēīōūȳăĕĭŏŭçâêîôûãõÁÉÍÓÚÂÊÎÔÛÃÕÇ\-]+  # Lema (ao menos 2 letras/hífens)
        ,\s             # vírgula e espaço
    )""",
    re.VERBOSE | re.MULTILINE,
)


# ValueError: Erro de cabeçalho na página 8: ABAKIS != Abāris
def remover_acentuacao(texto):
    # Remove acentuação latina (macrons, breves, etc.)
    return re.sub(r"[āăēĕīĭōŏūŭȳȳă]", "", texto)


def carregar_paginas(doc_name, dbfile, start_page=7):
    conn = sqlite3.connect(dbfile)
    c = conn.cursor()
    c.execute(
        "SELECT page_num, extracted_text FROM pages WHERE doc_name=? AND page_num>=? ORDER BY page_num",
        (doc_name, start_page),
    )
    pages = c.fetchall()
    conn.close()
    return pages


def extrair_headers(pages):
    headers = []
    for page_num, text in pages:
        first_line = text.strip().split("\n")[0]
        m = HEADER_REGEX.match(first_line)
        if m:
            headers.append(
                {
                    "page_num": page_num,
                    "first_word": m.group(1),
                    "last_word": m.group(2),
                }
            )
    return headers


def extrair_header(page_text):
    lines = page_text.strip().split("\n")
    if not lines:
        return None
    first_line = lines[0]
    m = HEADER_REGEX.match(first_line)
    if m:
        return {"first_word": m.group(1), "last_word": m.group(2)}
    return None


def remover_cabecalho(texto):
    linhas = texto.strip().split("\n")
    # Remove primeira linha se for cabeçalho
    if linhas and HEADER_REGEX.match(linhas[0].strip()):
        return "\n".join(linhas[1:])

    if len(linhas) == 1 and linhas[0].strip() in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
        # Se o cabeçalho for apenas uma letra
        return "\n".join(linhas[1:])

    # pode estar como ——— Número ———
    if linhas and linhas[0].strip().startswith("——") and len(linhas[0].strip()) > 10:
        return "\n".join(linhas[1:])
    return texto


def remover_notas_rodape(texto):
    # Remove rodapés LLM (ex: "(*): Nenhum sinal diacrítico ...")
    # Notas das dúvidas marcadas com (*):
    whereit = texto.find("Notas das dúvidas marcadas com (*)")
    if whereit != -1:
        texto = texto[:whereit].strip()

    # —

    # Explicações dos
    whereit = texto.find("—\n\nExplicações dos (*)")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("--\nExplicações dos (*)")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("---\n\nExplicações")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("—\n\nFootnotes:")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("---\n\nFootnotes:")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("\n\nFootnotes:")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("\n\nNotes:")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("\n\nExplicações dos (*)")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("\n* Explicações")
    if whereit != -1:
        texto = texto[:whereit].strip()

    whereit = texto.find("\nExplicaç")
    if whereit != -1:
        texto = texto[:whereit].strip()

    # Remove rodapés de páginas (ex: "Página 12")
    texto = re.sub(r"\nPágina \d+\n", "\n", texto)
    # Remove rodapés de notas de rodapé
    return re.sub(r"\(\*\).*", "", texto, flags=re.DOTALL)


def corrige_quebras_de_palavra_por_quebras_de_linha(texto):
    # Localizar palavras que terminam com hífen e continuam na linha seguinte
    padrao = re.compile(r"(\w+)-\n(\w+)")
    # Remover o hífen e juntar as palavras
    texto_corrigido = padrao.sub(r"\1\2", texto)
    # Corrigir quebras de linha desnecessárias
    # texto_corrigido = re.sub(r'\n+', '\n', texto_corrigido)

    # Remove quebras de linha e espaços logo após '(' e logo antes de ')'
    texto_corrigido = re.sub(r"\(\s*\n\s*", "(", texto_corrigido)
    texto_corrigido = re.sub(r"\s*\n\s*\)", ")", texto_corrigido)

    texto_corrigido = re.sub(r"\nI — ", "\n\tI — ", texto_corrigido)
    texto_corrigido = re.sub(r"\nII — ", "\n\tII — ", texto_corrigido)
    texto_corrigido = re.sub(r"\nIII — ", "\n\tIII — ", texto_corrigido)
    texto_corrigido = re.sub(r"\nIV — ", "\n\tIV — ", texto_corrigido)

    texto_corrigido = re.sub(r"\nI—", "\n\tI — ", texto_corrigido)
    texto_corrigido = re.sub(r"\nII—", "\n\tII — ", texto_corrigido)
    texto_corrigido = re.sub(r"\nIII—", "\n\tIII — ", texto_corrigido)
    texto_corrigido = re.sub(r"\nIV—", "\n\tIV — ", texto_corrigido)

    texto_corrigido = re.sub(r"\n1\) ", "\n\t1) ", texto_corrigido)
    texto_corrigido = re.sub(r"\n2\) ", "\n\t2) ", texto_corrigido)
    texto_corrigido = re.sub(r"\n3\) ", "\n\t3) ", texto_corrigido)
    texto_corrigido = re.sub(r"\n4\) ", "\n\t4) ", texto_corrigido)
    texto_corrigido = re.sub(r"\n5\) ", "\n\t5) ", texto_corrigido)
    texto_corrigido = re.sub(r"\n6\) ", "\n\t6) ", texto_corrigido)

    # Remove quebras de linha dentro de parênteses (ex: "(foo\nbar)" -> "(foo bar)")
    def junta_quebras_parenteses(match):
        return match.group(0).replace("\n", " ")

    texto_corrigido = re.sub(r"\([^\)]*\)", junta_quebras_parenteses, texto_corrigido)

    texto_corrigido = texto_corrigido.replace("Sent. \npróprio:", "Sent. próprio:")
    texto_corrigido = texto_corrigido.replace("Sent.\npróprio:", "Sent. próprio:")

    # Pesquisa pela quebra de linha em Sent. Próprio com regex usando caracter especial para blank
    texto_corrigido = re.sub(r"Sent\.\s+próprio:", "Sent. próprio:", texto_corrigido)
    texto_corrigido = re.sub(r"Sent\.\s+figurado:", "Sent. figurado:", texto_corrigido)

    texto_corrigido = texto_corrigido.replace("\nSent. próprio:", " Sent. próprio:")
    texto_corrigido = texto_corrigido.replace("\nSent.próprio:", " Sent. próprio:")

    texto_corrigido = texto_corrigido.replace("\nSent. figurado:", " Sent. figurado:")
    texto_corrigido = texto_corrigido.replace("\nSent.figurado:", " Sent. figurado:")

    # Procura todas as linhas que começam com uma das palavras do ABREVS list
    # e remove a quebra de linha no início da linha
    abrev_pattern = re.compile(
        r"^\s*(" + "|".join(re.escape(a.capitalize()) for a in ABREVS) + r")\b"
    )

    linhas = texto_corrigido.splitlines()
    texto_corrigido = ""
    for i, linha in enumerate(linhas):
        if abrev_pattern.match(linha):
            texto_corrigido += " " + linha
        else:
            texto_corrigido += "\n" + linha

    return texto_corrigido


def parse_linha(lema_line):
    # 1. Extrair lema (antes da primeira vírgula)
    m = re.match(r"^([^\s,]+)", lema_line)
    lema = m.group(1) if m else None

    # 2. Extrair sinônimos (padrão: "ou ...", "v. ...", etc)
    sinonimos = []
    ou_match = re.search(r"\bou\s+([^\s,;]+)", lema_line)
    if ou_match:
        sinonimos.append(ou_match.group(1))
    v_match = re.search(r"\bv\.\s*([^\s,;]+)", lema_line)
    if v_match:
        sinonimos.append(v_match.group(1))
    parenteses_ou = re.findall(r"\(ou ([^)]+)\)", lema_line)
    sinonimos += parenteses_ou
    sinonimos = list(set(sinonimos)) if sinonimos else None

    # 3. Extrair regras (terminações)
    regras = []
    regras_matches = re.findall(r"((?:-[\wāēīōūăĕĭŏŭȳ]+(?:, )?)+)", lema_line)
    for rm in regras_matches:
        for parte in rm.split(","):
            parte = parte.strip()
            if parte.startswith("-"):
                regras.append(parte)
    regras = regras or None

    # 4. Descrição curta (após abreviação)
    desc_idx = None
    for abrv in [
        "subs.",
        "adj.",
        "v.",
        "adv.",
        "pron.",
        "prep.",
        "conj.",
        "interj.",
        "num.",
        "part.",
    ]:
        idx = lema_line.find(abrv)
        if idx != -1:
            desc_idx = idx + len(abrv)
            break
    descricao = lema_line[desc_idx:].strip() if desc_idx is not None else ""

    return {
        "lema": lema,
        "sinonimos": sinonimos if sinonimos else [],
        "regras": regras if regras else [],
        "descricao_curta": descricao,
    }


def parse_linha_multi(lema_line):
    # 1. Separe a parte de lemas: até a primeira ocorrência de traço, abreviação ou terminações
    # Procura por traço ("—") ou abreviação
    sep = lema_line.find("—")
    abrv_match = re.search(
        r"\b(subs\.|adj\.|v\.|adv\.|pron\.|prep\.|conj\.|interj\.|num\.|part\.)\b",
        lema_line,
    )
    if abrv_match and (sep == -1 or abrv_match.start() < sep):
        sep = abrv_match.start()
    cabecalho = lema_line[:sep].strip() if sep != -1 else lema_line.strip()
    resto = lema_line[sep:].strip() if sep != -1 else ""

    # 2. Divide por vírgula (e por “ou” se for o caso)
    lemas = [l.strip() for l in re.split(r",\s*", cabecalho) if l.strip()]

    # 3. Restante do seu parser, mas agora para cada lema!
    resultados = []
    for lema in lemas:
        campos = {
            "lema": lema,
            "sinonimos": None,  # pode deixar vazio se são entradas equivalentes
            "regras": None,
            "descricao_curta": resto,
        }
        resultados.append(campos)
    return resultados


ABREVS = [
    "abl.",  # ablativo
    "abs.",  # absoluto, ou em absoluto
    "absl.",  # absolutamente
    "abrev.",  # abreviação
    "acus.",  # acusativo
    "adj.",  # adjetivo
    "adv.",  # advérbio
    "cf.",  # confere, compare
    "comp.",  # comparativo
    "conj.",  # conjunção
    "dat.",  # dativo
    "dem.",  # demonstrativo
    "dep.",  # deponente
    "dim.",  # diminutivo
    "distrib.",  # distributivo
    "f.",  # feminino
    "freq.",  # frequentativo
    "fut.",  # futuro
    "gen.",  # genitivo
    "impf.",  # imperfeito
    "impess.",  # impessoal
    "indecl.",  # indeclinável
    "inf.",  # infinitivo
    "interj.",  # interjeição
    "interr.",  # interrogação, interrogativo
    "intr.",  # intransitivo
    "loc.",  # locativo
    "m.",  # masculino
    "n.",  # neutro
    "nom.",  # nominativo
    "num.",  # numeral
    "ord.",  # ordinal
    "part.",  # particípio
    "perf.",  # perfeito
    "pass.",  # passado ou passivo
    "pes.",  # pessoa
    "pl.",  # plural
    "prep.",  # preposição
    "prés.",  # presente
    "pr.",  # próprio
    "pron.",  # pronome
    "prev.",  # provérbio
    "reflex.",  # reflexivo
    "sg.",  # singular
    "sent.",  # sentido
    "sinc.",  # sincopado
    "subs.",  # substantivo
    "subj.",  # subjuntivo
    "superl.",  # superlativo
    "tr.",  # transitivo
    "v.",  # verbo ou veja
    "voc.",  # vocativo
    " = ",  # igual a
]

ABREV_RE = re.compile(r"\b(?:" + "|".join(map(re.escape, ABREVS)) + r")\b")
TRACO_RE = re.compile(r"\s+—\s+")

# -ae 	-ī		-is 		-ūs		-eī/-ēī

DESINENCIAS_GENITIVOS_CONJUG = [
    # Genitivos singulares
    "ae",
    "i",
    "is",
    "us",
    "ei",
    # Verbos
    "are",
    "ere",
    "ire",
    # Participios
    "atus",
    "itus",
]


def detecta_multiplos_lemas(linha):
    # Se a linha inicia com Número. Remove o número e o ponto. Exemplo: 2. Word
    linha = re.sub(r"^\d+\.\s*", "", linha).strip()
    linha = re.sub(r"^\d+\)\.\s*", "", linha).strip()

    stop = len(linha)
    m1 = TRACO_RE.search(linha)
    m2 = ABREV_RE.search(linha)
    if m1:
        stop = min(stop, m1.start())
    if m2:
        stop = min(stop, m2.start())
    blocolemas = linha[:stop].strip()
    lemas = [x.strip() for x in blocolemas.split(",") if x.strip()]
    if len(lemas) > 1 and all(re.fullmatch(r"[a-z]{1,30}", l) for l in lemas):
        return lemas
    else:
        return [blocolemas]


cache_ls_lemas = set()


def verifica_se_existe_ls(lema):
    if len(cache_ls_lemas) == 0:
        c = conn.cursor()
        c.execute("SELECT lemma FROM ls_entries")
        cache_ls_lemas.update(
            strip_accents(row[0].lower()).strip() for row in c.fetchall()
        )

    # if there is a ( in the lemma, remove it and anything after it
    if "(" in lema:
        lema = lema.split("(")[0].strip()
    if "<" in lema:
        lema = lema.split("<")[0].strip()

    lema_normalizado = strip_accents(lema.lower()).strip()
    if lema_normalizado in cache_ls_lemas:
        return True

    res_whitaker = consulta_whitaker_words([lema_normalizado])

    if res_whitaker is None:
        return False

    if "UNKNOWN" in res_whitaker or "NO MATCH" in res_whitaker:
        return False

    return True


# in ['a, subs. f. (ou n.) indecl.'] out "a"
def extrair_lema(linha):
    # Extrai o lema da linha, que é a primeira parte antes de vírgula ou traço
    m = re.match(r"^([^\s,—]+)", linha)
    if m:
        return m.group(1).strip()
    return None


from buscadicionarios import (
    verbetes_lista_formatado,
    verbetes_lista_todos_dicionarios,
    busca_todos_os_lemas,
    busca_lemas_com_todos_campos,
)

ABREV_PATTERN = re.compile(
    r"(?<!\w)(?:" + "|".join(map(re.escape, ABREVS)) + r")(?!\w)", flags=re.IGNORECASE
)


def extrair_sequencia_abrevs(texto: str) -> list[str]:
    """
    Retorna todos os tokens abreviação (separados por espaço ou vírgula),
    parando no primeiro que não bate com ABREV_PATTERN.
    """
    # divide em tokens: remove espaços extras e divide por vírgula/espaço
    tokens = re.split(r"[,\s]+", texto.strip())
    resultado = []
    for tok in tokens:
        # tenta casar o token inteiro; se não for, já pára tudo
        if ABREV_PATTERN.fullmatch(tok):
            resultado.append(tok.lower())
        else:
            break
    return resultado


def filtra_por_tipo(verbetes, filtros=None):
    """
    verbetes: lista de dicts (cada verbete deve ter campos como 'itypes', 'descricao', etc)
    filtros: lista de palavras obrigatórias (ex: ["indecl.", "subst.", "f.", "n."])
    """
    if filtros is None:
        return verbetes

    if not isinstance(filtros, list):
        filtros = [filtros]

    # Mantém apenas os filtros válidos conhecidos em ABREVS
    filtros = [f for f in filtros if f in ABREVS]

    if not filtros:
        return verbetes  # Se não há filtros, retorna todos os verbetes

    filtrados = []
    for v in verbetes:
        hit = False
        for f in filtros:
            # Testa tanto em itypes (pode ser string ou lista) quanto em descricao
            itypes = v.get("itypes", "")
            if isinstance(itypes, str):
                itypes_str = itypes
            else:
                itypes_str = " ".join(itypes)
            desc = v.get("descricao", "")
            if f in itypes_str or f in desc:
                hit = True
        if hit:
            filtrados.append(v)
    if len(filtrados) == 0:
        print(f"Nenhum verbete encontrado com os filtros: {filtros}")
        return verbetes
    return filtrados


"""
  {
    "id": "n1649",
    "orths": "[\"āh\", \"ā\"]",
    "itypes": "[]",
    "explicit_forms": "[\"grief\", \"ah\", \"doleam\", \"noli\", \"not\", \"stulte\", \"obsecra\", \"desine\", \"volet\", \"scio\", \"joy\"]",
    "lemma": "āh",
    "pos": "interj.",
    "definition": "ah! alas! ha! ah me! an exclamation.\nOf pain or grief, Gr. αἴ, αἴ: ah, nescis quam doleam, Ter. Heaut. 5, 1, 61; Verg. E. 1, 15. —\nOf entreaty to avert an evil: ah! noli, do not, I pray! Plaut. Am. 1, 3, 22. —\nOf indignation or reproach: ah stulte, Ter. Ad. 4, 7, 6: ah, rogitas? id. And. 5, 1, 9; 3, 1, 11.—\nOf admonition: ah, ne me obsecra, Ter. And. 3, 3, 11: ah desine, id. ib. 5, 6, 8.—\nOf consolation: quid? ah volet, certo scio, Ter. Eun. 5, 2, 50.—\nOf raillery or joy, Plaut. Curc. 1, 2, 39.",
    "search_blob": "desine doleam grief joy noli not obsecra scio stulte volet"
  }
]
Lema: a
Sinônimos: None
Regras: None
Descrição: Veja ah.
----------------------------------------
Lema: ah
Sinônimos: None
Regras: None
Descrição: Veja ah.

"""


def filtrar_por_orths_melhor_match(verbetes, lemas_filtrados):
    """
    Filtra verbetes por orths, priorizando os que têm todos os orths exatos (ordem não importa).
    Se não encontrar, retorna os verbetes com maior interseção de orths.
    """
    if not lemas_filtrados or not verbetes:
        return verbetes

    def normaliza(s):
        # Remove acentos e normaliza para minúsculas
        s = unicodedata.normalize("NFD", s)
        s = "".join(c for c in s if unicodedata.category(c) != "Mn")
        return s.lower().strip()

    orths_set = set([normaliza(o) for o in lemas_filtrados if o])

    melhores = []
    max_intersec = 0

    for v in verbetes:
        v_orths = v.get("orths")
        if isinstance(v_orths, str):
            try:
                v_orths_list = json.loads(v_orths)
            except Exception:
                v_orths_list = [v_orths]
        elif isinstance(v_orths, list):
            v_orths_list = v_orths
        else:
            v_orths_list = []

        v_orths_set = set([normaliza(x) for x in v_orths_list if x])

        # Exato: todos os orths coincidem (ordem não importa)
        # print(f"Verificando verbete: {v['lemma']} com orths {v_orths_set} e entrada {orths_set}")
        if v_orths_set == orths_set:
            melhores.append(v)
            continue

        intersec = len(v_orths_set & orths_set)
        if intersec > max_intersec:
            max_intersec = intersec

    if melhores:
        return melhores

    if max_intersec > 0:
        result = [
            v
            for v in verbetes
            if len(
                set(
                    [
                        normaliza(x)
                        for x in (
                            json.loads(v["orths"])
                            if isinstance(v["orths"], str)
                            else v.get("orths", [])
                        )
                    ]
                )
                & orths_set
            )
            == max_intersec
        ]

        if len(result) > 0:
            return result

    return verbetes


def chunkenizacao_especial(texto):
    """
    Chunkeniza o texto em um ou múltiplos verbetes para análise via LLM
    """
    matches = list(LEMA_RE.finditer(texto))
    posicoes = [m.start() for m in matches] + [len(texto)]

    # Agora cada verbete está entre posicoes[i] e posicoes[i+1]
    verbetes = [
        texto[posicoes[i] : posicoes[i + 1]].strip() for i in range(len(posicoes) - 1)
    ]

    verbetes_reescritos = []
    chunk_atual = ""
    contagem = 0

    chunk_verbete = ""
    verbetes_novos = []

    sliding_window_size = (
        5  # Tamanho da janela deslizante para primeira letra dos verbetes
    )
    window_chars = list()

    lista_problemas = []

    for verbete in verbetes:
        # Extrair a primeira palavra da primeira linha como lema
        linhas = verbete.splitlines()
        if not linhas:
            continue

        # Se começar com número. remover
        if re.match(r"^\d\.", linhas[0]):
            lema = re.sub(r"^\d+\.\s*", "", linhas[0].strip())

        lema = linhas[0].strip()
        lema = unicodedata.normalize("NFD", lema.strip().lower())
        lemas = detecta_multiplos_lemas(lema)

        lemas = [l.split(",")[0].strip().split(" ")[0] for l in lemas if l.strip()]
        primeiro_lema = lemas[0] if lemas else None

        # se o primeiro_lema for None ou não for alfabético, adiciona a linha inteira
        if (
            len(primeiro_lema) == 0
            or not primeiro_lema[0].isalpha()
            or strip_accents(primeiro_lema.lower()).strip() == "dai:"
        ):
            # print(f"Adicionando verbete completo: {verbete}")
            chunk_atual += "\n" + verbete
            chunk_verbete += "\n" + verbete
            continue

        comeco_verbete = strip_accents(verbete.lower()[0:50]).strip()
        existe_abreviacao = False
        for abreviacao in ABREVS:
            if abreviacao in comeco_verbete:
                existe_abreviacao = True
                break

        palavras = comeco_verbete.split(" ")
        # remover vírgulas e pontos finais
        # Split each word by ',' and '.' and flatten the list
        novas_palavras = []
        for p in palavras:
            for parte in re.split(r"[,.]", p):
                parte = parte.strip()
                if parte:
                    novas_palavras.append(parte)
        palavras = novas_palavras
        for desinencia in DESINENCIAS_GENITIVOS_CONJUG:
            if desinencia in palavras:
                existe_abreviacao = True

        if not existe_abreviacao:
            # Se o começo não for uma abreviação, adiciona o verbete completo pois não é um lema válido
            chunk_atual += "\n" + verbete
            chunk_verbete += "\n" + verbete
            continue

        if primeiro_lema == "i." or primeiro_lema == "l.":
            # Falha de OCR, deveria ser 1. corrigir e prosseguir
            print(f"Falha de OCR detectada: {verbete}, corrigindo...")
            verbetetmp = verbete[2:].lstrip()
            linhas_tmp = linhas[0][2:].strip()
            lema_tmp = linhas_tmp[0].strip()
            lema_tmp = unicodedata.normalize("NFD", lema_tmp.strip().lower())
            lemas_tmp = detecta_multiplos_lemas(lema_tmp)

            lemas_tmp = [
                l.split(",")[0].strip().split(" ")[0] for l in lemas_tmp if l.strip()
            ]
            primeiro_lema_tmp = lemas_tmp[0] if lemas_tmp else None

            if primeiro_lema_tmp[0].isalpha():
                existe = verifica_se_existe_ls(primeiro_lema_tmp)

                if existe and len(primeiro_lema_tmp) > 0:
                    verbete = verbetetmp
                    linhas = linhas_tmp
                    lema = lema_tmp
                    lemas = lemas_tmp
                    primeiro_lema = primeiro_lema_tmp
                    print(f"Correção aplicada: {verbete}")
                else:
                    print(
                        f"Correção não aplicada, mantendo verbete original: {verbete}"
                    )
                    input("Pressione Enter para continuar...")
            else:
                # Se o primeiro lema não é alfabético, mantém o verbete original
                print(f"Correção não aplicada, mantendo verbete original: {verbete}")
                chunk_atual += "\n" + verbete
                chunk_verbete += "\n" + verbete
                continue

        if primeiro_lema.endswith(".") or primeiro_lema.endswith(":"):
            print(verbete)

        if strip_accents(primeiro_lema.lower()).strip() in ABREVS:
            # Se o lema for uma abreviação, ignora e adiciona o verbete completo
            # print(f"Adicionando verbete completo (abreviação): {verbete}")
            chunk_atual += "\n" + verbete
            chunk_verbete += verbete + "\n"
            continue

        found_in_ls = verifica_se_existe_ls(primeiro_lema)
        print(
            f"Lema(s) detectado(s): {primeiro_lema} (Encontrado no LS: {found_in_ls})"
        )

        if not found_in_ls:

            if primeiro_lema == "um":
                print(verbete)
        # input("Pressione Enter para continuar...")

        def char_distance_too_set(char, char_set : set[str]):
            for c in char_set:
                if abs(ord(c) - ord(char)) <= 1:
                    return True
            return False

        if found_in_ls:
            contagem += 1

            if len(window_chars) == 0:
                window_chars.append(strip_accents(primeiro_lema)[0].lower())

            chunk_set = set(window_chars)
            cur_char = strip_accents(primeiro_lema)[0].lower()

            if (
                (len(chunk_set) == 0 or cur_char in chunk_set)
                or (
                    len(chunk_set) > 0
                    and char_distance_too_set(cur_char, chunk_set)
                )
                or ("v" in chunk_set and cur_char == "x")
            ):  # Não temos palavras com W, portanto pulamos do V para o X
                if len(chunk_verbete) > 0:
                    verbetes_novos.append(chunk_verbete.strip())
                chunk_verbete = ""
            else:
                # input(f"Caractere {cur_char} não encontrado na janela: {chunk_set} {verbete}. Pressione Enter para continuar...")
                lista_problemas.append(
                    {
                        "lema": primeiro_lema,
                        "verbete": verbete,
                        "chunk_set": window_chars.copy(),
                        "cur_char": cur_char,
                    }
                )

            window_chars.append(cur_char)
            if len(window_chars) > sliding_window_size:
                window_chars.pop(0)

            if (len(chunk_atual) + len(verbete)) > 2000 or contagem > 4:
                # Se o chunk atual + o novo verbete ultrapassar 2000 caracteres, salva o chunk atual
                verbetes_reescritos.append(chunk_atual)
                chunk_atual = ""
                contagem = 0
            else:
                chunk_atual += "\n\n---------------------------------------"
            # Adiciona o verbete ao chunk atual
            chunk_atual += "\n" + verbete
            chunk_verbete += verbete + "\n"
        else:
            # Se não encontrou no LS, adiciona o verbete completo
            # print(f"Adicionando verbete completo (não encontrado no LS): {verbete}")
            chunk_atual += "\n" + verbete

            chunk_set = set(window_chars)
            cur_char = strip_accents(primeiro_lema)[0].lower()

            # se o caractere atual estiver na janela ou a distância for 1, adiciona o verbete
            if (
                cur_char in chunk_set
                or (
                    len(chunk_set) > 0
                    and char_distance_too_set(cur_char, chunk_set)
                )
                or ("v" in chunk_set and cur_char == "x")
            ):  # Não temos palavras com W, portanto pulamos do V para o X
                if len(chunk_verbete) > 0:
                    verbetes_novos.append(chunk_verbete.strip())
                    window_chars.append(cur_char)
                    if len(window_chars) > sliding_window_size:
                        window_chars.pop(0)
                chunk_verbete = ""

            chunk_verbete += verbete + "\n"

        # Se o lema for uma palavra latina

    if len(chunk_atual) > 0:
        # Adiciona o último chunk se não estiver vazio
        verbetes_reescritos.append(chunk_atual.strip())

    if len(chunk_verbete := chunk_verbete.strip()) > 0:
        verbetes_novos.append(chunk_verbete)

    print(f"Total de verbetes novos: {len(verbetes_novos)}")

    if len(lista_problemas) > 0:
        print(f"Total de problemas encontrados: {len(lista_problemas)}")
        with open("problemas.txt", "w", encoding="utf-8") as f:
            for problema in lista_problemas:
                f.write(
                    f"Lema: {problema['lema']}\n"
                    f"Verbete: {problema['verbete']}\n"
                    f"Chunk set: {problema['chunk_set']}\n"
                    f"Caractere atual: {problema['cur_char']}\n\n"
                )

    # return verbetes_reescritos
    return verbetes_novos


def analise_completa(texto):
    matches = list(LEMA_RE.finditer(texto))
    posicoes = [m.start() for m in matches] + [len(texto)]

    # Agora cada verbete está entre posicoes[i] e posicoes[i+1]
    verbetes = [
        texto[posicoes[i] : posicoes[i + 1]].strip() for i in range(len(posicoes) - 1)
    ]

    print(f"Total de verbetes encontrados: {len(verbetes)}\n")

    parsed_verbetes = []

    letras_iniciais_anteriores = set()

    lemas_anteriores = []

    erros_de_ordem = []

    for i, verbete in enumerate(verbetes):
        linhas = verbete.splitlines()
        if not linhas:
            continue
        primeira_linha = linhas[0].strip()
        resto = "\n".join(linhas[1:]).strip() if len(linhas) > 1 else ""

        # Detecta múltiplos lemas reais
        print(primeira_linha)
        primeira_linha = primeira_linha.strip("—").strip()
        lemas = detecta_multiplos_lemas(primeira_linha)

        # verbetes = verbetes_lista_todos_dicionarios(lemas=['Puell'], dbpath='dicionarios_unificados.sqlite')
        # verbete = busca_lemas_com_todos_campos(termo="Puella", dbfile='dicionarios_unificados.sqlite')

        # print(json.dumps(verbete, ensure_ascii=False, indent=2))
        # print(verbetes)

        lemas_filtrados = [extrair_lema(l) for l in lemas if l.strip()]

        if len(lemas_anteriores) > 0 and len(lemas_filtrados) > 0:
            print(
                f"Comparando lemas anteriores e atuais: {lemas_anteriores} vs {lemas_filtrados}"
            )

            anterior = strip_accents(lemas_anteriores[0]).lower()
            filtrado = strip_accents(lemas_filtrados[0]).lower()

            if (ord(filtrado[0]) - ord(anterior[0])) > 1:
                # Se a diferença entre os primeiros caracteres for maior que 1, é um erro
                print(
                    f"Erro de ordem alfabética: {lemas_anteriores[0]} > {lemas_filtrados[0]}"
                )
                erros_de_ordem.append(
                    {
                        "lema_anterior": lemas_anteriores[0],
                        "lema_filtrado": lemas_filtrados[0],
                        "anterior": anterior,
                        "filtrado": filtrado,
                    }
                )
                # input("Pressione Enter para continuar...")

            if anterior > filtrado:
                print(
                    f"Erro de ordem alfabética: {lemas_anteriores[0]} > {lemas_filtrados[0]}"
                )
                erros_de_ordem.append(
                    {
                        "lema_anterior": lemas_anteriores[0],
                        "lema_filtrado": lemas_filtrados[0],
                        "anterior": anterior,
                        "filtrado": filtrado,
                    }
                )
                # input("Pressione Enter para continuar...")

        if (
            lemas_filtrados is None
            or len(lemas_filtrados) == 0
            or lemas_filtrados[0] is None
        ):
            lemas_filtrados = lemas_anteriores
            # input(f"Verbetes sem lemas válidos: {lemas}. Usando lemas anteriores: {lemas_anteriores} (Pressione Enter para continuar...)")
        else:
            lemas_anteriores = lemas_filtrados

        print(f"Processando verbete {i+1}/{len(verbetes)} {lemas}: {lemas_filtrados}")

        continue

        letras_iniciais = set(l[0].upper() for l in lemas_filtrados if l.strip())

        verbetes = find_lema_in_ls_dict(lemas_filtrados)

        # continue  # Se quiser parar aqui, descomente esta linha

        if len(verbetes) == 0:
            fuzzy_matches = find_lema_fuzzy(
                lemas_filtrados[0], conn.cursor(), cutoff=0.7
            )
            if fuzzy_matches:
                input(f"Fuzzy matches encontrados: {fuzzy_matches}")
                # Tenta primeiro com letras_iniciais_anteriores, depois com as letras atuais
                letras_tentar = []
                if letras_iniciais_anteriores:
                    letras_tentar.extend(
                        [l.lower() for l in letras_iniciais_anteriores]
                    )
                letras_tentar.extend([strip_accents(lemas_filtrados[0])[0].lower()])

                filtered = []
                for letra in letras_tentar:
                    filtered = [
                        m
                        for m in fuzzy_matches
                        if strip_accents(m).lower().startswith(letra)
                    ]
                    if filtered:
                        print(f"Após filtro de inicial ‘{letra}’: {filtered}")
                        lemas_filtrados = filtered
                        break
                else:
                    # se nenhum começou por nenhuma das letras, mantém todos
                    print(
                        "Nenhum fuzzy match começa por letras esperadas; mantendo todos"
                    )
                    lemas_filtrados = fuzzy_matches

                verbetes = find_lema_in_ls_dict(lemas_filtrados)
        else:
            verbetes = filtrar_por_orths_melhor_match(verbetes, lemas_filtrados)
            letras_iniciais_anteriores = letras_iniciais

        filtros = []

        for lema in lemas:
            # O restante da linha são as formas, regras, etc
            resto_linha = primeira_linha[len(lema) :].lstrip(", ").strip()
            linha_parse = lema + (", " + resto_linha if resto_linha else "")
            campos = parse_linha(linha_parse)

            filtros.extend(campos["descricao_curta"].split(" "))

        verbetes = filtra_por_tipo(verbetes, filtros=filtros)

        print(f"Verbetes encontrados para {lemas}: {len(verbetes) if verbetes else 0}")
        print(
            f"Verbetes encontrados: {json.dumps(verbetes, ensure_ascii=False, indent=2)}"
        )

        resto_linha = primeira_linha[len(lemas[0]) :].lstrip(", ").strip()
        if resto_linha:
            resto_linha = ", " + resto_linha

        campos = parse_linha(linha_parse)

        lemas_whitaker = lemas_filtrados.copy()
        lemas_whitaker.extend(
            [l for obj in verbetes for l in json.loads(obj.get("orths", "[]"))]
        )

        campos["whitaker"] = consulta_whitaker_words(lemas_whitaker)

        campos["lemas"] = lemas_filtrados
        campos["descricao"] = (campos["descricao_curta"] + "\n" + resto).strip()

        del campos["descricao_curta"]

        # Extração de abreviações
        campos["abrevs_morfossintaticas"] = extrair_sequencia_abrevs(
            campos["descricao"].lower()
        )

        print(f"Campos extraídos: {json.dumps(campos, ensure_ascii=False, indent=2)}")

        print()

        parsed_verbetes.append(campos)

        # for lema in lemas:
        #     # O restante da linha são as formas, regras, etc
        #     resto_linha = primeira_linha[len(lema) :].lstrip(", ").strip()
        #     linha_parse = lema + (", " + resto_linha if resto_linha else "")
        #     campos = parse_linha(linha_parse)
        #     campos["lema"] = lema
        #     campos["descricao"] = (campos["descricao_curta"] + "\n" + resto).strip()
        #     del campos["descricao_curta"]
        #     parsed_verbetes.append(campos)

        #     # Debug/preview:
        #     print(f"Lema: {campos['lema']}")
        #     print(f"Sinônimos: {campos['sinonimos']}")
        #     print(f"Regras: {campos['regras']}")
        #     print(f"Descrição: {campos['descricao']}")
        #     print("-" * 40)

        # input(f"Verbetes formatados: Pressione Enter para continuar...")

    with open("erros.json", "w", encoding="utf-8") as f:
        json.dump(erros_de_ordem, f, ensure_ascii=False, indent=2)
    print(f"Total de erros de ordem alfabética: {len(erros_de_ordem)}")


def extrair_verbetes(texto):
    verbetes = []
    texto += "\nfimdapagina, adv. finalizado\n"  # Garante que o último verbete seja capturado

    for m in LEMA_REGEX.finditer(texto):
        numero = (m.group(1) or "").strip()
        lema = m.group(2).strip()
        formas = m.group(3).strip()
        definicao = m.group(4).strip().replace("\n", " ")
        verbetes.append(
            {"numero": numero, "lema": lema, "formas": formas, "definicao": definicao}
        )
    return verbetes


def check_alphabetical_order(lemmas):
    """
    Recebe a lista de lemas (strings, na ordem extraída do dicionário)
    e retorna uma lista de tuplas (i, anterior, atual) para cada par fora de ordem.
    """
    errors = []
    for i in range(len(lemmas) - 1):
        a, b = lemmas[i], lemmas[i + 1]
        # compara usando a mesma lógica de noaccents/SQLite
        if noaccents(a, b) > 0:
            errors.append((i, a, b))
    return errors


def verificar_ordem_alfabetica():
    pages = carregar_paginas(DOC_NAME, DB_FILE, START_PAGE)
    todas_as_paginas = ""
    for _, raw in pages:
        txt = remover_cabecalho(raw)
        txt = remover_notas_rodape(txt)
        todas_as_paginas += (
            corrige_quebras_de_palavra_por_quebras_de_linha(txt) + "\n\n"
        )

    verbetes = extrair_verbetes(todas_as_paginas)
    lemmas = [vb["lema"] for vb in verbetes]

    erros = check_alphabetical_order(lemmas)
    if not erros:
        print("✅ Todos os lemas estão em ordem alfabética.")
    else:
        print(f"❌ {len(erros)} erros de ordenação encontrados:\n")
        for idx, prev_, curr in erros:
            print(f"  Posição {idx:5d}: “{prev_}”  ➞  “{curr}”")


if __name__ == "__main__":
    pages = carregar_paginas(DOC_NAME, DB_FILE, START_PAGE)
    headers = extrair_headers(pages)

    print("PRIMEIROS 5 CABEÇALHOS DETECTADOS:")
    for h in headers[:5]:
        print(f"Pág {h['page_num']}: {h['first_word']} ... {h['last_word']}")
    print("-" * 40)

    todas_as_paginas = ""

    if os.path.exists("completo_faria.txt"):
        with open("completo_faria.txt", "r", encoding="utf-8") as f:
            todas_as_paginas = f.read()
    else:

        for page, (page_num, raw_text) in enumerate(pages, start=1):
            # 1. Remove cabeçalho da página
            header = extrair_header(raw_text)
            clean = remover_cabecalho(raw_text)
            # 2. Remove notas de rodapé (footnotes da LLM)
            clean = remover_notas_rodape(clean)
            # 3. Extrai verbetes via regex

            clean = clean.strip("\n---\n")

            todas_as_paginas += clean + "\n\n"

            continue  # Para evitar processamento completo aqui

            verbetes = extrair_verbetes(clean)
            if (
                header
                and header["first_word"]
                and header["first_word"].upper()
                != remover_acentuacao(verbetes[0]["lema"]).upper()
            ):
                print(
                    f"ALERTA: Cabeçalho não corresponde ao primeiro lema na página {page_num}!"
                )
                # raise ValueError(
                #    f"Erro de cabeçalho na página {page_num}: {header['first_word'].upper()} != {remover_acentuacao(verbetes[0]['lema']).upper()}"
                # )

            print(f"\n==== Página {page_num} - {len(verbetes)} verbetes ====")
            if header:
                print(
                    f"Cabeçalho da página: {header['first_word']} ... {header['last_word']}"
                )
            for vb in verbetes:
                print(f"Lema: {vb['lema']}")
                print(f"Formas: {vb['formas']}")
                print(f"Definição: {vb['definicao']}")
                print("-" * 20)

            if (
                header
                and header["last_word"]
                and header["last_word"].upper()
                != remover_acentuacao(verbetes[-1]["lema"]).upper()
            ):
                print(
                    f"ALERTA: Cabeçalho não corresponde ao último lema na página {page_num}!"
                )
                raise ValueError(
                    f"Erro de cabeçalho na página {page_num}: {header['last_word'].upper()} != {remover_acentuacao(verbetes[-1]['lema']).upper()}"
                )

        todas_as_paginas = corrige_quebras_de_palavra_por_quebras_de_linha(
            todas_as_paginas
        )

        with open("completo_faria.txt", "w", encoding="utf-8") as f:
            f.write(todas_as_paginas)

    chunks = chunkenizacao_especial(todas_as_paginas)
    # print(json.dumps(chunks, ensure_ascii=False, indent=2))

    with open("chunks_faria.json", "w", encoding="utf-8") as f:
        json.dump(chunks, f, ensure_ascii=False, indent=2)

    # analise_completa(todas_as_paginas)
