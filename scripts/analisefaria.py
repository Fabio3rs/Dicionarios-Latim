import json
import sqlite3
import re

DOC_NAME = "Dicionário - Ernesto Faria.pdf"
DB_FILE = "ocr_results.db"
START_PAGE = 7  # Verbete começa aqui

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
    return texto_corrigido.strip()


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
        "sinonimos": json.dumps(sinonimos, ensure_ascii=False) if sinonimos else None,
        "regras": json.dumps(regras, ensure_ascii=False) if regras else None,
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
    "freq.",
    "dim.",
    "comp.",
    "superl.",
    "tr.",
    "intr.",
    "m.",
    "f.",
    "n.",
    "pl.",
    "sg.",
]
ABREV_RE = re.compile(r"\b(?:" + "|".join(map(re.escape, ABREVS)) + r")\b")
TRACO_RE = re.compile(r"\s+—\s+")


def detecta_multiplos_lemas(linha):
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


def analise_completa(texto):
    texto = corrige_quebras_de_palavra_por_quebras_de_linha(texto)

    matches = list(LEMA_RE.finditer(texto))
    posicoes = [m.start() for m in matches] + [len(texto)]

    # Agora cada verbete está entre posicoes[i] e posicoes[i+1]
    verbetes = [
        texto[posicoes[i] : posicoes[i + 1]].strip() for i in range(len(posicoes) - 1)
    ]

    print(f"Total de verbetes encontrados: {len(verbetes)}\n")

    parsed_verbetes = []

    for i, verbete in enumerate(verbetes):
        linhas = verbete.splitlines()
        if not linhas:
            continue
        primeira_linha = linhas[0].strip()
        resto = "\n".join(linhas[1:]).strip() if len(linhas) > 1 else ""

        # Detecta múltiplos lemas reais
        lemas = detecta_multiplos_lemas(primeira_linha)
        for lema in lemas:
            # O restante da linha são as formas, regras, etc
            resto_linha = primeira_linha[len(lema):].lstrip(", ").strip()
            linha_parse = (lema + (", " + resto_linha if resto_linha else ""))
            campos = parse_linha(linha_parse)
            campos["lema"] = lema
            campos["descricao"] = (campos["descricao_curta"] + "\n" + resto).strip()
            del campos["descricao_curta"]
            parsed_verbetes.append(campos)

            # Debug/preview:
            print(f"Lema: {campos['lema']}")
            print(f"Sinônimos: {campos['sinonimos']}")
            print(f"Regras: {campos['regras']}")
            print(f"Descrição: {campos['descricao']}")
            print('-' * 40)


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


if __name__ == "__main__":
    pages = carregar_paginas(DOC_NAME, DB_FILE, START_PAGE)
    headers = extrair_headers(pages)

    print("PRIMEIROS 5 CABEÇALHOS DETECTADOS:")
    for h in headers[:5]:
        print(f"Pág {h['page_num']}: {h['first_word']} ... {h['last_word']}")
    print("-" * 40)

    todas_as_paginas = ""

    for page, (page_num, raw_text) in enumerate(pages, start=1):
        # 1. Remove cabeçalho da página
        header = extrair_header(raw_text)
        clean = remover_cabecalho(raw_text)
        # 2. Remove notas de rodapé (footnotes da LLM)
        clean = remover_notas_rodape(clean)
        # 3. Extrai verbetes via regex

        clean = clean.strip("\n---\n")

        todas_as_paginas += clean + "\n\n"
        verbetes = extrair_verbetes(clean)

        continue
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

    analise_completa(todas_as_paginas)
