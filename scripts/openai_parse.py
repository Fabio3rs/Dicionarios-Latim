import re
import sqlite3
import ollama
import json
import os
import difflib
import unicodedata
from openai import OpenAI

DB_LS = "ls_dict.sqlite"
client = OpenAI()
client.api_key = os.getenv("OPENAI_API_KEY")


db_check = sqlite3.connect("parse_check.db")
# cria tabela para checar se o texto já foi processado e o seu resultado
db_check.execute(
    """
CREATE TABLE IF NOT EXISTS parse_results (
    doc_name TEXT,
    page_num INTEGER,
    extracted_text TEXT,
    raw_text TEXT
)
"""
)
db_check.commit()


def noaccents(a, b):
    def strip(s):
        return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")

    a_norm, b_norm = strip(a), strip(b)
    return (a_norm > b_norm) - (a_norm < b_norm)


def strip_accents(s: str) -> str:
    return unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode("ascii")


def consulta_whitaker_words(palavras: list[str]):
    # path exec whitakers-words/bin/words
    # working dir whitakers-words
    import subprocess

    # Remover toda a acentuação das palavras
    palavras = [strip_accents(p) for p in palavras if p and p.lower().strip()]

    # Remover duplicatas
    palavras = set(palavras)

    # Se tiver caracteres especiais, remover
    palavras = [re.sub(r"[^a-zA-Z0-9]", "", p) for p in palavras if p]

    if len(palavras) == 0:
        return ""

    # Ordenar as palavras
    palavras = sorted(palavras)

    try:
        # print(f"Consultando whitakers-words para: {palavras}")
        result = subprocess.run(
            [
                "bash",
                "-c",
                f"echo -e '\n' * {len(palavras)} | bin/words {' '.join(palavras)}",
            ],
            capture_output=True,
            cwd="whitakers-words",
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

    return ""


conn = sqlite3.connect(DB_LS)
conn.create_collation("NOACCENTS", noaccents)
lemmas_ls_cache = set()


def find_lema_fuzzy(lema: str, cursor, cutoff=0.7):
    if len(lemmas_ls_cache) == 0:
        cursor.execute("SELECT lemma FROM ls_entries")
        todas = [row[0] for row in cursor.fetchall()]
        todas_norm = [strip_accents(x).lower() for x in todas]
        lemmas_ls_cache.update(todas_norm)

    match = difflib.get_close_matches(lema, lemmas_ls_cache, n=3, cutoff=cutoff)
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

    def try_decode_json(value):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value

    c.execute(
        f"SELECT * FROM ls_entries WHERE {where_clause}",
        [f"{lema}" for lema in lemas],
    )
    columns = [desc[0] for desc in c.description]
    result = c.fetchall()
    if result and len(result) > 0:
        return [dict(zip(columns, map(try_decode_json, row))) for row in result]

    return []


system_prompt = """Você é um lexicográfico escolar especializado em latim com a tarefa de corrigir o texto do dicionário Faria latim-português, com profundo conhecimento do Latim Clássico e Eclesiástico. Sua tarefa é analisar textos extraídos de dicionários latinos, identificar e corrigir erros comuns de OCR, e reescrever os verbetes de forma clara e precisa em português formal, utilize JSON para estruturar suas respostas.

O usuário pode enviar vários verbetes em um único prompt. Cada verbete começa com um lema ou mais lemas (palavra latina), seguido por uma vírgula, e depois a definição ou explicação. Verbete exemplo: "abucus, -i, m. (gr. abaxc) mesa de mármore ou madeira, usada para cálculos. (Cic. Mil. 16; Plin. Nat. 37, 2; Varr. R.R. 1, 7)".

Abreviações comuns do Faria:
abl. = ablativo
abs. = absoluto, ou em absoluto
absl. = absolutamente
acus. = acusativo
adj. = adjetivo
adv. = advérbio
cf. = confere, compare
comp. = comparativo
conj. = conjunção
dat. = dativo
dem. = demonstrativo
dep. = deponente
dim. = diminutivo
distrib. = distributivo
f. = feminino
freq. = frequentativo
fut. = futuro
gen. = genitivo
impf. = imperfeito
impess. = impessoal
indecl. = indeclinável
inf. = infinitivo
interj. = interjeição
interr. = interrogação, interrogativo
intr. = intransitivo
loc. = locativo

m. = masculino
n. = neutro
nom. = nominativo
num. = numeral
ord. = ordinal
part. = particípio
perf. = perfeito
pass. = passado ou passivo
pes. = pessoa
pl. = plural
prep. = preposição
prés. = presente
pr. = próprio
pron. = pronome
prev. = provérbio
reflex. = reflexivo
sg. = singular
sent. = sentido
sinc. = sincopado
subs. = substantivo
subj. = subjuntivo
superl. = superlativo
tr. = transitivo
v. = verbo
v. = veja
voc. = vocativo.

Formato esperado da resposta:
[
  {
    "lemas": ["abacus"],
    "morfologia": "-i, m.",
    "definicao": "(gr. abax) mesa de mármore ou madeira, usada para cálculos. (Cic. Mil. 16; Plin. Nat. 37, 2; Varr. R.R. 1, 7)",
    "exemplos": [
      "Cic. Mil. 16",
      "Plin. Nat. 37, 2",
      "Varr. R.R. 1, 7"
    ],
    "notas": "Corrigido erro de OCR: 'abacus' estava como 'abucus'; 'abax' estava escrito como 'abaxc'."
  },
  ...
]}

Casos como "a, ab, abs" irão ter lemas separados, ou "a, ah", e devem ser tratados como tal. Verbete exemplo: "a, ab, abs - prep. com ablativo, de, desde, por causa de, a partir de; (Cic. Fam. 1, 9; Cic. Att. 1, 1; Cic. Phil. 2, 3)".
...
"lemas": ["a", "ab", "abs"]
...
"lemas": ["a", "ah"]
...

Não precisa mencionar o nome do dicionário Faria na resposta, apenas reescreva os verbetes. Foque em corrigir erros, como confusões entre 'c' e 'e', 'l' e 'i', 'n' e 'u', etc. Se a definição for multilinha, adicione-a na chave "definicao" também. Se não houver exemplos, deixe a chave "exemplos" como uma lista vazia. Se não houver notas, deixe a chave "notas" como uma string vazia.

Instruções:
1. Separe cada verbete individualmente.
2. Identifique e corrija erros comuns de OCR/scan, como confusões entre 'c' e 'e', 'l' e 'i', etc. se estiver correto, apenas reescreva como está/copie.
3. Extraia o lema, a definição, exemplos de uso (se houver), e quaisquer notas relevantes, se a explicação for multilinha, adicione-a na chave "definicao" também.
4. Estruture cada verbete em um objeto JSON
5. Retorne uma lista JSON contendo todos os verbetes processados, não faça comentários extras.
"""

with open("chunks_faria.json", "r") as f:
    verbetes = json.load(f)

print(f"Total de verbetes a processar: {len(verbetes)}")
# response_fix = ollama.chat(
#    model="llama3:8b",
#    messages=[{"role": "user", "content": prompt_fix}],
#    options={"num_ctx": 32768},  # or any number up to the model's max (16K or 128K)
# )


def find_todos_lemas(verbete: str):
    verbetes = verbete.splitlines()

    lemas = []
    lemma_next = True

    lemas_fixos = []

    for v in verbetes:
        linha = v
        if re.match(r"^\d\.", v):
            linha = re.sub(r"^\d+\.\s*", "", v.strip())

        if linha.startswith("---------------------------------------"):
            lemma_next = True
            continue

        lema = linha.split(",")[0].strip()
        lema_esp = lema.split(" ")
        lema_esp = [l.strip() for l in lema_esp if len(l.strip()) > 0]

        if lemma_next:
            for l in lema_esp:
                l = l.strip()
                if len(l) == 0:
                    continue
                lemas.append(l)
                lemas_fixos.append(l)
                print(l)
            lemma_next = False
            continue

        if len(lema) == 0:
            continue

        lemas_fuzzy = []
        for l in lema_esp:
            l = l.strip()
            if len(l) == 0:
                continue
            lemas_fuzzy.extend(find_lema_fuzzy(l, conn.cursor(), cutoff=0.7))

        print(lemas_fuzzy)
        if len(lemas_fuzzy) > 0:
            lemas.extend(lemas_fuzzy)
            lemma_next = False
            # input("Pressione Enter para continuar...")

    letras_iniciais_lemas_fixos = set(l[0] for l in lemas_fixos if len(l) > 0)
    lemas = [l for l in lemas if len(l) > 0 and l[0] in letras_iniciais_lemas_fixos]

    print(f"Lemas finais encontrados: {lemas}")

    return set(lemas)


for i, chunk in enumerate(verbetes):
    print(f"Processando chunk {i+1}/{len(verbetes)} com {len(chunk)} caracteres...")

    # Verifica se o chunk já foi processado
    existing = db_check.execute(
        "SELECT 1 FROM parse_results WHERE doc_name = ? AND page_num = ?",
        (f"chunk_{i+1}", i + 1),
    ).fetchone()
    if existing:
        print(f"Chunk {i+1} já processado, pulando...")
        continue

    lemas = find_todos_lemas(chunk)

    dados_lema = find_lema_in_ls_dict(list(lemas))

    mensagem_user = ""

    whitaker = consulta_whitaker_words(list(lemas))
    if whitaker and len(whitaker) > 0:
        mensagem_user += "Resultados da consulta ao Whitaker's Words:\n"
        mensagem_user += whitaker + "\n\n"

    if len(dados_lema) > 0:
        mensagem_user += "Lista de possíveis verbetes relacionados encontrados do dicionário LS (que não possui erros OCR, o Lewis&Short está correto) para pesquisa de grafia, macrons e breves (diacríticos). Verbetes LS:\n\n"

    for dado in dados_lema:
        lemma = dado.get("lemma", "")
        if len(lemma) == 0:
            continue

        mensagem_user += f"Lema: {lemma}\n"
        mensagem_user += (
            f"Ortografias: {json.dumps(dado.get('orths'), ensure_ascii=False)}\n"
        )
        mensagem_user += (
            f"Classificações: {json.dumps(dado.get('itypes'), ensure_ascii=False)}\n"
        )
        mensagem_user += f"Definição: {dado.get('definition')}\n\n"

    mensagem_user += "\n\nAgora, pense em alguns passos os relacionamentos possíveis entre os verbetes acima do LS e os verbetes abaixo do dicionário Faria, corrigindo possíveis erros de OCR (do Faria) e estruturando-o e informando apenas JSON de resultado. Verbetes do Faria:\n\n"
    mensagem_user += chunk

    print(mensagem_user)

    json_success = False
    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": mensagem_user},
    ]

    # response = ollama.chat(
    #     model="llama3:8b",
    #     messages=messages,
    #     options={
    #         "num_ctx": 32768,
    #         "temperature": 0.0,
    #         "frequency_penalty": 0.0,
    #         "presence_penalty": 0.0,
    #     },  # or any number up to the model's max (16K or 128K)
    # )

    resp = client.chat.completions.create(
        model="gpt-5-mini",
        messages=messages,
        temperature=1,
        top_p=1.0,
        presence_penalty=0.0,
        frequency_penalty=0.0,
    )
    resposta_texto = resp.choices[0].message.content
    try:
        resposta_json = json.loads(resposta_texto)
        print(json.dumps(resposta_json, ensure_ascii=False, indent=2))
        with open(f"resposta_chunk_{i+1}.json", "w", encoding="utf-8") as f:
            json.dump(resposta_json, f, ensure_ascii=False, indent=2)
    except json.JSONDecodeError as e:
        print(f"Erro ao decodificar JSON na resposta do chunk {i+1}: {e}")

    # Insere no banco de Dados
    db_check.execute(
        """
    INSERT INTO parse_results (doc_name, page_num, extracted_text, raw_text)
    VALUES (?, ?, ?, ?)
    """,
        (f"chunk_{i+1}", i + 1, json.dumps(resposta_json, ensure_ascii=False), chunk),
    )
    db_check.commit()

    # input("Pressione Enter para continuar para o próximo chunk...")
