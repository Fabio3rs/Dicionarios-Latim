import sqlite3
import xml.etree.ElementTree as ET
import unidecode
import re
import json

XML_FILE = "lat.ls.perseus-eng2.xml"
DB_FILE = "ls_dict.sqlite"


def normalize(word):
    return unidecode.unidecode(word or "").lower()


def extract_orths(entry):
    return [orth.text.strip() for orth in entry.findall(".//orth") if orth.text]


def extract_itypes(entry):
    return [itype.text.strip() for itype in entry.findall(".//itype") if itype.text]


def extract_explicit_forms_in_senses(entry):
    forms = []
    for sense in entry.findall(".//sense"):
        text = "".join(sense.itertext())
        # Formatos tipo "legentum, <bibl" ou "lectum, <bibl"
        for match in re.finditer(r"([a-zA-Z\-]+),\s*<bibl", text):
            forms.append(match.group(1))
        # Outra tentativa: "palavra," sozinha
        for match in re.finditer(r"([a-zA-Z\-]+),", text):
            forms.append(match.group(1))
    return forms


def extract_all_unique(items):
    seen = set()
    result = []
    for i in items:
        if i and i.strip() and i not in seen:
            seen.add(i)
            result.append(i)
    return result


# Banco de Dados
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()
cur.execute(
    """
CREATE TABLE IF NOT EXISTS ls_entries (
    id TEXT PRIMARY KEY,
    orths TEXT,
    itypes TEXT,
    explicit_forms TEXT,
    lemma TEXT,
    pos TEXT,
    definition TEXT,
    search_blob TEXT
)
"""
)
cur.execute("CREATE INDEX IF NOT EXISTS idx_search_blob ON ls_entries(search_blob)")

# Parse XML
context = ET.iterparse(XML_FILE, events=("end",))
total = 0

for event, elem in context:
    if elem.tag == "entryFree":
        entry_id = elem.attrib.get("id", "")
        orths = extract_all_unique(extract_orths(elem))
        itypes = extract_all_unique(extract_itypes(elem))
        explicit_forms = extract_all_unique(extract_explicit_forms_in_senses(elem))
        lemma = orths[0] if orths else ""
        pos_el = elem.find("pos")
        pos = pos_el.text.strip() if pos_el is not None and pos_el.text else ""
        def_parts = []
        for sense in elem.findall(".//sense"):
            def_parts.append("".join(sense.itertext()).strip())
        if not def_parts:
            def_parts = ["".join(elem.itertext()).strip()]
        definition = "\n".join(def_parts)
        # Serializa como JSON
        orths_json = json.dumps(orths, ensure_ascii=False)
        itypes_json = json.dumps(itypes, ensure_ascii=False)
        explicit_forms_json = json.dumps(explicit_forms, ensure_ascii=False)
        # Search_blob
        search_forms = set()
        for form in orths + itypes + explicit_forms:
            for word in re.split(r"[ ,;]+", form):
                if word and len(word) > 2:
                    search_forms.add(normalize(word))
        search_blob = " ".join(sorted(search_forms))
        cur.execute(
            """
            INSERT OR IGNORE INTO ls_entries
                (id, orths, itypes, explicit_forms, lemma, pos, definition, search_blob)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                entry_id,
                orths_json,
                itypes_json,
                explicit_forms_json,
                lemma,
                pos,
                definition,
                search_blob,
            ),
        )
        total += 1
        elem.clear()
        if total % 10000 == 0:
            print(f"{total} verbetes...")

conn.commit()
print(f"Pronto! {total} entradas importadas em {DB_FILE}.")

# Exemplo para leitura em Python:
# import json
# row = cur.execute("SELECT orths FROM ls_entries WHERE ...").fetchone()
# orths_list = json.loads(row[0])
