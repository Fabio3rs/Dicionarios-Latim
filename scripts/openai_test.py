import os, io, base64, sqlite3
from pdf2image import convert_from_path
from pdfminer.high_level import extract_text
from openai import OpenAI

# Configuração da API
client = OpenAI()
client.api_key = os.getenv("OPENAI_API_KEY")

# Banco de dados SQLite
conn = sqlite3.connect("ocr_results.db")
conn.execute(
    """
CREATE TABLE IF NOT EXISTS pages (
    doc_name TEXT,
    page_num INTEGER,
    extracted_text TEXT,
    raw_text TEXT
)
"""
)
conn.commit()


def image_to_base64(img):
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    return base64.b64encode(buf.getvalue()).decode()


def ocr_page_with_context(img_b64, prev_text):
    messages = [
        {
            "role": "system",
            "content": (
                "You are an automated OCR assistant. Transcribe the scanned text exactly as it appears, left offset preserved."
                " NEVER repeat lines or words. NEVER attempt to complete, infer, or fill in gaps using your own knowledge or previous text."
                " If any word, character, or line is illegible, mark with (*) and explain only at the end. Do not guess or invent missing parts."
                " If the same word or line appears multiple times in the image, transcribe each only as it appears, never more."
                " Only output the raw transcription and the footnotes for (*). No introductions, no conclusions, no rephrasing, no translation, no summaries."
                " Be literal, even if the result is incomplete or fragmented."
            )
        }
    ]
    if prev_text:
        shortenedarr = prev_text.rsplit(" ", 10000)
        if len(shortenedarr) > 10000:
            short = shortenedarr[-10000:]
        else:
            short = prev_text
        messages.append(
            {"role": "user", "content": f"Contexto da página anterior:\n{short}"}
        )

    content = [
        {
            "type": "text",
            "text": (
                "Você é um assistente de OCR para dicionários bilíngues e glossários. "
                "Transcreva exatamente como impresso, mantendo quebras de linha e colunas, "
                "e PRESERVE todos os espaços à esquerda de cada linha (indentação). "
                "Se a linha começa SEM espaço, pode ser início de novo verbete. Se começa com espaço(s), é continuação do anterior. "
                "Não una linhas nem quebre linhas. Não corrija, não deduza. "
                "Transcreva todos os sinais diacríticos latinos como macrons (¯) e breves (˘). "
                "Se o sinal não está claro, marque com (*). Não repita nada."
            ),
        },
        {"type": "image_url", "image_url": {"url": f"data:image/png;base64,{img_b64}"}},
    ]

    messages.append({"role": "user", "content": content})

    resp = client.chat.completions.create(
        model="gpt-4.1",
        messages=messages,
        temperature=0.05,
        top_p=1.0,
        presence_penalty=0.0,
        frequency_penalty=0.0,
    )
    return resp.choices[0].message.content


def process_pdf(pdf_path, start_page=1, end_page: int | None = None):
    # Checa a última página do PDF presente no banco para ser o contexto inicial
    # Busca todas as páginas já processadas para identificar gaps
    processed_pages = conn.execute(
        "SELECT page_num FROM pages WHERE doc_name = ? ORDER BY page_num ASC",
        (os.path.basename(pdf_path),),
    ).fetchall()
    processed_pages = [row[0] for row in processed_pages]

    # Identifica gaps entre os números de página já processados
    gaps = []
    if processed_pages:
        # Verifica gaps antes do primeiro page_num processado
        if processed_pages[0] > start_page:
            gaps.extend(range(start_page, processed_pages[0]))
        # Verifica gaps entre páginas processadas
        for idx in range(1, len(processed_pages)):
            prev_page = processed_pages[idx - 1]
            curr_page = processed_pages[idx]
            if curr_page - prev_page > 1:
                gaps.extend(range(prev_page + 1, curr_page))
    print(f"Gaps de páginas processadas: {gaps}")
    # Seleciona a última página processada antes do próximo gap (ou última processada se não houver gap)
    if gaps and gaps[0] >= start_page:
        # Pega o menor gap e seleciona a página imediatamente anterior ao gap
        gap_start = gaps[0]
        selected = conn.execute(
            "SELECT extracted_text, page_num FROM pages WHERE doc_name = ? AND page_num = ?",
            (os.path.basename(pdf_path), gap_start - 1),
        ).fetchone()
    else:
        # Se não houver gap, seleciona a última página processada normalmente
        selected = conn.execute(
            "SELECT extracted_text, page_num FROM pages WHERE doc_name = ? ORDER BY page_num DESC LIMIT 1",
            (os.path.basename(pdf_path),),
        ).fetchone()

    prev_text = selected[0] if selected else None
    start_page = selected[1] + 1 if selected else start_page

    print(
        f"Processando PDF: {pdf_path} de página {start_page} até {end_page or 'fim'}..."
    )

    images = convert_from_path(
        pdf_path,
        dpi=300,
        first_page=start_page,
        last_page=end_page,
        thread_count=os.cpu_count(),
    )

    for i, img in enumerate(images, start=start_page):
        print(f"Processando página {i}...")

        if conn.execute(
            "SELECT COUNT(*) FROM pages WHERE doc_name = ? AND page_num = ?",
            (os.path.basename(pdf_path), i),
        ).fetchone()[0] > 0:
            print(f"Página {i} já processada, pulando.")

            selected = conn.execute(
                "SELECT extracted_text, page_num FROM pages WHERE doc_name = ? AND page_num = ?",
                (os.path.basename(pdf_path), i),
            ).fetchone()

            prev_text = selected[0] if selected else None
            continue

        img_b64 = image_to_base64(img)

        # opcional: extrair texto via pdfminer para comparação
        raw = ""
        try:
            raw = extract_text(pdf_path, page_numbers=[i - 1]) or ""
        except Exception as e:
            print(f"Erro ao extrair texto bruto da página {i}: {e}")
            raw = "**Erro ao extrair texto bruto da página**"
        result_text = ocr_page_with_context(img_b64, prev_text)

        conn.execute(
            "INSERT INTO pages (doc_name, page_num, extracted_text, raw_text) VALUES (?, ?, ?, ?)",
            (os.path.basename(pdf_path), i, result_text, raw),
        )
        conn.commit()
        prev_text = result_text  # contexto para próxima página

        print(result_text[:500] + "...\n")


if __name__ == "__main__":
    print("Iniciando processamento do PDF...")
    process_pdf("Dicionário - Ernesto Faria.pdf", start_page=1, end_page=None)
    conn.close()
