#!/usr/bin/env python3
"""
export_dicionarios.py — Exporta dicionários SQLite para formatos legíveis

Uso:
    python export_dicionarios.py --db retificado_v2.db --format md --out faria.md
    python export_dicionarios.py --db ls_dict.db --format json --out ls.json
    python export_dicionarios.py --unified --format md --out completo.md

Formatos suportados:
    - md (Markdown humanizado)
    - json (JSON estruturado para APIs)
    - html (HTML com bootstrap styling)
    - tsv (Tab-separated values para planilhas)
"""

import argparse
import json
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional


def detect_db_type(db_path: str) -> str:
    """Detecta o tipo de dicionário pelo schema."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = conn.cursor()

    tables = {row[0] for row in cur.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    if "entry" in tables:
        # Verificar se é Faria ou LS
        cols = {row[1] for row in cur.execute("PRAGMA table_info(entry)")}
        if "tr_gloss_pt" in cols:
            return "ls"
        elif "morph_render" in cols:
            return "faria"

    if "dicionario" in tables:
        return "unified"

    conn.close()
    raise ValueError(f"Schema desconhecido em {db_path}")


def export_faria_md(db_path: str, output_path: str):
    """Exporta Faria para Markdown."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Dicionário Latino-Português — Ernesto Faria (Edição Digital Corrigida)\n\n")
        f.write(f"*Exportado em {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n\n")
        f.write("---\n\n")

        # Estatísticas
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN conf = 'high' THEN 1 ELSE 0 END) as high_conf,
                SUM(CASE WHEN conf = 'med' THEN 1 ELSE 0 END) as med_conf,
                SUM(CASE WHEN conf = 'low' THEN 1 ELSE 0 END) as low_conf,
                SUM(CASE WHEN needs_review = 1 THEN 1 ELSE 0 END) as review
            FROM entry
        """).fetchone()

        f.write(f"**Estatísticas:**\n")
        f.write(f"- Total de entradas: {stats['total']:,}\n")
        f.write(f"- Confiança alta: {stats['high_conf']:,} ({100*stats['high_conf']/stats['total']:.1f}%)\n")
        f.write(f"- Confiança média: {stats['med_conf']:,} ({100*stats['med_conf']/stats['total']:.1f}%)\n")
        f.write(f"- Confiança baixa: {stats['low_conf']:,} ({100*stats['low_conf']/stats['total']:.1f}%)\n")
        f.write(f"- Necessitam revisão: {stats['review']:,}\n\n")
        f.write("---\n\n")

        # Entradas
        query = """
            SELECT lemma, morph_render, definicao, notas, conf, needs_review
            FROM entry
            ORDER BY lemma COLLATE NOCASE
        """

        current_letter = None
        for row in conn.execute(query):
            # Cabeçalhos por letra
            first_letter = row["lemma"][0].upper() if row["lemma"] else "?"
            if first_letter != current_letter:
                current_letter = first_letter
                f.write(f"\n## {current_letter}\n\n")

            # Entrada
            f.write(f"### {row['lemma']}\n")

            if row["morph_render"]:
                f.write(f"- **Morfologia:** {row['morph_render']}\n")

            f.write(f"- **Definição:** {row['definicao']}\n")

            if row["notas"]:
                f.write(f"- **Notas:** {row['notas']}\n")

            # Badges de qualidade
            conf_badge = {"high": "✅", "med": "⚠️", "low": "❌"}.get(row["conf"], "❓")
            f.write(f"- **Confiança:** {conf_badge} {row['conf']}")

            if row["needs_review"]:
                f.write(" 🔍 *necessita revisão*")

            f.write("\n\n")

    conn.close()
    print(f"✅ Exportado Faria → {output_path} ({Path(output_path).stat().st_size / 1024 / 1024:.1f} MB)")


def export_ls_md(db_path: str, output_path: str):
    """Exporta Lewis & Short para Markdown."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Lewis & Short Latin Dictionary — Edição PT-BR\n\n")
        f.write(f"*Exportado em {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n\n")
        f.write("*Baseado no Perseus Digital Library*\n\n")
        f.write("---\n\n")

        # Estatísticas
        stats = conn.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN tr_trad_pt != '' THEN 1 ELSE 0 END) as traduzidos,
                COUNT(DISTINCT pos) as pos_count
            FROM entry
        """).fetchone()

        f.write(f"**Estatísticas:**\n")
        f.write(f"- Total de entradas: {stats['total']:,}\n")
        f.write(f"- Traduzidas para PT-BR: {stats['traduzidos']:,} ({100*stats['traduzidos']/stats['total']:.1f}%)\n")
        f.write(f"- Classes gramaticais: {stats['pos_count']}\n\n")
        f.write("---\n\n")

        # Entradas traduzidas
        query = """
            SELECT
                lemma,
                pos,
                gen_text,
                homograph_no,
                tr_gloss_pt,
                tr_trad_pt,
                tr_notas,
                tr_updated_at
            FROM entry
            WHERE tr_trad_pt != '' AND tr_trad_pt IS NOT NULL
            ORDER BY lemma_sort
        """

        current_letter = None
        for row in conn.execute(query):
            first_letter = row["lemma"][0].upper() if row["lemma"] else "?"
            if first_letter != current_letter:
                current_letter = first_letter
                f.write(f"\n## {current_letter}\n\n")

            # Lema com homógrafo
            lemma_display = row["lemma"]
            if row["homograph_no"]:
                lemma_display += f" ({row['homograph_no']})"

            f.write(f"### {lemma_display}\n")

            # Morfologia
            pos_parts = []
            if row["pos"]:
                pos_parts.append(row["pos"])
            if row["gen_text"]:
                pos_parts.append(row["gen_text"])

            if pos_parts:
                f.write(f"- **Classe:** {', '.join(pos_parts)}\n")

            # Glosas curtas
            if row["tr_gloss_pt"]:
                f.write(f"- **Glosas:** {row['tr_gloss_pt']}\n")

            # Definição completa
            f.write(f"\n{row['tr_trad_pt']}\n")

            # Notas do tradutor
            if row["tr_notas"]:
                f.write(f"\n> **Notas:** {row['tr_notas']}\n")

            f.write("\n---\n\n")

    conn.close()
    print(f"✅ Exportado LS → {output_path} ({Path(output_path).stat().st_size / 1024 / 1024:.1f} MB)")


def export_to_json(db_path: str, output_path: str, db_type: str):
    """Exporta para JSON estruturado."""
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row

    entries = []

    if db_type == "faria":
        query = "SELECT * FROM entry ORDER BY lemma COLLATE NOCASE"
        for row in conn.execute(query):
            entries.append({
                "lemma": row["lemma"],
                "morfologia": row["morph_render"],
                "definicao": row["definicao"],
                "notas": row["notas"],
                "confianca": row["conf"],
                "precisa_revisao": bool(row["needs_review"]),
            })

    elif db_type == "ls":
        query = """
            SELECT lemma, pos, gen_text, homograph_no, tr_gloss_pt, tr_trad_pt, tr_notas
            FROM entry
            WHERE tr_trad_pt != ''
            ORDER BY lemma_sort
        """
        for row in conn.execute(query):
            entries.append({
                "lemma": row["lemma"],
                "homografo": row["homograph_no"],
                "classe": row["pos"],
                "genero": row["gen_text"],
                "glosas": row["tr_gloss_pt"],
                "traducao": row["tr_trad_pt"],
                "notas": row["tr_notas"],
            })

    conn.close()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump({
            "metadata": {
                "tipo": db_type,
                "exportado_em": datetime.now().isoformat(),
                "total_entradas": len(entries),
            },
            "entradas": entries,
        }, f, ensure_ascii=False, indent=2)

    print(f"✅ Exportado JSON → {output_path} ({len(entries):,} entradas)")


def export_unified_md(output_path: str):
    """Exporta visão unificada Faria + LS."""
    db_faria = Path("retificado_v2.db")
    db_ls = Path("ls_dict.db")

    if not db_faria.exists() or not db_ls.exists():
        print("❌ Erro: bancos retificado_v2.db e ls_dict.db devem existir")
        sys.exit(1)

    conn_faria = sqlite3.connect(f"file:{db_faria}?mode=ro", uri=True)
    conn_ls = sqlite3.connect(f"file:{db_ls}?mode=ro", uri=True)
    conn_faria.row_factory = sqlite3.Row
    conn_ls.row_factory = sqlite3.Row

    # Obter todos os lemas únicos
    lemas_faria = {row[0] for row in conn_faria.execute("SELECT DISTINCT lemma FROM entry")}
    lemas_ls = {row[0] for row in conn_ls.execute("SELECT DISTINCT lemma FROM entry WHERE tr_trad_pt != ''")}
    lemas_todos = sorted(lemas_faria | lemas_ls, key=str.lower)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("# Dicionário Latino-Português Unificado\n\n")
        f.write("*Faria (Ernesto Faria) + LS (Lewis & Short traduzido)*\n\n")
        f.write(f"*Exportado em {datetime.now().strftime('%Y-%m-%d %H:%M')}*\n\n")
        f.write(f"**Total de lemas:** {len(lemas_todos):,}\n\n")
        f.write("---\n\n")

        current_letter = None
        for lemma in lemas_todos:
            first_letter = lemma[0].upper()
            if first_letter != current_letter:
                current_letter = first_letter
                f.write(f"\n## {current_letter}\n\n")

            f.write(f"### {lemma}\n\n")

            # Faria
            faria = conn_faria.execute(
                "SELECT morph_render, definicao, conf FROM entry WHERE lemma = ?",
                (lemma,)
            ).fetchone()

            if faria:
                f.write("#### 📖 Faria\n")
                if faria["morph_render"]:
                    f.write(f"- **Morfologia:** {faria['morph_render']}\n")
                f.write(f"- **Definição:** {faria['definicao']}\n")
                conf_badge = {"high": "✅", "med": "⚠️", "low": "❌"}.get(faria["conf"], "❓")
                f.write(f"- **Confiança:** {conf_badge}\n\n")

            # LS
            ls = conn_ls.execute(
                "SELECT pos, tr_trad_pt FROM entry WHERE lemma = ? AND tr_trad_pt != '' LIMIT 1",
                (lemma,)
            ).fetchone()

            if ls:
                f.write("#### 📚 Lewis & Short\n")
                if ls["pos"]:
                    f.write(f"- **Classe:** {ls['pos']}\n")
                f.write(f"- **Definição:** {ls['tr_trad_pt']}\n\n")

            if not faria and not ls:
                f.write("*Entrada não encontrada em nenhum dicionário*\n\n")

            f.write("---\n\n")

    conn_faria.close()
    conn_ls.close()

    print(f"✅ Exportado dicionário unificado → {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Exporta dicionários SQLite para formatos legíveis"
    )
    parser.add_argument(
        "--db",
        help="Caminho do banco SQLite (retificado_v2.db, ls_dict.db, etc.)"
    )
    parser.add_argument(
        "--format",
        choices=["md", "json", "html", "tsv"],
        default="md",
        help="Formato de saída (padrão: md)"
    )
    parser.add_argument(
        "--out",
        required=True,
        help="Arquivo de saída"
    )
    parser.add_argument(
        "--unified",
        action="store_true",
        help="Exportar visão unificada Faria + LS (ignora --db)"
    )
    parser.add_argument(
        "--type",
        choices=["faria", "ls", "unified"],
        help="Forçar tipo de dicionário (auto-detecta se omitido)"
    )

    args = parser.parse_args()

    # Modo unificado
    if args.unified:
        if args.format != "md":
            print("⚠️ Modo unificado suporta apenas formato MD")
            sys.exit(1)
        export_unified_md(args.out)
        return

    # Validação
    if not args.db:
        print("❌ Erro: --db é obrigatório (ou use --unified)")
        sys.exit(1)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"❌ Erro: banco não encontrado: {db_path}")
        sys.exit(1)

    # Detectar tipo
    db_type = args.type or detect_db_type(str(db_path))
    print(f"📂 Tipo detectado: {db_type}")

    # Exportar
    if args.format == "md":
        if db_type == "faria":
            export_faria_md(str(db_path), args.out)
        elif db_type == "ls":
            export_ls_md(str(db_path), args.out)
        else:
            print(f"❌ Formato MD não implementado para {db_type}")
            sys.exit(1)

    elif args.format == "json":
        export_to_json(str(db_path), args.out, db_type)

    else:
        print(f"❌ Formato {args.format} ainda não implementado")
        sys.exit(1)


if __name__ == "__main__":
    main()
