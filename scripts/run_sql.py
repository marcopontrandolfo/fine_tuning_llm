import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import re
import mysql.connector


def _sanitize_single_statement(sql: str) -> str:
    """Rende tollerante il pattern comune 'WITH ...; SELECT ...;' unendolo in un singolo statement.
    Non tocca altri casi per evitare side-effect. Aggiunge il ';' finale se assente."""
    raw = sql.strip()
    if not raw:
        return raw
    parts = [p.strip() for p in raw.split(';') if p.strip()]
    if len(parts) == 2 and parts[0].lower().startswith('with') and parts[1].lower().startswith('select'):
        merged = parts[0] + '\n' + parts[1]
        if not merged.endswith(';'):
            merged += ';'
        return merged
    # Se già singolo statement, assicurati del ';' finale
    if len(parts) == 1 and not raw.endswith(';'):
        return raw + ';'
    return sql


def ensure_select_only(sql: str) -> str:
    sql = _sanitize_single_statement(sql)
    s = sql.strip().lower()
    if not (s.startswith("select") or s.startswith("with")):
        raise ValueError("Sono permesse solo query SELECT (anche con CTE).")
    stripped = sql.strip()
    if ";" in stripped[:-1]:
        raise ValueError("La query non deve contenere più statement.")
    return sql


def _build_count_sql(stmt: str) -> str:
    body = _strip_leading_comments(stmt).rstrip().rstrip(";")
    if not body:
        return "SELECT 0"
    low = body.lower()
    if low.startswith("select"):
        return f"SELECT COUNT(*) FROM ( {body} ) AS _t"
    if low.startswith("with"):
        # Heuristic: split CTE part and main SELECT at the last 'select'
        idx = low.rfind("select")
        if idx == -1:
            return f"SELECT COUNT(*) FROM ( {body} ) AS _t"
        cte_part = body[:idx]
        main_select = body[idx:]
        return f"{cte_part} SELECT COUNT(*) FROM ( {main_select} ) AS _t"
    # Fallback
    return f"SELECT COUNT(*) FROM ( {body} ) AS _t"


def run(sql: str, preview: int = 20):
    sql = ensure_select_only(sql)

    load_dotenv()
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DB", "Northwind")

    conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
    try:
        cur = conn.cursor(buffered=True)
        cur.execute(sql)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchmany(preview)

        # Stampa anteprima
        if cols:
            print(" | ".join(cols))
        for r in rows:
            print(" | ".join(str(x) for x in r))

        # Conteggio totale righe
        count_sql = _build_count_sql(sql)
        cur2 = conn.cursor(buffered=True)
        cur2.execute(count_sql)
        total = cur2.fetchone()[0]
        cur2.close()
        cur.close()
        print(f"\nTotale righe: {total}")
    finally:
        conn.close()


def _connect_from_env():
    load_dotenv()
    host = os.getenv("MYSQL_HOST", "localhost")
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "")
    database = os.getenv("MYSQL_DB", "Northwind")
    return mysql.connector.connect(host=host, user=user, password=password, database=database)


def _strip_leading_comments(sql: str) -> str:
    s = sql.lstrip()
    while True:
        if s.startswith("--"):
            nl = s.find("\n")
            if nl == -1:
                return ""
            s = s[nl + 1 :].lstrip()
            continue
        if s.startswith("/*"):
            end = s.find("*/")
            if end == -1:
                return ""
            s = s[end + 2 :].lstrip()
            continue
        break
    return s


def _first_token(sql: str) -> str:
    s = _strip_leading_comments(sql)
    if not s:
        return ""
    s_lower = s.lower().lstrip()
    if s_lower.startswith("with"):
        return "with"
    if s_lower.startswith("select"):
        return "select"
    if s_lower.startswith("set"):
        return "set"
    return s_lower.split(None, 1)[0] if s_lower.split() else ""


def run_script(sql_script: str, preview: int = 10, echo: bool = False, stop_on_error: bool = False):
    """Esegue uno script SQL multi-statement.
    - Per ogni SELECT stampa anteprima e totale righe
    - Altri statement (SET, etc.) vengono eseguiti senza output
    """
    conn = _connect_from_env()
    # Abilita autocommit per comandi SET e simili
    try:
        conn.autocommit = True
    except Exception:
        pass
    try:
        cur = conn.cursor(buffered=True)
        # Split semplice per ';' (assumiamo file controllato senza ';' in stringhe)
        statements = [stmt.strip() for stmt in sql_script.split(";") if stmt.strip()]
        for i, stmt in enumerate(statements, 1):
            tok = _first_token(stmt)
            if echo:
                preview_sql = (stmt[:140] + ("..." if len(stmt) > 140 else "")).replace("\n", " ")
                print(f"\n>> [{i}] {preview_sql}")
            try:
                # Sanifica pattern WITH ...; SELECT ...; per gli script
                stmt_exec = _sanitize_single_statement(stmt)
                cur.execute(stmt_exec)
                if tok in ("select", "with"):
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchmany(preview)
                    print(f"\n-- Result {i} --")
                    if cols:
                        print(" | ".join(cols))
                    for r in rows:
                        print(" | ".join(str(x) for x in r))
                    # count totale
                    count_sql = _build_count_sql(stmt)
                    cur2 = conn.cursor(buffered=True)
                    cur2.execute(count_sql)
                    total = cur2.fetchone()[0]
                    cur2.close()
                    print(f"Totale righe: {total}")
            except Exception as e:
                print(f"\n!! Errore nello statement {i}: {e}", file=sys.stderr)
                if stop_on_error:
                    raise
        cur.close()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Esegue query SELECT su MySQL (singola) o uno script multi-statement")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--sql-file", type=str, help="Percorso file .sql contenente UNA sola SELECT (anche con CTE)")
    g.add_argument("--sql", type=str, help="Query SELECT da eseguire (tra virgolette)")
    g.add_argument("--script-file", type=str, help="Percorso file .sql con PIÙ statement (es. SET + più SELECT)")
    parser.add_argument("--preview", type=int, default=20, help="Numero massimo di righe di anteprima per SELECT (default 20)")
    parser.add_argument("--echo", action="store_true", help="Stampa il riepilogo di ogni statement eseguito nello script")
    parser.add_argument("--stop-on-error", action="store_true", help="Interrompe l'esecuzione dello script al primo errore")
    args = parser.parse_args()

    if args.script_file:
        p = Path(args.script_file)
        if not p.exists():
            print(f"File non trovato: {p}", file=sys.stderr)
            sys.exit(1)
        sql_script = p.read_text(encoding="utf-8")
        run_script(sql_script, preview=args.preview, echo=args.echo, stop_on_error=args.stop_on_error)
        return

    if args.sql_file:
        p = Path(args.sql_file)
        if not p.exists():
            print(f"File non trovato: {p}", file=sys.stderr)
            sys.exit(1)
        sql = p.read_text(encoding="utf-8")
    else:
        sql = args.sql

    run(sql, preview=args.preview)


if __name__ == "__main__":
    main()
