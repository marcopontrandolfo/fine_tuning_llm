import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
import mysql.connector


def ensure_select_only(sql: str):
    s = sql.strip().lower()
    if not s.startswith("select"):
        raise ValueError("Sono permesse solo query SELECT.")
    # Evita più statement nella stessa esecuzione
    if ";" in sql.strip()[:-1]:
        raise ValueError("La query non deve contenere più statement.")


def run(sql: str, preview: int = 20):
    ensure_select_only(sql)

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
        count_sql = f"SELECT COUNT(*) FROM ( {sql.rstrip(';')} ) AS _t"
        cur2 = conn.cursor(buffered=True)
        cur2.execute(count_sql)
        total = cur2.fetchone()[0]
        cur2.close()
        cur.close()
        print(f"\nTotale righe: {total}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Esegue una query SELECT su MySQL e mostra anteprima + totale righe")
    g = parser.add_mutually_exclusive_group(required=True)
    g.add_argument("--sql-file", type=str, help="Percorso file .sql contenente una singola SELECT")
    g.add_argument("--sql", type=str, help="Query SELECT da eseguire (tra virgolette)")
    parser.add_argument("--preview", type=int, default=20, help="Numero massimo di righe di anteprima (default 20)")
    args = parser.parse_args()

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
