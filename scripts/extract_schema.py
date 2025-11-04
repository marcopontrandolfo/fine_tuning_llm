"""
Estrae uno schema canonico (tabelle, colonne, foreign keys) da dataset/northwind.sql.
Output: dataset/northwind_schema_canonical.json
"""

from __future__ import annotations

import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT / "dataset"
SQL_FILE = DATASET_DIR / "northwind.sql"
OUT_JSON = DATASET_DIR / "northwind_schema_canonical.json"


def strip_comments(sql: str) -> str:
	sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.S)
	sql = re.sub(r"--.*?$", " ", sql, flags=re.M)
	return sql


def unquote_ident(ident: str) -> str:
	ident = ident.strip()
	if "." in ident:
		ident = ident.split(".")[-1]
	if ident.startswith("[") and ident.endswith("]"):
		ident = ident[1:-1]
	if ident.startswith("`") and ident.endswith("`"):
		ident = ident[1:-1]
	if ident.startswith('"') and ident.endswith('"'):
		ident = ident[1:-1]
	return ident.strip()


def find_matching_paren(s: str, start_idx: int) -> int:
	depth = 0
	for i in range(start_idx, len(s)):
		ch = s[i]
		if ch == "(":
			depth += 1
		elif ch == ")":
			depth -= 1
			if depth == 0:
				return i
	return -1


def split_top_level_commas(s: str) -> list[str]:
	parts = []
	depth = 0
	current = []
	for ch in s:
		if ch == "(":
			depth += 1
			current.append(ch)
		elif ch == ")":
			depth -= 1
			current.append(ch)
		elif ch == "," and depth == 0:
			parts.append("".join(current).strip())
			current = []
		else:
			current.append(ch)
	tail = "".join(current).strip()
	if tail:
		parts.append(tail)
	return parts


def parse_fk(item: str):
	m = re.search(
		r"FOREIGN\s+KEY\s*\(([^\)]*)\)\s*REFERENCES\s+([^\s\(]+)\s*\(([^\)]*)\)",
		item,
		flags=re.I | re.S,
	)
	if not m:
		return None
	cols = [unquote_ident(c) for c in re.split(r"\s*,\s*", m.group(1).strip()) if c.strip()]
	ref_table = unquote_ident(m.group(2).strip())
	ref_cols = [unquote_ident(c) for c in re.split(r"\s*,\s*", m.group(3).strip()) if c.strip()]
	return cols, ref_table, ref_cols


def parse_create_table_blocks(sql: str) -> list[tuple[str, str]]:
	blocks: list[tuple[str, str]] = []
	for m in re.finditer(r"CREATE\s+TABLE\s+", sql, flags=re.I):
		start = m.end()
		paren_idx = sql.find("(", start)
		if paren_idx == -1:
			continue
		header = sql[start:paren_idx].strip()
		table_name = unquote_ident(header.split()[0]) if header else ""
		end = find_matching_paren(sql, paren_idx)
		if end == -1:
			continue
		content = sql[paren_idx + 1 : end]
		blocks.append((table_name, content))
	return blocks


def extract_schema(sql_text: str) -> dict:
	sql_text = strip_comments(sql_text)
	tables: dict[str, list[str]] = {}
	fks: list[dict] = []
	for tname, content in parse_create_table_blocks(sql_text):
		if not tname:
			continue
		cols: list[str] = []
		items = split_top_level_commas(content)
		for it in items:
			it_strip = it.strip()
			if re.match(r"^(CONSTRAINT|PRIMARY|FOREIGN|UNIQUE|CHECK|INDEX)\b", it_strip, flags=re.I):
				fk = parse_fk(it_strip)
				if fk:
					col_list, ref_table, ref_cols = fk
					fks.append({
						"table": tname,
						"columns": col_list,
						"ref_table": ref_table,
						"ref_columns": ref_cols,
					})
				continue
			m = re.match(r"^([\[\]`\"A-Za-z0-9_\.]+)", it_strip)
			if not m:
				continue
			col = unquote_ident(m.group(1))
			if col:
				cols.append(col)
		tables[tname] = cols
	return {"tables": tables, "foreign_keys": fks}


def main():
	if not SQL_FILE.exists():
		raise FileNotFoundError(f"File SQL non trovato: {SQL_FILE}")
	sql_text = SQL_FILE.read_text(encoding="utf-8", errors="ignore")
	schema = extract_schema(sql_text)
	OUT_JSON.write_text(json.dumps(schema, ensure_ascii=False, indent=2), encoding="utf-8")
	print(f"Schema estratto in: {OUT_JSON}")


if __name__ == "__main__":
	main()

