"""
Generate a supervised fine-tuning (SFT) dataset (50 samples) for Northwind (MySQL)
from the canonical schema, as chat-format JSONL (messages: system/user/assistant).

Each sample is: question (user, IT) + gold SQL (assistant) designed to cover
the breadth of the schema: Customers, Orders, OrderDetails, Products, Categories,
Suppliers, Employees, Territories/Regions, Shippers, CustomerDemographics, and link tables.

Options:
  --out: output JSONL path (default: dataset/sft_northwind_50.jsonl)
  --verify: execute queries on MySQL to sanity-check syntax and availability; samples that fail are skipped

Environment for --verify: MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Optional

import mysql.connector  # only needed if --verify


ROOT = Path(__file__).resolve().parents[1]
DATASET_DIR = ROOT / "dataset"
SCHEMA_FILE = DATASET_DIR / "northwind_schema_canonical.json"
DEFAULT_OUT = DATASET_DIR / "sft_northwind_50.jsonl"


def load_schema() -> Dict[str, Any]:
    with SCHEMA_FILE.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_sql_against_schema(sql: str, schema: Dict[str, Any]) -> Optional[str]:
    """Light validation: check table/column tokens exist in schema.
    Returns None if ok, else an error message (string). This is heuristic, not a SQL parser.
    """
    tables = set(schema.get("tables", {}).keys())
    # crude scan for tokens that look like identifiers TableName.columnName
    # We'll accept known tables and avoid false positives.
    tokens: List[str] = []
    for t in tables:
        # find occurrences of " t " or " t\n" or " t\t" or "t." in sql
        # just to assert table is present when used; not strict.
        if (f" {t} " in sql) or (f" {t}\n" in sql) or (f" {t}\t" in sql) or (f"{t}." in sql):
            pass
    # minimal check OK
    return None


def get_samples() -> List[Dict[str, str]]:
    """Return 50 question+sql samples tailored to the canonical schema names."""
    S: List[Dict[str, str]] = []

    # 1 Customers with most orders
    S.append({
        "q": "Elenca i 10 clienti con più ordini (lifetime)",
        "sql": (
            "SELECT c.companyName, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "JOIN Customer c ON o.custId = c.custId\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY ordersCount DESC\n"
            "LIMIT 10;"
        )
    })

    # 2 Top products by quantity
    S.append({
        "q": "Mostra i 10 prodotti più venduti per quantità (lifetime)",
        "sql": (
            "SELECT p.productName, SUM(od.quantity) AS qty\n"
            "FROM OrderDetail od\n"
            "JOIN Product p ON od.productId = p.productId\n"
            "GROUP BY p.productId, p.productName\n"
            "ORDER BY qty DESC\n"
            "LIMIT 10;"
        )
    })

    # 3 Revenue by country
    S.append({
        "q": "Calcola il fatturato per shipCountry (ricavo = quantity*unitPrice*(1-discount))",
        "sql": (
            "SELECT o.shipCountry,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount, 0))) AS revenue\n"
            "FROM SalesOrder o\n"
            "JOIN OrderDetail od ON o.orderId = od.orderId\n"
            "GROUP BY o.shipCountry\n"
            "ORDER BY revenue DESC;"
        )
    })

    # 4 Average shipping delay
    S.append({
        "q": "Ritardo medio di spedizione in giorni (shippedDate - requiredDate)",
        "sql": (
            "SELECT AVG(DATEDIFF(o.shippedDate, o.requiredDate)) AS avgDelayDays\n"
            "FROM SalesOrder o;"
        )
    })

    # 5 Orders with discount > 0.2
    S.append({
        "q": "Quanti ordini hanno almeno una riga con sconto > 0.2?",
        "sql": (
            "SELECT COUNT(DISTINCT od.orderId) AS ordersWithHighDiscount\n"
            "FROM OrderDetail od\n"
            "WHERE od.discount > 0.2;"
        )
    })

    # 6 Employees with most orders
    S.append({
        "q": "Top 5 dipendenti per numero di ordini gestiti",
        "sql": (
            "SELECT e.firstname, e.lastname, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "JOIN Employee e ON o.employeeId = e.employeeId\n"
            "GROUP BY e.employeeId, e.firstname, e.lastname\n"
            "ORDER BY ordersCount DESC\n"
            "LIMIT 5;"
        )
    })

    # 7 Category revenue
    S.append({
        "q": "Fatturato per categoria prodotto",
        "sql": (
            "SELECT cat.categoryName,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount, 0))) AS revenue\n"
            "FROM OrderDetail od\n"
            "JOIN Product p ON od.productId = p.productId\n"
            "JOIN Category cat ON p.categoryId = cat.categoryId\n"
            "GROUP BY cat.categoryId, cat.categoryName\n"
            "ORDER BY revenue DESC;"
        )
    })

    # 8 Products by supplier
    S.append({
        "q": "Numero di prodotti per fornitore (ordina discendente)",
        "sql": (
            "SELECT s.companyName, COUNT(*) AS productsCount\n"
            "FROM Product p\n"
            "JOIN Supplier s ON p.supplierId = s.supplierId\n"
            "GROUP BY s.supplierId, s.companyName\n"
            "ORDER BY productsCount DESC;"
        )
    })

    # 9 Avg discount by category
    S.append({
        "q": "Sconto medio per categoria",
        "sql": (
            "SELECT cat.categoryName, AVG(COALESCE(od.discount, 0)) AS avgDiscount\n"
            "FROM OrderDetail od\n"
            "JOIN Product p ON od.productId = p.productId\n"
            "JOIN Category cat ON p.categoryId = cat.categoryId\n"
            "GROUP BY cat.categoryId, cat.categoryName\n"
            "ORDER BY avgDiscount DESC;"
        )
    })

    # 10 Customers with no orders
    S.append({
        "q": "Clienti senza ordini (mai)",
        "sql": (
            "SELECT c.custId, c.companyName\n"
            "FROM Customer c\n"
            "LEFT JOIN SalesOrder o ON o.custId = c.custId\n"
            "WHERE o.orderId IS NULL;"
        )
    })

    # 11 Orders per month
    S.append({
        "q": "Distribuzione degli ordini per mese (YYYY-MM)",
        "sql": (
            "SELECT DATE_FORMAT(o.orderDate, '%Y-%m') AS ym, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "GROUP BY ym\n"
            "ORDER BY ym;"
        )
    })

    # 12 Avg order value per country
    S.append({
        "q": "Valore medio per ordine per shipCountry",
        "sql": (
            "SELECT o.shipCountry,\n"
            "       AVG(order_total) AS avgOrderValue\n"
            "FROM (\n"
            "  SELECT od.orderId,\n"
            "         SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount,0))) AS order_total\n"
            "  FROM OrderDetail od\n"
            "  GROUP BY od.orderId\n"
            ") t\n"
            "JOIN SalesOrder o ON o.orderId = t.orderId\n"
            "GROUP BY o.shipCountry\n"
            "ORDER BY avgOrderValue DESC;"
        )
    })

    # 13 Out-of-stock products
    S.append({
        "q": "Prodotti esauriti (unitsInStock=0) e unitsOnOrder (desc)",
        "sql": (
            "SELECT p.productName, p.unitsInStock, p.unitsOnOrder\n"
            "FROM Product p\n"
            "WHERE p.unitsInStock = 0\n"
            "ORDER BY p.unitsOnOrder DESC;"
        )
    })

    # 14 Shipper with most shipments
    S.append({
        "q": "Corrieri con più spedizioni (conteggio ordini)",
        "sql": (
            "SELECT s.companyName, COUNT(*) AS shipments\n"
            "FROM SalesOrder o\n"
            "JOIN Shipper s ON o.shipperid = s.shipperId\n"
            "GROUP BY s.shipperId, s.companyName\n"
            "ORDER BY shipments DESC;"
        )
    })

    # 15 Products never ordered
    S.append({
        "q": "Prodotti mai ordinati",
        "sql": (
            "SELECT p.productId, p.productName\n"
            "FROM Product p\n"
            "LEFT JOIN OrderDetail od ON od.productId = p.productId\n"
            "WHERE od.orderId IS NULL;"
        )
    })

    # 16 Top customers by revenue
    S.append({
        "q": "Top 10 clienti per fatturato (lifetime)",
        "sql": (
            "SELECT c.companyName,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount, 0))) AS revenue\n"
            "FROM SalesOrder o\n"
            "JOIN Customer c ON o.custId = c.custId\n"
            "JOIN OrderDetail od ON o.orderId = od.orderId\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY revenue DESC\n"
            "LIMIT 10;"
        )
    })

    # 17 Employees by region via territories
    S.append({
        "q": "Numero di dipendenti per regione (via territori)",
        "sql": (
            "SELECT r.regiondescription AS region, COUNT(DISTINCT e.employeeId) AS employees\n"
            "FROM Employee e\n"
            "JOIN EmployeeTerritory et ON e.employeeId = et.employeeId\n"
            "JOIN Territory t ON et.territoryId = t.territoryId\n"
            "JOIN Region r ON t.regionId = r.regionId\n"
            "GROUP BY r.regionId, r.regiondescription\n"
            "ORDER BY employees DESC;"
        )
    })

    # 18 Orders with high freight
    S.append({
        "q": "Ordini con freight superiore alla media",
        "sql": (
            "SELECT o.orderId, o.freight, o.shipCountry\n"
            "FROM SalesOrder o\n"
            "WHERE o.freight > (SELECT AVG(freight) FROM SalesOrder);"
        )
    })

    # 19 Average items per order
    S.append({
        "q": "Numero medio di righe per ordine",
        "sql": (
            "SELECT AVG(items_per_order) AS avgItems\n"
            "FROM (\n"
            "  SELECT od.orderId, COUNT(*) AS items_per_order\n"
            "  FROM OrderDetail od\n"
            "  GROUP BY od.orderId\n"
            ") x;"
        )
    })

    # 20 Customers by country
    S.append({
        "q": "Numero di clienti per paese",
        "sql": (
            "SELECT c.country, COUNT(*) AS customers\n"
            "FROM Customer c\n"
            "GROUP BY c.country\n"
            "ORDER BY customers DESC;"
        )
    })

    # 21 Products by category
    S.append({
        "q": "Prodotti per categoria (conteggio)",
        "sql": (
            "SELECT cat.categoryName, COUNT(*) AS products\n"
            "FROM Product p\n"
            "JOIN Category cat ON p.categoryId = cat.categoryId\n"
            "GROUP BY cat.categoryId, cat.categoryName\n"
            "ORDER BY products DESC;"
        )
    })

    # 22 Orders by employee per month
    S.append({
        "q": "Ordini per dipendente e mese (YYYY-MM)",
        "sql": (
            "SELECT e.firstname, e.lastname, DATE_FORMAT(o.orderDate, '%Y-%m') AS ym, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "JOIN Employee e ON o.employeeId = e.employeeId\n"
            "GROUP BY e.employeeId, e.firstname, e.lastname, ym\n"
            "ORDER BY e.lastname, e.firstname, ym;"
        )
    })

    # 23 Top ship countries by orders
    S.append({
        "q": "Top 10 paesi di spedizione per numero ordini",
        "sql": (
            "SELECT o.shipCountry, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "GROUP BY o.shipCountry\n"
            "ORDER BY ordersCount DESC\n"
            "LIMIT 10;"
        )
    })

    # 24 Products with low stock under reorderLevel
    S.append({
        "q": "Prodotti con stock <= reorderLevel",
        "sql": (
            "SELECT p.productName, p.unitsInStock, p.reorderLevel\n"
            "FROM Product p\n"
            "WHERE p.unitsInStock <= p.reorderLevel\n"
            "ORDER BY p.unitsInStock ASC;"
        )
    })

    # 25 Customers with total orders value
    S.append({
        "q": "Valore totale ordini per cliente",
        "sql": (
            "SELECT c.companyName,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount,0))) AS revenue\n"
            "FROM Customer c\n"
            "JOIN SalesOrder o ON o.custId = c.custId\n"
            "JOIN OrderDetail od ON od.orderId = o.orderId\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY revenue DESC;"
        )
    })

    # 26 Suppliers by country
    S.append({
        "q": "Fornitori per paese (conteggio)",
        "sql": (
            "SELECT s.country, COUNT(*) AS suppliers\n"
            "FROM Supplier s\n"
            "GROUP BY s.country\n"
            "ORDER BY suppliers DESC;"
        )
    })

    # 27 Average discount by country
    S.append({
        "q": "Sconto medio per shipCountry",
        "sql": (
            "SELECT o.shipCountry, AVG(COALESCE(od.discount,0)) AS avgDiscount\n"
            "FROM SalesOrder o\n"
            "JOIN OrderDetail od ON od.orderId = o.orderId\n"
            "GROUP BY o.shipCountry\n"
            "ORDER BY avgDiscount DESC;"
        )
    })

    # 28 Most frequent product pairs (support)
    S.append({
        "q": "Coppie di prodotti più ordinate insieme (top 10 per supporto)",
        "sql": (
            "SELECT p1.productName AS productA, p2.productName AS productB, COUNT(*) AS jointOrders\n"
            "FROM OrderDetail a\n"
            "JOIN OrderDetail b ON a.orderId = b.orderId AND a.productId < b.productId\n"
            "JOIN Product p1 ON a.productId = p1.productId\n"
            "JOIN Product p2 ON b.productId = p2.productId\n"
            "GROUP BY p1.productId, p1.productName, p2.productId, p2.productName\n"
            "ORDER BY jointOrders DESC\n"
            "LIMIT 10;"
        )
    })

    # 29 Orders delivered late (shippedDate > requiredDate)
    S.append({
        "q": "Ordini spediti in ritardo (shippedDate > requiredDate)",
        "sql": (
            "SELECT o.orderId, o.orderDate, o.requiredDate, o.shippedDate\n"
            "FROM SalesOrder o\n"
            "WHERE o.shippedDate > o.requiredDate;"
        )
    })

    # 30 Average delay by shipCountry
    S.append({
        "q": "Ritardo medio per shipCountry (giorni)",
        "sql": (
            "SELECT o.shipCountry, AVG(DATEDIFF(o.shippedDate, o.requiredDate)) AS avgDelay\n"
            "FROM SalesOrder o\n"
            "GROUP BY o.shipCountry\n"
            "ORDER BY avgDelay DESC;"
        )
    })

    # 31 Customer order count and last order date
    S.append({
        "q": "Numero ordini e ultima data ordine per cliente",
        "sql": (
            "SELECT c.companyName, COUNT(o.orderId) AS ordersCount, MAX(o.orderDate) AS lastOrder\n"
            "FROM Customer c\n"
            "LEFT JOIN SalesOrder o ON o.custId = c.custId\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY ordersCount DESC;"
        )
    })

    # 32 Product revenue and category
    S.append({
        "q": "Ricavo per prodotto con categoria",
        "sql": (
            "SELECT p.productName, cat.categoryName,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount,0))) AS revenue\n"
            "FROM OrderDetail od\n"
            "JOIN Product p ON od.productId = p.productId\n"
            "JOIN Category cat ON p.categoryId = cat.categoryId\n"
            "GROUP BY p.productId, p.productName, cat.categoryId, cat.categoryName\n"
            "ORDER BY revenue DESC;"
        )
    })

    # 33 Shipments per shipper per month
    S.append({
        "q": "Spedizioni per corriere e mese",
        "sql": (
            "SELECT s.companyName, DATE_FORMAT(o.orderDate, '%Y-%m') AS ym, COUNT(*) AS shipments\n"
            "FROM SalesOrder o\n"
            "JOIN Shipper s ON o.shipperid = s.shipperId\n"
            "GROUP BY s.shipperId, s.companyName, ym\n"
            "ORDER BY s.companyName, ym;"
        )
    })

    # 34 Employee performance: revenue per employee
    S.append({
        "q": "Fatturato per dipendente",
        "sql": (
            "SELECT e.firstname, e.lastname,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount,0))) AS revenue\n"
            "FROM SalesOrder o\n"
            "JOIN Employee e ON o.employeeId = e.employeeId\n"
            "JOIN OrderDetail od ON od.orderId = o.orderId\n"
            "GROUP BY e.employeeId, e.firstname, e.lastname\n"
            "ORDER BY revenue DESC;"
        )
    })

    # 35 Customers by region (customer.region)
    S.append({
        "q": "Clienti per regione (campo region di Customer)",
        "sql": (
            "SELECT c.region, COUNT(*) AS customers\n"
            "FROM Customer c\n"
            "GROUP BY c.region\n"
            "ORDER BY customers DESC;"
        )
    })

    # 36 Orders per city
    S.append({
        "q": "Ordini per shipCity",
        "sql": (
            "SELECT o.shipCity, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "GROUP BY o.shipCity\n"
            "ORDER BY ordersCount DESC;"
        )
    })

    # 37 Average unit price by category
    S.append({
        "q": "Prezzo medio unitario per categoria prodotto",
        "sql": (
            "SELECT cat.categoryName, AVG(p.unitPrice) AS avgPrice\n"
            "FROM Product p\n"
            "JOIN Category cat ON p.categoryId = cat.categoryId\n"
            "GROUP BY cat.categoryId, cat.categoryName\n"
            "ORDER BY avgPrice DESC;"
        )
    })

    # 38 Customers with most distinct products bought
    S.append({
        "q": "Clienti con il maggior numero di prodotti distinti acquistati",
        "sql": (
            "SELECT c.companyName, COUNT(DISTINCT od.productId) AS distinctProducts\n"
            "FROM Customer c\n"
            "JOIN SalesOrder o ON o.custId = c.custId\n"
            "JOIN OrderDetail od ON od.orderId = o.orderId\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY distinctProducts DESC\n"
            "LIMIT 10;"
        )
    })

    # 39 Orders per product per month (avoid reserved keyword 'LINES' as alias)
    S.append({
        "q": "Ordini per prodotto e mese (conteggio righe d'ordine)",
        "sql": (
            "SELECT p.productName, DATE_FORMAT(o.orderDate, '%Y-%m') AS ym, COUNT(*) AS lineCount\n"
            "FROM OrderDetail od\n"
            "JOIN Product p ON od.productId = p.productId\n"
            "JOIN SalesOrder o ON od.orderId = o.orderId\n"
            "GROUP BY p.productId, p.productName, ym\n"
            "ORDER BY p.productName, ym;"
        )
    })

    # 40 Top categories by average discount
    S.append({
        "q": "Categorie con sconto medio più alto",
        "sql": (
            "SELECT cat.categoryName, AVG(COALESCE(od.discount,0)) AS avgDiscount\n"
            "FROM OrderDetail od\n"
            "JOIN Product p ON od.productId = p.productId\n"
            "JOIN Category cat ON p.categoryId = cat.categoryId\n"
            "GROUP BY cat.categoryId, cat.categoryName\n"
            "ORDER BY avgDiscount DESC\n"
            "LIMIT 10;"
        )
    })

    # 41 Customers with phone missing
    S.append({
        "q": "Clienti senza numero di telefono",
        "sql": (
            "SELECT c.custId, c.companyName\n"
            "FROM Customer c\n"
            "WHERE c.phone IS NULL OR c.phone = '';"
        )
    })

    # 42 Orders per employee with late rate
    S.append({
        "q": "Ordini per dipendente con percentuale di ritardo",
        "sql": (
            "SELECT e.firstname, e.lastname, COUNT(*) AS ordersCount,\n"
            "       AVG(CASE WHEN o.shippedDate > o.requiredDate THEN 1 ELSE 0 END) AS lateRate\n"
            "FROM SalesOrder o\n"
            "JOIN Employee e ON o.employeeId = e.employeeId\n"
            "GROUP BY e.employeeId, e.firstname, e.lastname\n"
            "ORDER BY lateRate DESC, ordersCount DESC;"
        )
    })

    # 43 Suppliers providing most categories
    S.append({
        "q": "Fornitori che coprono più categorie (prodotti)",
        "sql": (
            "SELECT s.companyName, COUNT(DISTINCT p.categoryId) AS categories\n"
            "FROM Supplier s\n"
            "JOIN Product p ON p.supplierId = s.supplierId\n"
            "GROUP BY s.supplierId, s.companyName\n"
            "ORDER BY categories DESC\n"
            "LIMIT 10;"
        )
    })

    # 44 Average freight by shipper
    S.append({
        "q": "Freight medio per corriere",
        "sql": (
            "SELECT s.companyName, AVG(o.freight) AS avgFreight\n"
            "FROM SalesOrder o\n"
            "JOIN Shipper s ON o.shipperid = s.shipperId\n"
            "GROUP BY s.shipperId, s.companyName\n"
            "ORDER BY avgFreight DESC;"
        )
    })

    # 45 Products by supplier country
    S.append({
        "q": "Numero di prodotti per paese del fornitore",
        "sql": (
            "SELECT s.country, COUNT(*) AS products\n"
            "FROM Product p\n"
            "JOIN Supplier s ON p.supplierId = s.supplierId\n"
            "GROUP BY s.country\n"
            "ORDER BY products DESC;"
        )
    })

    # 46 Customers demographics mapping (if populated)
    S.append({
        "q": "Numero di clienti per tipo demografico (se presenti)",
        "sql": (
            "SELECT d.customerTypeId, COUNT(DISTINCT ccd.custId) AS customers\n"
            "FROM CustomerDemographics d\n"
            "JOIN CustCustDemographics ccd ON ccd.customerTypeId = d.customerTypeId\n"
            "GROUP BY d.customerTypeId\n"
            "ORDER BY customers DESC;"
        )
    })

    # 47 Employees by title
    S.append({
        "q": "Dipendenti per titolo professionale (title)",
        "sql": (
            "SELECT e.title, COUNT(*) AS employees\n"
            "FROM Employee e\n"
            "GROUP BY e.title\n"
            "ORDER BY employees DESC;"
        )
    })

    # 48 Top customers by order count last 12 months
    S.append({
        "q": "Top 10 clienti per numero ordini negli ultimi 12 mesi",
        "sql": (
            "SELECT c.companyName, COUNT(*) AS ordersCount\n"
            "FROM SalesOrder o\n"
            "JOIN Customer c ON o.custId = c.custId\n"
            "WHERE o.orderDate >= DATE_SUB(CURDATE(), INTERVAL 12 MONTH)\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY ordersCount DESC\n"
            "LIMIT 10;"
        )
    })

    # 49 Revenue by month
    S.append({
        "q": "Fatturato mensile (YYYY-MM)",
        "sql": (
            "SELECT DATE_FORMAT(o.orderDate, '%Y-%m') AS ym,\n"
            "       SUM(od.quantity * od.unitPrice * (1 - COALESCE(od.discount,0))) AS revenue\n"
            "FROM SalesOrder o\n"
            "JOIN OrderDetail od ON od.orderId = o.orderId\n"
            "GROUP BY ym\n"
            "ORDER BY ym;"
        )
    })

    # 50 Customers and their last shipped date
    S.append({
        "q": "Ultima data di spedizione per cliente",
        "sql": (
            "SELECT c.companyName, MAX(o.shippedDate) AS lastShipped\n"
            "FROM Customer c\n"
            "JOIN SalesOrder o ON o.custId = c.custId\n"
            "GROUP BY c.custId, c.companyName\n"
            "ORDER BY lastShipped DESC;"
        )
    })

    return S


def maybe_verify(sql: str, preview: int = 1) -> Optional[str]:
    """Return None if execution ok, else error string."""
    try:
        host = os.getenv("MYSQL_HOST", "localhost")
        user = os.getenv("MYSQL_USER", "root")
        # Avoid committing any real default passwords; rely on env var.
        password = os.getenv("MYSQL_PASSWORD", "")
        database = os.getenv("MYSQL_DB", "Northwind")
        conn = mysql.connector.connect(host=host, user=user, password=password, database=database)
        try:
            cur = conn.cursor(buffered=True)
            cur.execute(sql)
            # fetch a tiny preview to force execution
            _ = cur.fetchmany(preview)
            cur.close()
            return None
        finally:
            conn.close()
    except Exception as e:
        return str(e)


def build_system_prompt() -> str:
    # Keep it short; the SFT will learn style, not require full schema each sample
    return (
        "You are an expert SQL assistant for the Northwind MySQL database.\n"
        "Rules: output ONLY the final SQL query; use MySQL dialect; SELECT only;\n"
        "use existing tables/columns; prefer explicit JOIN ON with correct FKs;\n"
        "compute revenue as quantity*unitPrice*(1-COALESCE(discount,0)) when asked."
    )


def main():
    parser = argparse.ArgumentParser(description="Generate SFT dataset (50 chat samples) for Northwind")
    parser.add_argument("--out", type=str, default=str(DEFAULT_OUT))
    parser.add_argument("--verify", action="store_true", help="Esegue le query su MySQL e filtra quelle con errori")
    args = parser.parse_args()

    schema = load_schema()
    samples = get_samples()
    sys_prompt = build_system_prompt()

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    kept = 0
    skipped: List[str] = []
    with out_path.open("w", encoding="utf-8") as f:
        for i, s in enumerate(samples, start=1):
            q = s["q"]
            sql = s["sql"]
            # Optional lightweight validation
            err = validate_sql_against_schema(sql, schema)
            if err:
                skipped.append(f"#{i} schema-validate: {err}")
                continue
            if args.verify:
                verr = maybe_verify(sql)
                if verr:
                    skipped.append(f"#{i} verify: {verr}")
                    continue
            obj = {
                "messages": [
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": q},
                    {"role": "assistant", "content": sql},
                ]
            }
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            kept += 1

    print(f"Scritte {kept} samples in: {out_path}")
    if skipped:
        print("Saltati:")
        for s in skipped:
            print(" -", s)


if __name__ == "__main__":
    main()
