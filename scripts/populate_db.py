# -*- coding: utf-8 -*-
"""
Script per popolare automaticamente il database Northwind (MySQL) con dati fittizi.
Utilizza la libreria Faker per generare dati realistici e rispetta le relazioni tra le tabelle.
Assicurarsi di installare le librerie 'Faker' e 'mysql-connector-python' prima di eseguire.
"""

import os
import random
from datetime import datetime, timedelta
import mysql.connector
from faker import Faker
from dotenv import load_dotenv

# ============== Configurazione iniziale ==============
# Permette override via .env o variabili ambiente
load_dotenv()
HOST = os.getenv('MYSQL_HOST', 'localhost')
USER = os.getenv('MYSQL_USER', 'root')
# Avoid committing or relying on hardcoded passwords. Expect env or empty default.
PASSWORD = os.getenv('MYSQL_PASSWORD', '')
DB_NAME = os.getenv('MYSQL_DB', 'Northwind')

# Numero di record da generare per ciascuna tabella
NUM_CATEGORIES = 5
NUM_SUPPLIERS = 10
NUM_SHIPPERS = 5
NUM_EMPLOYEES = 8
NUM_CUSTOMERS = 20
NUM_PRODUCTS = 30
NUM_ORDERS = 50
NUM_ORDER_DETAILS = 100

# Creazione istanza Faker (locale predefinito en_US per dati internazionali)
fake = Faker()

def get_ids(cursor, table: str, id_col: str):
    cursor.execute(f"SELECT {id_col} FROM {table}")
    return [row[0] for row in cursor.fetchall()]

def populate_categories(cursor, num):
    """Genera e inserisce 'num' categorie fittizie nella tabella Category."""
    categories = []
    for _ in range(num):
        # Nome categoria (breve, max 15 char) e descrizione (breve frase)
        name = fake.word()[0:15]  # tagliamo a 15 caratteri se necessario
        desc = fake.sentence(nb_words=6)  # descrizione breve
        categories.append((name, desc))
    # Inserimento in tabella Category (picture è opzionale, lo lasciamo NULL omettendolo)
    cursor.executemany(
        "INSERT INTO Category (categoryName, description) VALUES (%s, %s)",
        categories
    )

def populate_suppliers(cursor, num):
    """Genera e inserisce 'num' fornitori fittizi nella tabella Supplier."""
    suppliers = []
    for _ in range(num):
        company = fake.company()[0:40]          # nome azienda fornitore (max 40 char)
        contact = fake.name()[0:30]            # nome contatto (max 30 char)
        title = fake.job()[0:30]               # titolo contatto (es. Purchasing Manager)
        address = fake.street_address()[0:60]  # via e numero civico (max 60 char)
        city = fake.city()[0:15]               # città (max 15 char)
        region = fake.state()[0:15]            # stato/provincia (max 15 char)
        postal = fake.postcode()[0:10]         # CAP (max 10 char)
        country = fake.country()[0:15]         # paese (max 15 char)
        phone = fake.phone_number()[0:24]      # telefono (max 24 char)
        email = fake.company_email()[0:225]    # email (max 225 char)
        fax = fake.phone_number()[0:24]        # fax (max 24 char, può essere lasciato vuoto)
        # Inseriamo fax e HomePage come NULL se non vogliamo generare homepage
        suppliers.append((company, contact, title, address, city, region, postal, country, phone, email, fax))
    cursor.executemany(
        "INSERT INTO Supplier (companyName, contactName, contactTitle, address, city, region, "
        "postalCode, country, phone, email, fax) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        suppliers
    )

def populate_shippers(cursor, num):
    """Genera e inserisce 'num' spedizionieri fittizi nella tabella Shipper."""
    shippers = []
    for _ in range(num):
        company = fake.company()[0:40]      # nome dell'azienda di spedizioni (max 40 char)
        phone = fake.phone_number()[0:44]   # numero di telefono (max 44 char)
        shippers.append((company, phone))
    cursor.executemany(
        "INSERT INTO Shipper (companyName, phone) VALUES (%s, %s)",
        shippers
    )

def populate_employees(cursor, num):
    """Genera e inserisce 'num' dipendenti fittizi nella tabella Employee."""
    employees = []
    # Per gestire mgrId, generiamo prima tutti i dati e assegniamo manager a posteriori
    for i in range(1, num + 1):
        last_name = fake.last_name()[0:20]    # cognome (max 20 char)
        first_name = fake.first_name()[0:10]  # nome (max 10 char)
        title = fake.job()[0:30]              # titolo posizione lavorativa (max 30 char)
        # titleOfCourtesy: prefisso (Mr, Mrs, Ms, Dr, etc.)
        title_courtesy = random.choice(["Mr.", "Ms.", "Mrs.", "Dr."])
        # Date di nascita e assunzione
        birth_date = fake.date_of_birth(minimum_age=20, maximum_age=65)  # date object
        # hire_date almeno 18 anni dopo la nascita, fino ad oggi
        hire_start = birth_date + timedelta(days=18*365)
        if hire_start > datetime.now().date():
            # Se la persona è troppo giovane, riduciamo l'età minima a 18 anni fa
            hire_start = datetime.now().date() - timedelta(days=1)
        hire_date = fake.date_between(start_date=hire_start, end_date='today')
        # Indirizzo e contatti
        address = fake.street_address()[0:60]  # indirizzo (max 60 char)
        city = fake.city()[0:15]               # città (max 15 char)
        region = fake.state()[0:15]            # regione/stato (max 15 char)
        postal = fake.postcode()[0:10]         # CAP (max 10 char)
        country = fake.country()[0:15]         # paese (max 15 char)
        phone = fake.phone_number()[0:24]      # telefono (max 24 char)
        extension = str(fake.random_int(min=100, max=999))  # interno (4 char max, es. '123')
        if len(extension) > 4:
            extension = extension[0:4]
        mobile = fake.phone_number()[0:24]     # cellulare (max 24 char)
        email = fake.email()[0:225]            # email (max 225 char)
        # Manager ID: per il primo dipendente None, per gli altri scegliamo un ID minore (già esistente)
        mgr_id = None
        if i > 1:
            mgr_id = random.randint(1, i-1)   # assegna come manager un employee precedente
        employees.append((last_name, first_name, title, title_courtesy, 
                          birth_date, hire_date, address, city, region, postal, country, 
                          phone, extension, mobile, email, mgr_id))
    cursor.executemany(
        "INSERT INTO Employee (lastName, firstName, title, titleOfCourtesy, birthDate, hireDate, "
        "address, city, region, postalCode, country, phone, extension, mobile, email, mgrId) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        employees
    )

def populate_customers(cursor, num):
    """Genera e inserisce 'num' clienti fittizi nella tabella Customer."""
    customers = []
    for _ in range(num):
        company = fake.company()[0:40]      # nome azienda cliente (max 40 char)
        contact = fake.name()[0:30]        # nome completo referente (max 30 char)
        title = fake.job()[0:30]           # titolo del referente (max 30 char)
        address = fake.street_address()[0:60]  # indirizzo (max 60 char)
        city = fake.city()[0:15]               # città (max 15 char)
        region = fake.state()[0:15]            # stato/provincia (max 15 char)
        postal = fake.postcode()[0:10]         # CAP (max 10 char)
        country = fake.country()[0:15]         # paese (max 15 char)
        phone = fake.phone_number()[0:24]      # telefono (max 24 char)
        mobile = fake.phone_number()[0:24]     # cellulare (max 24 char)
        email = fake.email()[0:225]            # email (max 225 char)
        fax = fake.phone_number()[0:24]        # fax (max 24 char)
        customers.append((company, contact, title, address, city, region, postal, country, phone, mobile, email, fax))
    cursor.executemany(
        "INSERT INTO Customer (companyName, contactName, contactTitle, address, city, region, "
        "postalCode, country, phone, mobile, email, fax) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        customers
    )

def populate_products(cursor, num):
    """Genera e inserisce 'num' prodotti fittizi nella tabella Product."""
    products = []
    # Recupera ID reali di supplier e category per evitare assunzioni su range contigui
    supplier_ids = get_ids(cursor, "Supplier", "supplierId")
    category_ids = get_ids(cursor, "Category", "categoryId")
    for _ in range(num):
        name = fake.word().capitalize()      # nome prodotto (una parola, capitalizzata)
        if len(name) > 40:
            name = name[:40]
        # Assegna supplierId e categoryId esistenti, se presenti
        supplier_id = random.choice(supplier_ids) if supplier_ids else None
        category_id = random.choice(category_ids) if category_ids else None
        quantity_per_unit = f"{random.randint(1, 20)} {random.choice(['boxes', 'packets', 'pcs', 'kg'])}"
        quantity_per_unit = quantity_per_unit[:20]  # max 20 char
        unit_price = round(random.uniform(1, 100), 2)  # prezzo unitario tra 1 e 100 con 2 decimali
        units_in_stock = random.randint(0, 100)  # quantità a magazzino
        units_on_order = random.randint(0, 50)   # quantità ordinate in arrivo
        reorder_level = random.randint(0, 20)    # livello di riordino
        discontinued = '0'  # di base non discontinuato
        if random.random() < 0.2:  # ~20% prodotti discontinued
            discontinued = '1'
        products.append((name, supplier_id, category_id, quantity_per_unit, unit_price,
                         units_in_stock, units_on_order, reorder_level, discontinued))
    cursor.executemany(
        "INSERT INTO Product (productName, supplierId, categoryId, quantityPerUnit, unitPrice, "
        "unitsInStock, unitsOnOrder, reorderLevel, discontinued) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        products
    )

def populate_orders(cursor, num):
    """Genera e inserisce 'num' ordini fittizi nella tabella SalesOrder."""
    orders = []
    customer_ids = get_ids(cursor, "Customer", "custId")
    shipper_ids = get_ids(cursor, "Shipper", "shipperId")
    employee_ids = get_ids(cursor, "Employee", "employeeId")
    for _ in range(num):
        # Se non ci sono clienti o spedizionieri, non possiamo creare ordini
        if not customer_ids or not shipper_ids:
            break
        cust_id = random.choice(customer_ids)
        shipper_id = random.choice(shipper_ids)
        emp_id = None
        if employee_ids:
            # Assegna un employee casuale (oppure lascia None se si vuole simulare ordine senza referente)
            emp_id = random.choice(employee_ids)
        # Date: orderDate tra un anno fa e oggi
        order_date = fake.date_between(start_date='-1y', end_date='today')
        # RequiredDate qualche giorno dopo l'ordine (es. 7-20 giorni dopo)
        required_date = order_date + timedelta(days=random.randint(7, 20))
        # ShippedDate tra 1 e 15 giorni dopo l'orderDate (non oltre la requiredDate se possibile)
        max_ship_gap = 15
        ship_gap = random.randint(1, max_ship_gap)
        shipped_date = order_date + timedelta(days=ship_gap)
        if shipped_date > required_date:
            # Se la spedizione eccede la data richiesta, la impostiamo uguale alla data richiesta (consegna giusta in extremis)
            shipped_date = required_date
        # Calcola un costo di spedizione (freight) casuale
        freight = round(random.uniform(0, 100), 2)
        # Recupera i dati del cliente per l'indirizzo di spedizione (per semplicità, query immediata)
        cursor.execute("SELECT companyName, address, city, region, postalCode, country FROM Customer WHERE custId = %s", (cust_id,))
        customer_info = cursor.fetchone()
        ship_name = customer_info[0] if customer_info else None
        ship_address = customer_info[1] if customer_info else None
        ship_city = customer_info[2] if customer_info else None
        ship_region = customer_info[3] if customer_info else None
        ship_postal = customer_info[4] if customer_info else None
        ship_country = customer_info[5] if customer_info else None
        orders.append((cust_id, emp_id, order_date, required_date, shipped_date,
                       shipper_id, freight, ship_name, ship_address, ship_city, ship_region, ship_postal, ship_country))
    cursor.executemany(
        "INSERT INTO SalesOrder (custId, employeeId, orderDate, requiredDate, shippedDate, shipperId, freight, "
        "shipName, shipAddress, shipCity, shipRegion, shipPostalCode, shipCountry) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        orders
    )

def populate_order_details(cursor, num):
    """Genera e inserisce 'num' dettagli d'ordine fittizi nella tabella OrderDetail."""
    order_details = []
    order_ids = get_ids(cursor, "SalesOrder", "orderId")
    product_ids = get_ids(cursor, "Product", "productId")
    if not order_ids or not product_ids:
        return  # niente da fare se non ci sono ordini o prodotti
    assigned = 0
    # Garantisci almeno un dettaglio per ordine se richiesto
    if num >= len(order_ids):
        for order_id in order_ids:
            product_id = random.choice(product_ids)
            cursor.execute("SELECT unitPrice FROM Product WHERE productId = %s", (product_id,))
            result = cursor.fetchone()
            unit_price = float(result[0]) if result else 0.0
            quantity = random.randint(1, 10)
            discount = 0.00
            if random.random() < 0.1:
                discount = random.choice([5.00, 10.00, 15.00, 20.00]) / 100.0
            order_details.append((order_id, product_id, unit_price, quantity, discount))
            assigned += 1
    # Aggiungi dettagli fino a num
    while assigned < num:
        order_id = random.choice(order_ids)
        product_id = random.choice(product_ids)
        cursor.execute("SELECT unitPrice FROM Product WHERE productId = %s", (product_id,))
        result = cursor.fetchone()
        unit_price = float(result[0]) if result else 0.0
        quantity = random.randint(1, 10)
        discount = 0.00
        if random.random() < 0.1:
            discount = random.choice([5.00, 10.00, 15.00, 20.00]) / 100.0
        order_details.append((order_id, product_id, unit_price, quantity, discount))
        assigned += 1
    cursor.executemany(
        "INSERT INTO OrderDetail (orderId, productId, unitPrice, quantity, discount) "
        "VALUES (%s, %s, %s, %s, %s)",
        order_details
    )

def main():
    # Connessione al database MySQL
    try:
        conn = mysql.connector.connect(host=HOST, user=USER, password=PASSWORD, database=DB_NAME)
    except mysql.connector.Error as err:
        print(f"Errore di connessione al database: {err}")
        return
    cursor = conn.cursor()
    try:
        # (Opzionale) Disabilita i vincoli di chiave esterna temporaneamente
        # cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
        # Popolamento delle tabelle nell'ordine corretto
        populate_categories(cursor, NUM_CATEGORIES)
        populate_suppliers(cursor, NUM_SUPPLIERS)
        populate_shippers(cursor, NUM_SHIPPERS)
        populate_employees(cursor, NUM_EMPLOYEES)
        populate_customers(cursor, NUM_CUSTOMERS)
        populate_products(cursor, NUM_PRODUCTS)
        populate_orders(cursor, NUM_ORDERS)
        populate_order_details(cursor, NUM_ORDER_DETAILS)
        # Riabilita i vincoli di chiave esterna se erano stati disabilitati
        # cursor.execute("SET FOREIGN_KEY_CHECKS=1;")
        conn.commit()  # conferma tutte le operazioni
        print("Popolamento completato con successo.")
    except Exception as e:
        # In caso di errore, annulla la transazione
        conn.rollback()
        print(f"Errore durante l'inserimento dei dati: {e}")
    finally:
        # Chiude la connessione
        cursor.close()
        conn.close()

# Esegue la funzione principale se lo script è lanciato direttamente
if __name__ == "__main__":
    main()
