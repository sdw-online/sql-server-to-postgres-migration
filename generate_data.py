import os
import pyodbc
from faker import Faker
import random
from dotenv import load_dotenv

# Initialize faker
fake = Faker()
Faker.seed(42)
random.seed(42)

# Load environment variables
load_dotenv()

sql_host = os.getenv('SQLSERVER_HOST', '<add-your-own-SQL-SERVER-HOST>')
sql_db = os.getenv('SQLSERVER_DB', 'TransactionDB_UAT')

print("="*70)
print("SQL SERVER DATA GENERATOR (1M+ ROWS)")
print("="*70)
print(f"\nConnecting to: {sql_host}")
print(f"Database: {sql_db}\n")

# Connect to SQL Server
conn_string = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={sql_host};"
    f"DATABASE={sql_db};"
    f"Trusted_Connection=yes;"
)

conn = pyodbc.connect(conn_string)
cursor = conn.cursor()

print("✓ Connected to SQL Server\n")

# ============================================
# REALISTIC PRODUCT NAMES (25 products)
# ============================================

PRODUCT_NAMES = [
    # Electronics
    "Wireless Bluetooth Headphones",
    "USB-C Charging Cable",
    "Portable Power Bank",
    "Laptop Stand",
    "Wireless Mouse",
    
    # Home & Kitchen
    "Stainless Steel Water Bottle",
    "Coffee Maker",
    "Blender",
    "Non-Stick Frying Pan",
    "Kitchen Knife Set",
    
    # Clothing
    "Cotton T-Shirt",
    "Denim Jeans",
    "Running Shoes",
    "Winter Jacket",
    "Baseball Cap",
    
    # Health & Beauty
    "Electric Toothbrush",
    "Yoga Mat",
    "Resistance Bands",
    "Face Moisturizer",
    "Shampoo & Conditioner Set",
    
    # Office Supplies
    "Notebook Set",
    "Ballpoint Pens (Pack of 10)",
    "Desk Organizer",
    "Sticky Notes",
    "Printer Paper (500 Sheets)"
]

# ============================================
# REALISTIC SUPPLIER NAMES (5000 suppliers)
# ============================================

print("Generating 5,000 realistic supplier names...")

SUPPLIER_SUFFIXES = ['LLC', 'Ltd', 'PLC', 'Inc', 'Corp', 'Co', 'Group', 'Industries', '']
SUPPLIER_TYPES = [
    'Electronics', 'Distribution', 'Supply', 'Manufacturing', 'Trading',
    'Global', 'International', 'Wholesale', 'Solutions', 'Technologies'
]

def generate_supplier_name():
    """Generate realistic company name."""
    company_type = random.choice([
        'company',  # Uses company name
        'business',  # Uses business buzzword
        'combined'  # Combines both
    ])
    
    if company_type == 'company':
        base = fake.company().replace(', Inc.', '').replace(', LLC', '').replace(', Ltd', '')
    elif company_type == 'business':
        base = f"{fake.bs().title().replace(' ', ' ').split()[0]} {random.choice(SUPPLIER_TYPES)}"
    else:
        base = f"{fake.last_name()} {random.choice(SUPPLIER_TYPES)}"
    
    # Add suffix
    suffix = random.choice(SUPPLIER_SUFFIXES)
    if suffix:
        return f"{base} {suffix}"
    return base

SUPPLIER_NAMES = [generate_supplier_name() for _ in range(5000)]

print(f"✓ Generated 5,000 supplier names\n")

# ============================================
# TABLE 1: Categories (8 rows)
# ============================================

print("Creating Categories table...")

cursor.execute("IF OBJECT_ID('Categories', 'U') IS NOT NULL DROP TABLE Categories")

cursor.execute("""
CREATE TABLE Categories (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(50),
    Description NVARCHAR(MAX)
)
""")

categories_data = [
    ('Electronics', 'Electronic devices and accessories'),
    ('Clothing', 'Apparel and fashion items'),
    ('Food', 'Food and beverages'),
    ('Books', 'Books and publications'),
    ('Toys', 'Toys and games'),
    ('Sports', 'Sports equipment and gear'),
    ('Home', 'Home and garden products'),
    ('Beauty', 'Beauty and personal care')
]

cursor.executemany(
    "INSERT INTO Categories (CategoryName, Description) VALUES (?, ?)",
    categories_data
)
conn.commit()

print(f"✓ Categories: {len(categories_data)} rows\n")

# ============================================
# TABLE 2: Suppliers (5,000 rows)
# ============================================

print("Creating Suppliers table...")

cursor.execute("IF OBJECT_ID('Suppliers', 'U') IS NOT NULL DROP TABLE Suppliers")

cursor.execute("""
CREATE TABLE Suppliers (
    SupplierID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierName NVARCHAR(150),
    ContactName NVARCHAR(100),
    Country NVARCHAR(100),
    Phone NVARCHAR(20)
)
""")

# Use our hardcoded supplier names
suppliers_data = [
    (SUPPLIER_NAMES[i], fake.name(), fake.country()[:100], fake.phone_number()[:20])
    for i in range(5000)
]

cursor.executemany(
    "INSERT INTO Suppliers (SupplierName, ContactName, Country, Phone) VALUES (?, ?, ?, ?)",
    suppliers_data
)
conn.commit()

print(f"✓ Suppliers: 5,000 rows\n")

# ============================================
# TABLE 3: Customers (900,000 rows)
# DATA QUALITY ISSUES:
# - NULL CustomerName (~0.5% of records)
# - Invalid email formats (~1% of records)
# - Future dates in CreatedDate (~1% of records)
# ============================================

print("Creating Customers table (this will take several minutes)...")

cursor.execute("IF OBJECT_ID('Customers', 'U') IS NOT NULL DROP TABLE Customers")

cursor.execute("""
CREATE TABLE Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(100),
    Email NVARCHAR(100),
    Phone NVARCHAR(20),
    Country NVARCHAR(100),
    CreatedDate DATETIME,
    IsActive BIT
)
""")

# Generate customers in batches
batch_size = 10000
total_customers = 900000
total_batches = total_customers // batch_size

for batch_num in range(total_batches):
    customers_batch = []
    
    for i in range(batch_size):
        # DATA QUALITY ISSUE #1: NULL CustomerName (~0.5% of records)
        # Mentioned in Design: "CustomerName should not be NULL"
        if random.random() < 0.005:
            customer_name = None
            email = fake.email()
        else:
            customer_name = fake.name()
            
            # Create email from customer name
            # e.g., "Fred Flintstone" → "fred.flintstone@email.com"
            name_parts = customer_name.lower().split()
            if len(name_parts) >= 2:
                first_name = name_parts[0]
                last_name = name_parts[-1]
                
                # DATA QUALITY ISSUE #2: Invalid email formats (~1% of records)
                # Mentioned in Design: "Email should not be NULL" (we make them invalid instead)
                if random.random() < 0.01:
                    email = f"{first_name}.{last_name}@invalid"
                else:
                    domain = random.choice(['gmail.com', 'yahoo.com', 'outlook.com', 
                                          'hotmail.com', 'email.com', 'mail.com'])
                    email = f"{first_name}.{last_name}@{domain}"
            else:
                email = fake.email()
        
        # DATA QUALITY ISSUE #3: Future dates in CreatedDate (~1% of records)
        # Mentioned in Design: "Future dates impossible for historical data"
        if random.random() < 0.01:
            created_date = fake.date_time_between(start_date='+1d', end_date='+30d')
        else:
            created_date = fake.date_time_between(start_date='-5y', end_date='now')
        
        customers_batch.append((
            customer_name,
            email,
            fake.phone_number()[:20],
            fake.country()[:100],
            created_date,
            random.choice([0, 1])
        ))
    
    cursor.executemany(
        """
        INSERT INTO Customers (CustomerName, Email, Phone, Country, CreatedDate, IsActive)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        customers_batch
    )
    conn.commit()
    
    if (batch_num + 1) % 10 == 0:
        print(f"  Inserted {(batch_num + 1) * batch_size:,} customers...")

print(f"✓ Customers: {total_customers:,} rows\n")

# ============================================
# TABLE 4: Products (150,000 rows)
# DATA QUALITY ISSUES:
# - Negative UnitPrice (~0.5% of records)
# - Negative StockQuantity (~1% of records)
# - Orphaned SupplierID foreign keys (~16% of records)
# - NULL ProductName (~0.2% of records)
# ============================================

print("Creating Products table...")

cursor.execute("IF OBJECT_ID('Products', 'U') IS NOT NULL DROP TABLE Products")

cursor.execute("""
CREATE TABLE Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(200),
    CategoryID INT,
    SupplierID INT,
    UnitPrice MONEY,
    StockQuantity INT,
    CreatedDate DATETIME
)
""")

# Generate products in batches
batch_size = 10000
total_products = 150000
total_batches = total_products // batch_size

for batch_num in range(total_batches):
    products_batch = []
    
    for i in range(batch_size):
        # DATA QUALITY ISSUE #4: NULL ProductName (~0.2% of records)
        # Mentioned in Design: "ProductName should not be NULL"
        if random.random() < 0.002:
            product_name = None
        else:
            # Use realistic product names (cycle through our list)
            product_name = PRODUCT_NAMES[i % len(PRODUCT_NAMES)]
        
        # DATA QUALITY ISSUE #5: Negative UnitPrice (~0.5% of records)
        # Mentioned in Design: "Negative prices not possible in real world"
        if random.random() < 0.005:
            unit_price = -random.uniform(10, 1000)
        else:
            unit_price = random.uniform(5, 2000)
        
        # DATA QUALITY ISSUE #6: Negative StockQuantity (~1% of records)
        # Mentioned in Design: "Cannot have -4 pairs of shoes in inventory"
        if random.random() < 0.01:
            stock_quantity = -random.randint(1, 100)
        else:
            stock_quantity = random.randint(0, 1000)
        
        # DATA QUALITY ISSUE #7: Orphaned SupplierID foreign keys (~16% of records)
        # Mentioned in Design: "SupplierID of 999 but no supplier 999 exists"
        # We have 5000 valid suppliers (IDs 1-5000)
        # ~16% of products will have SupplierID between 5001-6000 (orphaned)
        supplier_id = random.randint(1, 6000)
        
        products_batch.append((
            product_name,
            random.randint(1, 8),      # CategoryID (1-8 exist, all valid)
            supplier_id,               # SupplierID (some >5000 won't exist = orphaned FK)
            unit_price,
            stock_quantity,
            fake.date_time_between(start_date='-3y', end_date='now')
        ))
    
    cursor.executemany(
        """
        INSERT INTO Products (ProductName, CategoryID, SupplierID, UnitPrice, StockQuantity, CreatedDate)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        products_batch
    )
    conn.commit()
    
    if (batch_num + 1) % 5 == 0:
        print(f"  Inserted {(batch_num + 1) * batch_size:,} products...")

print(f"✓ Products: {total_products:,} rows\n")

# Close connection
cursor.close()
conn.close()

# Summary
total_rows = len(categories_data) + 5000 + total_customers + total_products

print("="*70)
print("✓ DATA GENERATION COMPLETE!")
print("="*70)
print("\nData created:")
print(f"  Categories:       8 rows")
print(f"  Suppliers:    5,000 rows")
print(f"  Customers: 900,000 rows")
print(f"  Products:  150,000 rows")
print(f"  {'─'*30}")
print(f"  TOTAL:  1,055,008 rows")
print("\nData quality issues included (matching Design phase):")
print("  ✓ NULL CustomerName (~0.5% = ~4,500 records)")
print("  ✓ NULL ProductName (~0.2% = ~300 records)")
print("  ✓ Invalid email formats (~1% = ~9,000 records)")
print("  ✓ Future dates in CreatedDate (~1% = ~9,000 records)")
print("  ✓ Negative UnitPrice (~0.5% = ~750 records)")
print("  ✓ Negative StockQuantity (~1% = ~1,500 records)")
print("  ✓ Orphaned SupplierID foreign keys (~16% = ~24,000 products)")
print("\nRealistic data:")
print("  ✓ Customer emails match their names (fred.flintstone@gmail.com)")
print("  ✓ Real product names (25 common e-commerce products)")
print("  ✓ Realistic supplier company names (5,000 unique)")
print("\nYour SQL Server UAT environment is ready for migration!")
print("="*70)