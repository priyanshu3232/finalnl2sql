import sqlite3
import json
from typing import Dict, List, Optional
from datetime import datetime, timedelta
import random
import os

class SchemaManager:
    def __init__(self, db_path: str = "tally_database.db"):
        self.db_path = db_path
        self.schema = {}
        self.connection = None
        self._ensure_database_exists()
        
    def _ensure_database_exists(self):
        """Ensure database file exists and connection is available"""
        try:
            # Create database file if it doesn't exist
            if not os.path.exists(self.db_path):
                # Create empty database file
                open(self.db_path, 'a').close()
            
            # Establish connection
            self.connection = sqlite3.connect(self.db_path)
            
            # Check if database has tables, if not, load sample schema
            cursor = self.connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            
            if not tables:
                # Database is empty, load default schema
                self.load_tally_schema()
                
        except Exception as e:
            print(f"Warning: Could not initialize database: {e}")
            self.connection = None
        
    def load_tally_schema(self) -> None:
        """Load Tally ERP schema with sample data"""
        # Define comprehensive Tally ERP schema
        self.schema = {
            # Configuration
            "config": {
                "columns": [
                    {"name": "name", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "value", "type": "VARCHAR(1024)"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [],
                "description": "System configuration parameters"
            },
            
            # Master Tables
            "mst_group": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "name", "type": "VARCHAR(1024)"},
                    {"name": "parent", "type": "VARCHAR(1024)"},
                    {"name": "primary_group", "type": "VARCHAR(1024)"},
                    {"name": "is_revenue", "type": "SMALLINT"},
                    {"name": "is_deemedpositive", "type": "SMALLINT"},
                    {"name": "is_reserved", "type": "SMALLINT"},
                    {"name": "affects_gross_profit", "type": "SMALLINT"},
                    {"name": "sort_position", "type": "INTEGER"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": ["mst_ledger.parent = mst_group.name"],
                "description": "Account group hierarchy (Assets, Liabilities, Income, Expenses)"
            },
            
            "mst_ledger": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "name", "type": "VARCHAR(1024)"},
                    {"name": "parent", "type": "VARCHAR(1024)"},
                    {"name": "alias", "type": "VARCHAR(256)"},
                    {"name": "description", "type": "VARCHAR(64)"},
                    {"name": "notes", "type": "VARCHAR(64)"},
                    {"name": "is_revenue", "type": "SMALLINT"},
                    {"name": "is_deemedpositive", "type": "SMALLINT"},
                    {"name": "opening_balance", "type": "NUMERIC DEFAULT 0"},
                    {"name": "closing_balance", "type": "NUMERIC DEFAULT 0"},
                    {"name": "mailing_name", "type": "VARCHAR(256)"},
                    {"name": "mailing_address", "type": "VARCHAR(1024)"},
                    {"name": "mailing_state", "type": "VARCHAR(256)"},
                    {"name": "mailing_country", "type": "VARCHAR(256)"},
                    {"name": "mailing_pincode", "type": "VARCHAR(64)"},
                    {"name": "email", "type": "VARCHAR(256)"},
                    {"name": "it_pan", "type": "VARCHAR(64)"},
                    {"name": "gstn", "type": "VARCHAR(64)"},
                    {"name": "gst_registration_type", "type": "VARCHAR(64)"},
                    {"name": "gst_supply_type", "type": "VARCHAR(64)"},
                    {"name": "gst_duty_head", "type": "VARCHAR(16)"},
                    {"name": "tax_rate", "type": "NUMERIC DEFAULT 0"},
                    {"name": "bank_account_holder", "type": "VARCHAR(256)"},
                    {"name": "bank_account_number", "type": "VARCHAR(64)"},
                    {"name": "bank_ifsc", "type": "VARCHAR(64)"},
                    {"name": "bank_swift", "type": "VARCHAR(64)"},
                    {"name": "bank_name", "type": "VARCHAR(64)"},
                    {"name": "bank_branch", "type": "VARCHAR(64)"},
                    {"name": "bill_credit_period", "type": "INTEGER DEFAULT 0"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_accounting.ledger = mst_ledger.name",
                    "trn_cost_centre.ledger = mst_ledger.name",
                    "trn_bill.ledger = mst_ledger.name"
                ],
                "description": "All account ledgers with GST and banking details"
            },
            
            "mst_vouchertype": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "name", "type": "VARCHAR(1024)"},
                    {"name": "parent", "type": "VARCHAR(1024)"},
                    {"name": "numbering_method", "type": "VARCHAR(64)"},
                    {"name": "is_deemedpositive", "type": "SMALLINT"},
                    {"name": "affects_stock", "type": "SMALLINT"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": ["trn_voucher.voucher_type = mst_vouchertype.name"],
                "description": "Different types of transactions"
            },
            
            "mst_stock_item": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "name", "type": "VARCHAR(1024)"},
                    {"name": "parent", "type": "VARCHAR(1024)"},
                    {"name": "alias", "type": "VARCHAR(256)"},
                    {"name": "description", "type": "VARCHAR(64)"},
                    {"name": "notes", "type": "VARCHAR(64)"},
                    {"name": "part_number", "type": "VARCHAR(256)"},
                    {"name": "uom", "type": "VARCHAR(32)"},
                    {"name": "alternate_uom", "type": "VARCHAR(32)"},
                    {"name": "conversion", "type": "INTEGER DEFAULT 0"},
                    {"name": "opening_balance", "type": "NUMERIC DEFAULT 0"},
                    {"name": "opening_rate", "type": "NUMERIC DEFAULT 0"},
                    {"name": "opening_value", "type": "NUMERIC DEFAULT 0"},
                    {"name": "closing_balance", "type": "NUMERIC DEFAULT 0"},
                    {"name": "closing_rate", "type": "NUMERIC DEFAULT 0"},
                    {"name": "closing_value", "type": "NUMERIC DEFAULT 0"},
                    {"name": "costing_method", "type": "VARCHAR(32)"},
                    {"name": "gst_type_of_supply", "type": "VARCHAR(32)"},
                    {"name": "gst_hsn_code", "type": "VARCHAR(64)"},
                    {"name": "gst_hsn_description", "type": "VARCHAR(256)"},
                    {"name": "gst_rate", "type": "NUMERIC DEFAULT 0"},
                    {"name": "gst_taxability", "type": "VARCHAR(32)"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_inventory.item = mst_stock_item.name",
                    "trn_batch.item = mst_stock_item.name"
                ],
                "description": "Inventory items with GST details"
            },
            
            "mst_employee": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "name", "type": "VARCHAR(1024)"},
                    {"name": "parent", "type": "VARCHAR(1024)"},
                    {"name": "id_number", "type": "VARCHAR(256)"},
                    {"name": "date_of_joining", "type": "DATE"},
                    {"name": "date_of_release", "type": "DATE"},
                    {"name": "designation", "type": "VARCHAR(64)"},
                    {"name": "function_role", "type": "VARCHAR(64)"},
                    {"name": "location", "type": "VARCHAR(256)"},
                    {"name": "gender", "type": "VARCHAR(32)"},
                    {"name": "date_of_birth", "type": "DATE"},
                    {"name": "blood_group", "type": "VARCHAR(32)"},
                    {"name": "father_mother_name", "type": "VARCHAR(256)"},
                    {"name": "spouse_name", "type": "VARCHAR(256)"},
                    {"name": "address", "type": "VARCHAR(256)"},
                    {"name": "mobile", "type": "VARCHAR(32)"},
                    {"name": "email", "type": "VARCHAR(64)"},
                    {"name": "pan", "type": "VARCHAR(32)"},
                    {"name": "aadhar", "type": "VARCHAR(32)"},
                    {"name": "uan", "type": "VARCHAR(32)"},
                    {"name": "pf_number", "type": "VARCHAR(32)"},
                    {"name": "pf_joining_date", "type": "DATE"},
                    {"name": "pf_relieving_date", "type": "DATE"},
                    {"name": "pr_account_number", "type": "VARCHAR(32)"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_employee.employee_name = mst_employee.name",
                    "trn_payhead.employee_name = mst_employee.name",
                    "trn_attendance.employee_name = mst_employee.name"
                ],
                "description": "Employee master data"
            },
            
            # Transaction Tables
            "trn_voucher": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64) PRIMARY KEY"},
                    {"name": "date", "type": "DATE"},
                    {"name": "voucher_type", "type": "VARCHAR(1024)"},
                    {"name": "voucher_number", "type": "VARCHAR(64)"},
                    {"name": "reference_number", "type": "VARCHAR(64)"},
                    {"name": "reference_date", "type": "DATE"},
                    {"name": "narration", "type": "VARCHAR(4000)"},
                    {"name": "party_name", "type": "VARCHAR(256)"},
                    {"name": "place_of_supply", "type": "VARCHAR(256)"},
                    {"name": "is_invoice", "type": "SMALLINT"},
                    {"name": "is_accounting_voucher", "type": "SMALLINT"},
                    {"name": "is_inventory_voucher", "type": "SMALLINT"},
                    {"name": "is_order_voucher", "type": "SMALLINT"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_accounting.guid = trn_voucher.guid",
                    "trn_inventory.guid = trn_voucher.guid",
                    "trn_cost_centre.guid = trn_voucher.guid"
                ],
                "description": "Main transaction records"
            },
            
            "trn_accounting": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64)"},
                    {"name": "ledger", "type": "VARCHAR(1024)"},
                    {"name": "amount", "type": "NUMERIC DEFAULT 0"},
                    {"name": "amount_forex", "type": "NUMERIC DEFAULT 0"},
                    {"name": "currency", "type": "VARCHAR(16)"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_accounting.guid = trn_voucher.guid",
                    "trn_accounting.ledger = mst_ledger.name"
                ],
                "description": "Accounting entries"
            },
            
            "trn_inventory": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64)"},
                    {"name": "item", "type": "VARCHAR(1024)"},
                    {"name": "quantity", "type": "NUMERIC DEFAULT 0"},
                    {"name": "rate", "type": "NUMERIC DEFAULT 0"},
                    {"name": "amount", "type": "NUMERIC DEFAULT 0"},
                    {"name": "additional_amount", "type": "NUMERIC DEFAULT 0"},
                    {"name": "discount_amount", "type": "NUMERIC DEFAULT 0"},
                    {"name": "godown", "type": "VARCHAR(1024)"},
                    {"name": "tracking_number", "type": "VARCHAR(256)"},
                    {"name": "order_number", "type": "VARCHAR(256)"},
                    {"name": "order_duedate", "type": "DATE"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_inventory.guid = trn_voucher.guid",
                    "trn_inventory.item = mst_stock_item.name"
                ],
                "description": "Stock movements"
            },
            
            "trn_payhead": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64)"},
                    {"name": "category", "type": "VARCHAR(1024)"},
                    {"name": "employee_name", "type": "VARCHAR(1024)"},
                    {"name": "employee_sort_order", "type": "INTEGER DEFAULT 0"},
                    {"name": "payhead_name", "type": "VARCHAR(1024)"},
                    {"name": "payhead_sort_order", "type": "INTEGER DEFAULT 0"},
                    {"name": "amount", "type": "NUMERIC DEFAULT 0"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_payhead.guid = trn_voucher.guid",
                    "trn_payhead.employee_name = mst_employee.name"
                ],
                "description": "Payroll transactions"
            },
            
            "trn_attendance": {
                "columns": [
                    {"name": "guid", "type": "VARCHAR(64)"},
                    {"name": "employee_name", "type": "VARCHAR(1024)"},
                    {"name": "attendancetype_name", "type": "VARCHAR(1024)"},
                    {"name": "time_value", "type": "NUMERIC DEFAULT 0"},
                    {"name": "type_value", "type": "NUMERIC DEFAULT 0"},
                    {"name": "user_id", "type": "VARCHAR(255)"},
                    {"name": "company_name", "type": "VARCHAR(255)"}
                ],
                "relationships": [
                    "trn_attendance.guid = trn_voucher.guid",
                    "trn_attendance.employee_name = mst_employee.name"
                ],
                "description": "Attendance records"
            }
        }
        
        # Create actual database with sample data
        try:
            self._create_tally_database()
        except Exception as e:
            print(f"Error creating database: {e}")
            # Even if database creation fails, we can still provide schema info
    
    def _create_tally_database(self) -> None:
        """Create SQLite database with Tally ERP structure and sample data"""
        # Close existing connection if any
        if self.connection:
            self.connection.close()
        
        # Create new connection
        self.connection = sqlite3.connect(self.db_path)
        cursor = self.connection.cursor()
        
        # Enable foreign keys
        cursor.execute("PRAGMA foreign_keys = ON")
        
        # Drop existing tables (in correct order to avoid foreign key constraints)
        drop_order = [
            'trn_attendance', 'trn_payhead', 'trn_inventory', 'trn_accounting', 
            'trn_voucher', 'mst_employee', 'mst_stock_item', 'mst_vouchertype', 
            'mst_ledger', 'mst_group', 'config'
        ]
        
        for table in drop_order:
            try:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
            except:
                pass  # Ignore errors if table doesn't exist
        
        # Create tables with full Tally ERP structure
        self._create_config_table(cursor)
        self._create_master_tables(cursor)
        self._create_transaction_tables(cursor)
        
        # Insert sample data
        self._insert_sample_data(cursor)
        
        self.connection.commit()
    
    def _create_config_table(self, cursor):
        """Create configuration table"""
        cursor.execute("""
            CREATE TABLE config (
                name VARCHAR(64) NOT NULL PRIMARY KEY,
                value VARCHAR(1024),
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
    
    def _create_master_tables(self, cursor):
        """Create master tables"""
        
        # Account Groups
        cursor.execute("""
            CREATE TABLE mst_group (
                guid VARCHAR(64) NOT NULL PRIMARY KEY,
                name VARCHAR(1024) NOT NULL DEFAULT '',
                parent VARCHAR(1024) NOT NULL DEFAULT '',
                primary_group VARCHAR(1024) NOT NULL DEFAULT '',
                is_revenue SMALLINT,
                is_deemedpositive SMALLINT,
                is_reserved SMALLINT,
                affects_gross_profit SMALLINT,
                sort_position INTEGER,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Ledger Master
        cursor.execute("""
            CREATE TABLE mst_ledger (
                guid VARCHAR(64) NOT NULL PRIMARY KEY,
                name VARCHAR(1024) NOT NULL DEFAULT '',
                parent VARCHAR(1024) NOT NULL DEFAULT '',
                alias VARCHAR(256) NOT NULL DEFAULT '',
                description VARCHAR(64) NOT NULL DEFAULT '',
                notes VARCHAR(64) NOT NULL DEFAULT '',
                is_revenue SMALLINT,
                is_deemedpositive SMALLINT,
                opening_balance NUMERIC DEFAULT 0,
                closing_balance NUMERIC DEFAULT 0,
                mailing_name VARCHAR(256) NOT NULL DEFAULT '',
                mailing_address VARCHAR(1024) NOT NULL DEFAULT '',
                mailing_state VARCHAR(256) NOT NULL DEFAULT '',
                mailing_country VARCHAR(256) NOT NULL DEFAULT '',
                mailing_pincode VARCHAR(64) NOT NULL DEFAULT '',
                email VARCHAR(256) NOT NULL DEFAULT '',
                it_pan VARCHAR(64) NOT NULL DEFAULT '',
                gstn VARCHAR(64) NOT NULL DEFAULT '',
                gst_registration_type VARCHAR(64) NOT NULL DEFAULT '',
                gst_supply_type VARCHAR(64) NOT NULL DEFAULT '',
                gst_duty_head VARCHAR(16) NOT NULL DEFAULT '',
                tax_rate NUMERIC DEFAULT 0,
                bank_account_holder VARCHAR(256) NOT NULL DEFAULT '',
                bank_account_number VARCHAR(64) NOT NULL DEFAULT '',
                bank_ifsc VARCHAR(64) NOT NULL DEFAULT '',
                bank_swift VARCHAR(64) NOT NULL DEFAULT '',
                bank_name VARCHAR(64) NOT NULL DEFAULT '',
                bank_branch VARCHAR(64) NOT NULL DEFAULT '',
                bill_credit_period INTEGER NOT NULL DEFAULT 0,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Voucher Types
        cursor.execute("""
            CREATE TABLE mst_vouchertype (
                guid VARCHAR(64) NOT NULL PRIMARY KEY,
                name VARCHAR(1024) NOT NULL DEFAULT '',
                parent VARCHAR(1024) NOT NULL DEFAULT '',
                numbering_method VARCHAR(64) NOT NULL DEFAULT '',
                is_deemedpositive SMALLINT,
                affects_stock SMALLINT,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Stock Items
        cursor.execute("""
            CREATE TABLE mst_stock_item (
                guid VARCHAR(64) NOT NULL PRIMARY KEY,
                name VARCHAR(1024) NOT NULL DEFAULT '',
                parent VARCHAR(1024) NOT NULL DEFAULT '',
                alias VARCHAR(256) NOT NULL DEFAULT '',
                description VARCHAR(64) NOT NULL DEFAULT '',
                notes VARCHAR(64) NOT NULL DEFAULT '',
                part_number VARCHAR(256) NOT NULL DEFAULT '',
                uom VARCHAR(32) NOT NULL DEFAULT '',
                alternate_uom VARCHAR(32) NOT NULL DEFAULT '',
                conversion INTEGER NOT NULL DEFAULT 0,
                opening_balance NUMERIC DEFAULT 0,
                opening_rate NUMERIC DEFAULT 0,
                opening_value NUMERIC DEFAULT 0,
                closing_balance NUMERIC DEFAULT 0,
                closing_rate NUMERIC DEFAULT 0,
                closing_value NUMERIC DEFAULT 0,
                costing_method VARCHAR(32) NOT NULL DEFAULT '',
                gst_type_of_supply VARCHAR(32) DEFAULT '',
                gst_hsn_code VARCHAR(64) DEFAULT '',
                gst_hsn_description VARCHAR(256) DEFAULT '',
                gst_rate NUMERIC DEFAULT 0,
                gst_taxability VARCHAR(32) DEFAULT '',
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Employee Master
        cursor.execute("""
            CREATE TABLE mst_employee (
                guid VARCHAR(64) NOT NULL PRIMARY KEY,
                name VARCHAR(1024) NOT NULL DEFAULT '',
                parent VARCHAR(1024) NOT NULL DEFAULT '',
                id_number VARCHAR(256) NOT NULL DEFAULT '',
                date_of_joining DATE,
                date_of_release DATE,
                designation VARCHAR(64) NOT NULL DEFAULT '',
                function_role VARCHAR(64) NOT NULL DEFAULT '',
                location VARCHAR(256) NOT NULL DEFAULT '',
                gender VARCHAR(32) NOT NULL DEFAULT '',
                date_of_birth DATE,
                blood_group VARCHAR(32) NOT NULL DEFAULT '',
                father_mother_name VARCHAR(256) NOT NULL DEFAULT '',
                spouse_name VARCHAR(256) NOT NULL DEFAULT '',
                address VARCHAR(256) NOT NULL DEFAULT '',
                mobile VARCHAR(32) NOT NULL DEFAULT '',
                email VARCHAR(64) NOT NULL DEFAULT '',
                pan VARCHAR(32) NOT NULL DEFAULT '',
                aadhar VARCHAR(32) NOT NULL DEFAULT '',
                uan VARCHAR(32) NOT NULL DEFAULT '',
                pf_number VARCHAR(32) NOT NULL DEFAULT '',
                pf_joining_date DATE,
                pf_relieving_date DATE,
                pr_account_number VARCHAR(32) NOT NULL DEFAULT '',
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
    
    def _create_transaction_tables(self, cursor):
        """Create transaction tables"""
        
        # Main Voucher Table
        cursor.execute("""
            CREATE TABLE trn_voucher (
                guid VARCHAR(64) NOT NULL PRIMARY KEY,
                date DATE NOT NULL,
                voucher_type VARCHAR(1024) NOT NULL,
                voucher_number VARCHAR(64) NOT NULL DEFAULT '',
                reference_number VARCHAR(64) NOT NULL DEFAULT '',
                reference_date DATE,
                narration VARCHAR(4000) NOT NULL DEFAULT '',
                party_name VARCHAR(256) NOT NULL,
                place_of_supply VARCHAR(256) NOT NULL,
                is_invoice SMALLINT,
                is_accounting_voucher SMALLINT,
                is_inventory_voucher SMALLINT,
                is_order_voucher SMALLINT,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Accounting Entries
        cursor.execute("""
            CREATE TABLE trn_accounting (
                guid VARCHAR(64) NOT NULL DEFAULT '',
                ledger VARCHAR(1024) NOT NULL DEFAULT '',
                amount NUMERIC NOT NULL DEFAULT 0,
                amount_forex NUMERIC NOT NULL DEFAULT 0,
                currency VARCHAR(16) NOT NULL DEFAULT '',
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Inventory Transactions
        cursor.execute("""
            CREATE TABLE trn_inventory (
                guid VARCHAR(64) NOT NULL DEFAULT '',
                item VARCHAR(1024) NOT NULL DEFAULT '',
                quantity NUMERIC NOT NULL DEFAULT 0,
                rate NUMERIC NOT NULL DEFAULT 0,
                amount NUMERIC NOT NULL DEFAULT 0,
                additional_amount NUMERIC NOT NULL DEFAULT 0,
                discount_amount NUMERIC NOT NULL DEFAULT 0,
                godown VARCHAR(1024),
                tracking_number VARCHAR(256),
                order_number VARCHAR(256),
                order_duedate DATE,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Payroll Transactions
        cursor.execute("""
            CREATE TABLE trn_payhead (
                guid VARCHAR(64) NOT NULL DEFAULT '',
                category VARCHAR(1024) NOT NULL DEFAULT '',
                employee_name VARCHAR(1024) NOT NULL DEFAULT '',
                employee_sort_order INTEGER NOT NULL DEFAULT 0,
                payhead_name VARCHAR(1024) NOT NULL DEFAULT '',
                payhead_sort_order INTEGER NOT NULL DEFAULT 0,
                amount NUMERIC NOT NULL DEFAULT 0,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
        
        # Attendance Records
        cursor.execute("""
            CREATE TABLE trn_attendance (
                guid VARCHAR(64) NOT NULL DEFAULT '',
                employee_name VARCHAR(1024) NOT NULL DEFAULT '',
                attendancetype_name VARCHAR(1024) NOT NULL DEFAULT '',
                time_value NUMERIC NOT NULL DEFAULT 0,
                type_value NUMERIC NOT NULL DEFAULT 0,
                user_id VARCHAR(255),
                company_name VARCHAR(255)
            )
        """)
    
    def _insert_sample_data(self, cursor):
        """Insert comprehensive sample data"""
        
        # Sample user and company
        user_id = "demo_user"
        company_name = "Demo Company Ltd"
        
        try:
            # Configuration data
            config_data = [
                ("company_name", "Demo Company Ltd", user_id, company_name),
                ("financial_year", "2024-25", user_id, company_name),
                ("base_currency", "INR", user_id, company_name)
            ]
            cursor.executemany(
                "INSERT INTO config (name, value, user_id, company_name) VALUES (?, ?, ?, ?)",
                config_data
            )
            
            # Account Groups
            groups_data = [
                ("grp001", "Assets", "", "Assets", 0, 1, 0, 0, 1, user_id, company_name),
                ("grp002", "Current Assets", "Assets", "Assets", 0, 1, 0, 0, 2, user_id, company_name),
                ("grp003", "Fixed Assets", "Assets", "Assets", 0, 1, 0, 0, 3, user_id, company_name),
                ("grp004", "Liabilities", "", "Liabilities", 0, 0, 0, 0, 4, user_id, company_name),
                ("grp005", "Current Liabilities", "Liabilities", "Liabilities", 0, 0, 0, 0, 5, user_id, company_name),
                ("grp006", "Income", "", "Income", 1, 0, 0, 1, 6, user_id, company_name),
                ("grp007", "Expenses", "", "Expenses", 0, 1, 0, 1, 7, user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO mst_group (guid, name, parent, primary_group, is_revenue, 
                   is_deemedpositive, is_reserved, affects_gross_profit, sort_position, 
                   user_id, company_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                groups_data
            )
            
            # Ledgers
            ledgers_data = [
                ("led001", "Cash", "Current Assets", "CASH001", "Cash Account", "", 0, 1, 50000, 75000, 
                 "", "", "", "", "", "", "", "", "", "", "", 0, "", "", "", "", "", "", 0, user_id, company_name),
                ("led002", "Bank Account - SBI", "Current Assets", "SBI001", "State Bank Account", "", 0, 1, 100000, 150000,
                 "", "", "", "", "", "", "", "", "", "", "", 0, "Demo Company Ltd", "123456789", "SBIN0001234", "", "State Bank of India", "Main Branch", 0, user_id, company_name),
                ("led003", "Sales Account", "Income", "SALES001", "Sales Revenue", "", 1, 0, 0, -200000,
                 "", "", "", "", "", "", "", "", "", "", "", 0, "", "", "", "", "", "", 0, user_id, company_name),
                ("led004", "Purchase Account", "Expenses", "PURCH001", "Purchase Expenses", "", 0, 1, 0, 80000,
                 "", "", "", "", "", "", "", "", "", "", "", 0, "", "", "", "", "", "", 0, user_id, company_name),
                ("led005", "Salary Expenses", "Expenses", "SAL001", "Employee Salaries", "", 0, 1, 0, 45000,
                 "", "", "", "", "", "", "", "", "", "", "", 0, "", "", "", "", "", "", 0, user_id, company_name),
                ("led006", "Customer A", "Current Assets", "CUST001", "Receivables", "", 0, 1, 0, 25000,
                 "Customer A Ltd", "123 Business St", "Maharashtra", "India", "400001", "customer@example.com", "ABCDE1234F", "27ABCDE1234F1Z5", "Regular", "Goods", "", 18, "", "", "", "", "", "", 30, user_id, company_name),
                ("led007", "Supplier B", "Current Liabilities", "SUPP001", "Payables", "", 0, 0, 0, -15000,
                 "Supplier B Pvt Ltd", "456 Supply Ave", "Karnataka", "India", "560001", "supplier@example.com", "FGHIJ5678K", "29FGHIJ5678K1Z5", "Regular", "Goods", "", 18, "", "", "", "", "", "", 45, user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO mst_ledger (guid, name, parent, alias, description, notes, is_revenue, 
                   is_deemedpositive, opening_balance, closing_balance, mailing_name, mailing_address, 
                   mailing_state, mailing_country, mailing_pincode, email, it_pan, gstn, 
                   gst_registration_type, gst_supply_type, gst_duty_head, tax_rate, bank_account_holder, 
                   bank_account_number, bank_ifsc, bank_swift, bank_name, bank_branch, 
                   bill_credit_period, user_id, company_name) VALUES 
                   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                ledgers_data
            )
            
            # Voucher Types
            voucher_types_data = [
                ("vt001", "Sales", "", "Auto", 0, 1, user_id, company_name),
                ("vt002", "Purchase", "", "Auto", 1, 1, user_id, company_name),
                ("vt003", "Payment", "", "Auto", 1, 0, user_id, company_name),
                ("vt004", "Receipt", "", "Auto", 0, 0, user_id, company_name),
                ("vt005", "Journal", "", "Auto", 0, 0, user_id, company_name),
                ("vt006", "Payroll", "", "Auto", 1, 0, user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO mst_vouchertype (guid, name, parent, numbering_method, 
                   is_deemedpositive, affects_stock, user_id, company_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                voucher_types_data
            )
            
            # Stock Items
            stock_items_data = [
                ("si001", "Laptop Dell Inspiron", "Electronics", "LAPTOP001", "Business Laptop", "High performance laptop", 
                 "LT-DELL-001", "Nos", "", 1, 10, 45000, 450000, 8, 50000, 400000, "FIFO", 
                 "Goods", "84713020", "Laptops and computers", 18, "Taxable", user_id, company_name),
                ("si002", "Office Chair", "Furniture", "CHAIR001", "Ergonomic Chair", "Comfortable office chair", 
                 "CH-ERG-001", "Nos", "", 1, 25, 8000, 200000, 20, 9000, 180000, "FIFO", 
                 "Goods", "94013000", "Office furniture", 18, "Taxable", user_id, company_name),
                ("si003", "A4 Paper", "Stationery", "PAPER001", "Copy Paper", "White A4 copy paper", 
                 "PP-A4-001", "Reams", "", 1, 100, 250, 25000, 80, 280, 22400, "FIFO", 
                 "Goods", "48025510", "Paper and paperboard", 12, "Taxable", user_id, company_name),
                ("si004", "Printer HP LaserJet", "Electronics", "PRINT001", "Laser Printer", "Black & white printer", 
                 "PR-HP-001", "Nos", "", 1, 5, 15000, 75000, 3, 18000, 54000, "FIFO", 
                 "Goods", "84433210", "Printing machinery", 18, "Taxable", user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO mst_stock_item (guid, name, parent, alias, description, notes, part_number, 
                   uom, alternate_uom, conversion, opening_balance, opening_rate, opening_value, 
                   closing_balance, closing_rate, closing_value, costing_method, gst_type_of_supply, 
                   gst_hsn_code, gst_hsn_description, gst_rate, gst_taxability, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                stock_items_data
            )
            
            # Employees
            employees_data = [
                ("emp001", "John Smith", "Employees", "EMP001", "2023-01-15", None, "Software Developer", 
                 "Development", "Mumbai", "Male", "1990-05-20", "O+", "Robert Smith", "Jane Smith", 
                 "123 Tech Park, Mumbai", "9876543210", "john.smith@company.com", "ABCDE1234F", 
                 "123456789012", "123456789012", "PF001234", "2023-01-15", None, "PR001234", user_id, company_name),
                ("emp002", "Priya Sharma", "Employees", "EMP002", "2023-03-01", None, "HR Manager", 
                 "Human Resources", "Delhi", "Female", "1988-08-15", "A+", "Raj Sharma", "Amit Sharma", 
                 "456 Business Center, Delhi", "9876543211", "priya.sharma@company.com", "FGHIJ5678K", 
                 "234567890123", "234567890123", "PF005678", "2023-03-01", None, "PR005678", user_id, company_name),
                ("emp003", "Raj Patel", "Employees", "EMP003", "2022-11-10", None, "Sales Executive", 
                 "Sales", "Ahmedabad", "Male", "1992-12-03", "B+", "Kishore Patel", "Meera Patel", 
                 "789 Commerce Hub, Ahmedabad", "9876543212", "raj.patel@company.com", "KLMNO9012P", 
                 "345678901234", "345678901234", "PF009012", "2022-11-10", None, "PR009012", user_id, company_name),
                ("emp004", "Anita Kumar", "Employees", "EMP004", "2023-06-01", None, "Accountant", 
                 "Finance", "Chennai", "Female", "1985-03-25", "AB+", "Suresh Kumar", "Ravi Kumar", 
                 "321 Finance Street, Chennai", "9876543213", "anita.kumar@company.com", "QRSTU3456V", 
                 "456789012345", "456789012345", "PF003456", "2023-06-01", None, "PR003456", user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO mst_employee (guid, name, parent, id_number, date_of_joining, date_of_release, 
                   designation, function_role, location, gender, date_of_birth, blood_group, 
                   father_mother_name, spouse_name, address, mobile, email, pan, aadhar, uan, 
                   pf_number, pf_joining_date, pf_relieving_date, pr_account_number, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                employees_data
            )
            
            # Sample Vouchers
            vouchers_data = [
                ("v001", "2024-01-15", "Sales", "S001", "INV001", "2024-01-15", "Sale of laptops to Customer A", 
                 "Customer A", "Maharashtra", 1, 1, 1, 0, user_id, company_name),
                ("v002", "2024-01-20", "Purchase", "P001", "PO001", "2024-01-20", "Purchase of office chairs", 
                 "Supplier B", "Karnataka", 1, 1, 1, 0, user_id, company_name),
                ("v003", "2024-01-25", "Payment", "PAY001", "CHQ001", "2024-01-25", "Salary payment for January", 
                 "", "", 0, 1, 0, 0, user_id, company_name),
                ("v004", "2024-02-01", "Receipt", "REC001", "TT001", "2024-02-01", "Receipt from Customer A", 
                 "Customer A", "Maharashtra", 0, 1, 0, 0, user_id, company_name),
                ("v005", "2024-02-10", "Sales", "S002", "INV002", "2024-02-10", "Sale of printers and paper", 
                 "Customer A", "Maharashtra", 1, 1, 1, 0, user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO trn_voucher (guid, date, voucher_type, voucher_number, reference_number, 
                   reference_date, narration, party_name, place_of_supply, is_invoice, 
                   is_accounting_voucher, is_inventory_voucher, is_order_voucher, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                vouchers_data
            )
            
            # Accounting Entries
            accounting_data = [
                # Sales voucher v001
                ("v001", "Customer A", 59000, 0, "INR", user_id, company_name),
                ("v001", "Sales Account", -50000, 0, "INR", user_id, company_name),
                ("v001", "GST Output", -9000, 0, "INR", user_id, company_name),
                
                # Purchase voucher v002
                ("v002", "Purchase Account", 36000, 0, "INR", user_id, company_name),
                ("v002", "GST Input", 6480, 0, "INR", user_id, company_name),
                ("v002", "Supplier B", -42480, 0, "INR", user_id, company_name),
                
                # Payment voucher v003 (Salary)
                ("v003", "Salary Expenses", 45000, 0, "INR", user_id, company_name),
                ("v003", "Bank Account - SBI", -45000, 0, "INR", user_id, company_name),
                
                # Receipt voucher v004
                ("v004", "Bank Account - SBI", 59000, 0, "INR", user_id, company_name),
                ("v004", "Customer A", -59000, 0, "INR", user_id, company_name),
                
                # Sales voucher v005
                ("v005", "Customer A", 26040, 0, "INR", user_id, company_name),
                ("v005", "Sales Account", -22500, 0, "INR", user_id, company_name),
                ("v005", "GST Output", -3540, 0, "INR", user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO trn_accounting (guid, ledger, amount, amount_forex, currency, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                accounting_data
            )
            
            # Inventory Transactions
            inventory_data = [
                # Sales voucher v001 - Laptops
                ("v001", "Laptop Dell Inspiron", -1, 50000, -50000, 0, 0, "Main Godown", "", "SO001", "2024-01-30", user_id, company_name),
                
                # Purchase voucher v002 - Office Chairs
                ("v002", "Office Chair", 4, 9000, 36000, 0, 0, "Main Godown", "", "PO001", "2024-02-05", user_id, company_name),
                
                # Sales voucher v005 - Printers and Paper
                ("v005", "Printer HP LaserJet", -1, 18000, -18000, 0, 0, "Main Godown", "", "SO002", "2024-02-15", user_id, company_name),
                ("v005", "A4 Paper", -20, 225, -4500, 0, 0, "Main Godown", "", "SO002", "2024-02-15", user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO trn_inventory (guid, item, quantity, rate, amount, additional_amount, 
                   discount_amount, godown, tracking_number, order_number, order_duedate, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                inventory_data
            )
            
            # Payroll Data
            payroll_data = [
                # January Salary - John Smith
                ("v003", "Salary", "John Smith", 1, "Basic Pay", 1, 25000, user_id, company_name),
                ("v003", "Salary", "John Smith", 1, "HRA", 2, 10000, user_id, company_name),
                ("v003", "Salary", "John Smith", 1, "Special Allowance", 3, 5000, user_id, company_name),
                ("v003", "Salary", "John Smith", 1, "PF Deduction", 4, -3000, user_id, company_name),
                ("v003", "Salary", "John Smith", 1, "TDS", 5, -2000, user_id, company_name),
                
                # January Salary - Priya Sharma
                ("v003", "Salary", "Priya Sharma", 2, "Basic Pay", 1, 30000, user_id, company_name),
                ("v003", "Salary", "Priya Sharma", 2, "HRA", 2, 12000, user_id, company_name),
                ("v003", "Salary", "Priya Sharma", 2, "Special Allowance", 3, 8000, user_id, company_name),
                ("v003", "Salary", "Priya Sharma", 2, "PF Deduction", 4, -3600, user_id, company_name),
                ("v003", "Salary", "Priya Sharma", 2, "TDS", 5, -3400, user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO trn_payhead (guid, category, employee_name, employee_sort_order, 
                   payhead_name, payhead_sort_order, amount, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                payroll_data
            )
            
            # Attendance Data
            attendance_data = [
                ("v003", "John Smith", "Regular Hours", 160, 1, user_id, company_name),
                ("v003", "John Smith", "Overtime Hours", 10, 1, user_id, company_name),
                ("v003", "Priya Sharma", "Regular Hours", 168, 1, user_id, company_name),
                ("v003", "Priya Sharma", "Leave Hours", -8, 1, user_id, company_name),
                ("v003", "Raj Patel", "Regular Hours", 172, 1, user_id, company_name),
                ("v003", "Anita Kumar", "Regular Hours", 165, 1, user_id, company_name)
            ]
            cursor.executemany(
                """INSERT INTO trn_attendance (guid, employee_name, attendancetype_name, 
                   time_value, type_value, user_id, company_name) 
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                attendance_data
            )
            
        except Exception as e:
            print(f"Error inserting sample data: {e}")
            # Continue even if sample data insertion fails
    
    def get_schema(self) -> Dict:
        """Get current schema"""
        return self.schema
    
    def get_connection(self) -> Optional[sqlite3.Connection]:
        """Get database connection"""
        if not self.connection:
            try:
                self.connection = sqlite3.connect(self.db_path)
            except Exception as e:
                print(f"Error connecting to database: {e}")
                return None
        return self.connection
    
    def load_sample_schema(self) -> None:
        """Load sample schema - redirect to Tally schema"""
        self.load_tally_schema()
    
    def get_table_statistics(self) -> Dict[str, Dict]:
        """Get statistics for each table with error handling"""
        stats = {}
        
        # If no connection or schema, return empty stats
        if not self.connection or not self.schema:
            return {table: {'row_count': 0, 'column_count': 0, 'relationship_count': 0, 'error': 'No connection'} 
                    for table in self.schema.keys()} if self.schema else {}
        
        try:
            cursor = self.connection.cursor()
            
            for table in self.schema.keys():
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    row_count = cursor.fetchone()[0]
                    
                    stats[table] = {
                        'row_count': row_count,
                        'column_count': len(self.schema[table]['columns']),
                        'relationship_count': len(self.schema[table].get('relationships', []))
                    }
                except Exception as e:
                    stats[table] = {
                        'row_count': 0,
                        'column_count': len(self.schema[table]['columns']),
                        'relationship_count': len(self.schema[table].get('relationships', [])),
                        'error': str(e)
                    }
            
            cursor.close()
            
        except Exception as e:
            print(f"Error getting table statistics: {e}")
            # Return default stats if there's an error
            for table in self.schema.keys():
                stats[table] = {
                    'row_count': 0,
                    'column_count': len(self.schema[table]['columns']),
                    'relationship_count': len(self.schema[table].get('relationships', [])),
                    'error': f'Database error: {str(e)}'
                }
        
        return stats
    
    def get_sample_queries(self) -> List[str]:
        """Get sample queries for Tally ERP system"""
        return [
            "Show all employees with their designations",
            "Get total sales amount for January 2024",
            "Find items with closing balance less than 10",
            "Show employee payroll summary for John Smith",
            "Get all purchase vouchers from Supplier B",
            "Find ledgers with GST registration",
            "Show inventory movements for laptops",
            "Get attendance summary for all employees",
            "Find vouchers created in last 30 days",
            "Show trial balance for current year",
            "Get stock items with GST rate 18%",
            "Find employees joined in 2023"
        ]
    
    def validate_user_access(self, user_id: str, company_name: str) -> bool:
        """Validate user access to company data"""
        if not self.connection:
            return False
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM config WHERE user_id = ? AND company_name = ?",
                (user_id, company_name)
            )
            
            result = cursor.fetchone()
            cursor.close()
            return result[0] > 0 if result else False
            
        except Exception as e:
            print(f"Error validating user access: {e}")
            return False
    
    def ensure_connection(self) -> bool:
        """Ensure database connection is available"""
        if self.connection:
            return True
            
        try:
            self.connection = sqlite3.connect(self.db_path)
            return True
        except Exception as e:
            print(f"Failed to establish database connection: {e}")
            return False
