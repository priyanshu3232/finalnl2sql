import re
from typing import Dict, List, Optional, Set
from dataclasses import dataclass

@dataclass
class ParsedQuery:
    """Structured representation of a parsed natural language query"""
    action: str  # SELECT, INSERT, UPDATE, DELETE
    tables: List[str]
    columns: List[str]
    conditions: List[Dict[str, any]]
    joins: List[Dict[str, str]]
    aggregations: List[Dict[str, str]]
    group_by: List[str]
    order_by: List[Dict[str, str]]
    limit: Optional[int]
    user_filters: Dict[str, str]  # For user_id and company_name

class NaturalLanguageParser:
    def __init__(self):
        # Tally-specific action keywords
        self.action_patterns = {
            'SELECT': [
                'show', 'display', 'get', 'find', 'list', 'fetch', 'retrieve', 'select', 
                'give me', 'what are', 'which', 'view', 'report', 'summary', 'details'
            ],
            'INSERT': ['insert', 'add', 'create', 'new', 'entry'],
            'UPDATE': ['update', 'modify', 'change', 'set', 'edit'],
            'DELETE': ['delete', 'remove', 'drop']
        }
        
        # Tally ERP specific table mappings
        self.table_aliases = {
            'employees': 'mst_employee',
            'employee': 'mst_employee',
            'staff': 'mst_employee',
            'workers': 'mst_employee',
            
            'ledgers': 'mst_ledger',
            'ledger': 'mst_ledger',
            'accounts': 'mst_ledger',
            'account': 'mst_ledger',
            'parties': 'mst_ledger',
            'customers': 'mst_ledger',
            'suppliers': 'mst_ledger',
            'vendors': 'mst_ledger',
            
            'items': 'mst_stock_item',
            'stock': 'mst_stock_item',
            'inventory': 'mst_stock_item',
            'products': 'mst_stock_item',
            'goods': 'mst_stock_item',
            
            'vouchers': 'trn_voucher',
            'voucher': 'trn_voucher',
            'transactions': 'trn_voucher',
            'entries': 'trn_voucher',
            
            'sales': 'trn_voucher',
            'purchases': 'trn_voucher',
            'receipts': 'trn_voucher',
            'payments': 'trn_voucher',
            
            'accounting': 'trn_accounting',
            'journal': 'trn_accounting',
            
            'payroll': 'trn_payhead',
            'salary': 'trn_payhead',
            'wages': 'trn_payhead',
            
            'attendance': 'trn_attendance'
        }
        
        # Tally-specific column mappings
        self.column_aliases = {
            'employee_name': 'name',
            'emp_name': 'name',
            'staff_name': 'name',
            'party_name': 'name',
            'customer_name': 'name',
            'supplier_name': 'name',
            'vendor_name': 'name',
            'ledger_name': 'name',
            'account_name': 'name',
            'item_name': 'name',
            'stock_name': 'name',
            'product_name': 'name',
            
            'emp_id': 'id_number',
            'employee_id': 'id_number',
            'staff_id': 'id_number',
            
            'joining_date': 'date_of_joining',
            'doj': 'date_of_joining',
            'start_date': 'date_of_joining',
            
            'salary_amount': 'amount',
            'pay_amount': 'amount',
            'wage_amount': 'amount',
            
            'stock_balance': 'closing_balance',
            'inventory_balance': 'closing_balance',
            'current_balance': 'closing_balance',
            
            'voucher_date': 'date',
            'transaction_date': 'date',
            'entry_date': 'date',
            
            'gst_number': 'gstn',
            'pan_number': 'it_pan',
            'mobile_number': 'mobile',
            'phone_number': 'mobile'
        }
        
        # Tally-specific aggregation keywords
        self.aggregation_keywords = {
            'count': ['count', 'how many', 'number of', 'total count'],
            'sum': ['sum', 'total', 'aggregate', 'add up', 'total amount'],
            'avg': ['average', 'avg', 'mean'],
            'max': ['maximum', 'max', 'highest', 'largest', 'top'],
            'min': ['minimum', 'min', 'lowest', 'smallest', 'bottom']
        }
        
        # Tally-specific condition keywords
        self.condition_keywords = {
            '=': ['is', 'equals', 'equal to', '=', 'named', 'called'],
            '>': ['greater than', 'more than', 'over', 'above', 'exceeds', '>'],
            '<': ['less than', 'below', 'under', 'fewer than', '<'],
            '>=': ['at least', 'greater than or equal', 'minimum', '>='],
            '<=': ['at most', 'less than or equal', 'maximum', '<='],
            '!=': ['not equal', 'not', 'different from', 'other than', '!='],
            'LIKE': ['contains', 'includes', 'has', 'with', 'like', 'similar to'],
            'IN': ['in', 'among', 'one of', 'any of'],
            'BETWEEN': ['between', 'from ... to', 'range', 'within']
        }
        
        # Tally ERP specific filters
        self.voucher_type_filters = {
            'sales': "voucher_type = 'Sales'",
            'purchase': "voucher_type = 'Purchase'",
            'payment': "voucher_type = 'Payment'", 
            'receipt': "voucher_type = 'Receipt'",
            'journal': "voucher_type = 'Journal'",
            'payroll': "voucher_type = 'Payroll'"
        }
        
        # Date patterns for Tally
        self.date_patterns = [
            (r'in (\d{4})', 'year'),
            (r'in (january|february|march|april|may|june|july|august|september|october|november|december) (\d{4})', 'month_year'),
            (r'in the last (\d+) (day|month|year)s?', 'relative_date'),
            (r'since (\d{4}-\d{2}-\d{2})', 'since_date'),
            (r'before (\d{4}-\d{2}-\d{2})', 'before_date'),
            (r'on (\d{4}-\d{2}-\d{2})', 'on_date'),
            (r'today', 'today'),
            (r'this (month|year)', 'current_period'),
            (r'last (month|year)', 'last_period')
        ]
        
        # GST-specific patterns
        self.gst_patterns = {
            'with gst': 'gstn IS NOT NULL',
            'without gst': 'gstn IS NULL',
            'gst registered': 'gstn IS NOT NULL',
            'non gst': 'gstn IS NULL'
        }
        
    def parse(self, query: str, schema: Dict, user_id: str = "demo_user", company_name: str = "Demo Company Ltd") -> ParsedQuery:
        """Parse natural language query into structured format with Tally context"""
        query_lower = query.lower().strip()
        
        # Detect action
        action = self._detect_action(query_lower)
        
        # Set user filters (mandatory for Tally)
        user_filters = {
            'user_id': user_id,
            'company_name': company_name
        }
        
        # Extract components based on action
        if action == 'SELECT':
            return self._parse_select_query(query_lower, schema, user_filters)
        elif action == 'INSERT':
            return self._parse_insert_query(query_lower, schema, user_filters)
        elif action == 'UPDATE':
            return self._parse_update_query(query_lower, schema, user_filters)
        elif action == 'DELETE':
            return self._parse_delete_query(query_lower, schema, user_filters)
        else:
            return self._parse_select_query(query_lower, schema, user_filters)
    
    def _detect_action(self, query: str) -> str:
        """Detect the SQL action from the query"""
        for action, keywords in self.action_patterns.items():
            for keyword in keywords:
                if keyword in query:
                    return action
        return 'SELECT'  # Default
    
    def _parse_select_query(self, query: str, schema: Dict, user_filters: Dict) -> ParsedQuery:
        """Parse a SELECT query with Tally ERP context"""
        tables = self._extract_tables(query, schema)
        columns = self._extract_columns(query, schema, tables)
        conditions = self._extract_conditions(query, schema, tables)
        joins = self._extract_joins(query, tables, schema)
        aggregations = self._extract_aggregations(query, columns)
        group_by = self._extract_group_by(query, columns)
        order_by = self._extract_order_by(query, columns)
        limit = self._extract_limit(query)
        
        # Add Tally-specific filters
        conditions.extend(self._extract_tally_filters(query, tables))
        
        return ParsedQuery(
            action='SELECT',
            tables=tables,
            columns=columns,
            conditions=conditions,
            joins=joins,
            aggregations=aggregations,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            user_filters=user_filters
        )
    
    def _extract_tables(self, query: str, schema: Dict) -> List[str]:
        """Extract table names from query with Tally aliases"""
        tables = []
        
        # Check for Tally-specific aliases first
        for alias, table_name in self.table_aliases.items():
            if alias in query and table_name in schema:
                if table_name not in tables:
                    tables.append(table_name)
        
        # Check direct table names
        for table_name in schema.keys():
            if table_name.lower() in query:
                if table_name not in tables:
                    tables.append(table_name)
        
        # If no tables found, try to infer from context
        if not tables:
            if any(word in query for word in ['salary', 'payroll', 'employee', 'staff']):
                if 'mst_employee' in schema:
                    tables.append('mst_employee')
                if 'trn_payhead' in schema:
                    tables.append('trn_payhead')
            elif any(word in query for word in ['voucher', 'transaction', 'sales', 'purchase']):
                if 'trn_voucher' in schema:
                    tables.append('trn_voucher')
            elif any(word in query for word in ['ledger', 'account', 'customer', 'supplier']):
                if 'mst_ledger' in schema:
                    tables.append('mst_ledger')
            elif any(word in query for word in ['stock', 'item', 'inventory', 'product']):
                if 'mst_stock_item' in schema:
                    tables.append('mst_stock_item')
        
        return tables
    
    def _extract_columns(self, query: str, schema: Dict, tables: List[str]) -> List[str]:
        """Extract column names from query with Tally aliases"""
        columns = []
        
        # Check for "all" or "*" keywords
        if any(word in query for word in ['all', 'everything', 'all columns', '*', 'details']):
            return ['*']
        
        # Check for Tally-specific column aliases
        for alias, column_name in self.column_aliases.items():
            if alias in query:
                # Find which table contains this column
                for table in tables:
                    if table in schema:
                        for col_info in schema[table]['columns']:
                            if col_info['name'] == column_name:
                                if column_name not in columns:
                                    columns.append(column_name)
        
        # Check direct column names from identified tables
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    col_name = col_info['name'].lower()
                    if col_name in query:
                        if col_info['name'] not in columns:
                            columns.append(col_info['name'])
        
        # Context-based column inference
        if not columns:
            if any(word in query for word in ['summary', 'report']):
                # For summary queries, include key columns
                if 'mst_employee' in tables:
                    columns.extend(['name', 'designation', 'location'])
                elif 'mst_ledger' in tables:
                    columns.extend(['name', 'closing_balance'])
                elif 'mst_stock_item' in tables:
                    columns.extend(['name', 'closing_balance', 'closing_value'])
                elif 'trn_voucher' in tables:
                    columns.extend(['date', 'voucher_type', 'voucher_number', 'party_name'])
        
        # If still no columns found, default to all
        if not columns and tables:
            columns = ['*']
        
        return columns
    
    def _extract_conditions(self, query: str, schema: Dict, tables: List[str]) -> List[Dict]:
        """Extract WHERE conditions with Tally-specific patterns"""
        conditions = []
        
        # Standard condition extraction
        for operator, keywords in self.condition_keywords.items():
            for keyword in keywords:
                # More flexible pattern matching
                pattern = rf"(\b\w+\b)\s+{re.escape(keyword)}\s+(['\"]?)([^'\"]+?)\2(?:\s|$|,)"
                matches = re.finditer(pattern, query, re.IGNORECASE)
                
                for match in matches:
                    field = match.group(1)
                    value = match.group(3).strip()
                    
                    # Resolve field aliases
                    resolved_field = self._resolve_field_alias(field, tables, schema)
                    if resolved_field:
                        conditions.append({
                            'field': resolved_field,
                            'operator': operator,
                            'value': value,
                            'parameterized': True
                        })
        
        # Extract GST-specific conditions
        for pattern, condition in self.gst_patterns.items():
            if pattern in query:
                conditions.append({
                    'field': 'gstn',
                    'operator': 'raw_condition',
                    'value': condition,
                    'parameterized': False
                })
        
        # Extract voucher type conditions
        for voucher_type, condition in self.voucher_type_filters.items():
            if voucher_type in query:
                conditions.append({
                    'field': 'voucher_type',
                    'operator': '=',
                    'value': voucher_type.title(),
                    'parameterized': True
                })
        
        # Extract date conditions
        for pattern, date_type in self.date_patterns:
            matches = re.finditer(pattern, query, re.IGNORECASE)
            for match in matches:
                date_columns = self._find_date_columns(tables, schema)
                if date_columns:
                    conditions.append({
                        'field': date_columns[0],
                        'operator': 'date_condition',
                        'value': match.group(0),
                        'type': date_type,
                        'parameterized': True
                    })
        
        return conditions
    
    def _extract_tally_filters(self, query: str, tables: List[str]) -> List[Dict]:
        """Extract Tally-specific filters"""
        filters = []
        
        # Balance-based filters
        if 'positive balance' in query or 'credit balance' in query:
            filters.append({
                'field': 'closing_balance',
                'operator': '>',
                'value': '0',
                'parameterized': True
            })
        elif 'negative balance' in query or 'debit balance' in query:
            filters.append({
                'field': 'closing_balance',
                'operator': '<',
                'value': '0',
                'parameterized': True
            })
        
        # Stock-specific filters
        if 'out of stock' in query or 'zero stock' in query:
            filters.append({
                'field': 'closing_balance',
                'operator': '<=',
                'value': '0',
                'parameterized': True
            })
        elif 'low stock' in query:
            filters.append({
                'field': 'closing_balance',
                'operator': '<',
                'value': '10',
                'parameterized': True
            })
        
        # Employee status filters
        if 'active employee' in query or 'current employee' in query:
            filters.append({
                'field': 'date_of_release',
                'operator': 'IS NULL',
                'value': '',
                'parameterized': False
            })
        elif 'ex employee' in query or 'former employee' in query:
            filters.append({
                'field': 'date_of_release',
                'operator': 'IS NOT NULL',
                'value': '',
                'parameterized': False
            })
        
        return filters
    
    def _resolve_field_alias(self, field: str, tables: List[str], schema: Dict) -> Optional[str]:
        """Resolve field alias to actual column name"""
        # Check if it's a direct alias
        if field.lower() in self.column_aliases:
            column_name = self.column_aliases[field.lower()]
            # Verify the column exists in one of the tables
            for table in tables:
                if table in schema:
                    for col_info in schema[table]['columns']:
                        if col_info['name'] == column_name:
                            return column_name
        
        # Check direct column name
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    if col_info['name'].lower() == field.lower():
                        return col_info['name']
        
        return None
    
    def _extract_joins(self, query: str, tables: List[str], schema: Dict) -> List[Dict]:
        """Extract JOIN information for Tally tables"""
        joins = []
        
        if len(tables) > 1:
            # Common Tally joins
            tally_joins = {
                ('trn_voucher', 'trn_accounting'): 'trn_voucher.guid = trn_accounting.guid',
                ('trn_voucher', 'trn_inventory'): 'trn_voucher.guid = trn_inventory.guid',
                ('trn_voucher', 'trn_payhead'): 'trn_voucher.guid = trn_payhead.guid',
                ('trn_voucher', 'trn_attendance'): 'trn_voucher.guid = trn_attendance.guid',
                ('mst_employee', 'trn_payhead'): 'mst_employee.name = trn_payhead.employee_name',
                ('mst_employee', 'trn_attendance'): 'mst_employee.name = trn_attendance.employee_name',
                ('mst_ledger', 'trn_accounting'): 'mst_ledger.name = trn_accounting.ledger',
                ('mst_stock_item', 'trn_inventory'): 'mst_stock_item.name = trn_inventory.item'
            }
            
            # Find applicable joins
            for i, table1 in enumerate(tables):
                for table2 in tables[i+1:]:
                    join_key = (table1, table2)
                    reverse_key = (table2, table1)
                    
                    if join_key in tally_joins:
                        joins.append({
                            'type': 'INNER',
                            'table1': table1,
                            'table2': table2,
                            'on': tally_joins[join_key]
                        })
                    elif reverse_key in tally_joins:
                        joins.append({
                            'type': 'INNER',
                            'table1': table2,
                            'table2': table1,
                            'on': tally_joins[reverse_key]
                        })
        
        return joins
    
    def _extract_aggregations(self, query: str, columns: List[str]) -> List[Dict]:
        """Extract aggregation functions with Tally context"""
        aggregations = []
        
        for func, keywords in self.aggregation_keywords.items():
            for keyword in keywords:
                if keyword in query:
                    if func == 'count':
                        aggregations.append({
                            'function': 'COUNT',
                            'column': '*'
                        })
                    else:
                        # Look for amount/balance columns for aggregation
                        amount_columns = ['amount', 'closing_balance', 'closing_value', 'opening_balance']
                        for col in columns:
                            if col in amount_columns:
                                aggregations.append({
                                    'function': func.upper(),
                                    'column': col
                                })
                                break
                        else:
                            # Default to amount if no specific column found
                            aggregations.append({
                                'function': func.upper(),
                                'column': 'amount'
                            })
        
        return aggregations
    
    def _extract_group_by(self, query: str, columns: List[str]) -> List[str]:
        """Extract GROUP BY with Tally context"""
        group_by = []
        
        # Common Tally grouping patterns
        if 'by employee' in query or 'per employee' in query:
            group_by.append('employee_name')
        elif 'by ledger' in query or 'per ledger' in query:
            group_by.append('ledger')
        elif 'by item' in query or 'per item' in query:
            group_by.append('item')
        elif 'by voucher type' in query:
            group_by.append('voucher_type')
        elif 'by month' in query:
            group_by.append("strftime('%Y-%m', date)")
        elif 'by year' in query:
            group_by.append("strftime('%Y', date)")
        
        # Standard grouping keywords
        group_keywords = ['by', 'per', 'for each', 'group by']
        for keyword in group_keywords:
            if keyword in query:
                pattern = rf"{keyword}\s+(\w+)"
                match = re.search(pattern, query, re.IGNORECASE)
                if match:
                    field = match.group(1)
                    if field in columns or field == '*':
                        group_by.append(field)
        
        return group_by
    
    def _extract_order_by(self, query: str, columns: List[str]) -> List[Dict]:
        """Extract ORDER BY with Tally context"""
        order_by = []
        
        # Tally-specific ordering
        if 'highest amount' in query or 'top amount' in query:
            order_by.append({'column': 'amount', 'direction': 'DESC'})
        elif 'lowest amount' in query or 'bottom amount' in query:
            order_by.append({'column': 'amount', 'direction': 'ASC'})
        elif 'alphabetical' in query or 'by name' in query:
            order_by.append({'column': 'name', 'direction': 'ASC'})
        elif 'latest' in query or 'recent' in query:
            order_by.append({'column': 'date', 'direction': 'DESC'})
        elif 'oldest' in query:
            order_by.append({'column': 'date', 'direction': 'ASC'})
        
        return order_by
    
    def _extract_limit(self, query: str) -> Optional[int]:
        """Extract LIMIT value from query"""
        limit_patterns = [
            r'top (\d+)',
            r'first (\d+)',
            r'limit (\d+)',
            r'up to (\d+)',
            r'maximum (\d+)',
            r'(\d+) records?'
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                return int(match.group(1))
        
        return None
    
    def _find_date_columns(self, tables: List[str], schema: Dict) -> List[str]:
        """Find date/datetime columns in Tally tables"""
        date_columns = []
        date_types = ['date', 'datetime', 'timestamp']
        
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    if any(dt in col_info['type'].lower() for dt in date_types):
                        date_columns.append(col_info['name'])
        
        return date_columns
    
    def _parse_insert_query(self, query: str, schema: Dict, user_filters: Dict) -> ParsedQuery:
        """Parse INSERT query for Tally"""
        tables = self._extract_tables(query, schema)
        
        return ParsedQuery(
            action='INSERT',
            tables=tables[:1] if tables else [],
            columns=[],
            conditions=[],
            joins=[],
            aggregations=[],
            group_by=[],
            order_by=[],
            limit=None,
            user_filters=user_filters
        )
    
    def _parse_update_query(self, query: str, schema: Dict, user_filters: Dict) -> ParsedQuery:
        """Parse UPDATE query for Tally"""
        tables = self._extract_tables(query, schema)
        conditions = self._extract_conditions(query, schema, tables)
        
        return ParsedQuery(
            action='UPDATE',
            tables=tables[:1] if tables else [],
            columns=[],
            conditions=conditions,
            joins=[],
            aggregations=[],
            group_by=[],
            order_by=[],
            limit=None,
            user_filters=user_filters
        )
    
    def _parse_delete_query(self, query: str, schema: Dict, user_filters: Dict) -> ParsedQuery:
        """Parse DELETE query for Tally"""
        tables = self._extract_tables(query, schema)
        conditions = self._extract_conditions(query, schema, tables)
        
        return ParsedQuery(
            action='DELETE',
            tables=tables[:1] if tables else [],
            columns=[],
            conditions=conditions,
            joins=[],
            aggregations=[],
            group_by=[],
            order_by=[],
            limit=None,
            user_filters=user_filters
        )
