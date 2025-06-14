from typing import Dict, List, Tuple, Any
from query_parser import ParsedQuery
import re
from datetime import datetime, timedelta

class SQLGenerator:
    def __init__(self):
        self.assumptions = []
        self.confidence = 0.0
        self.parameters = []
        
    def generate(self, parsed_query: ParsedQuery, schema: Dict) -> Dict:
        """Generate SQL query with parameters for Tally ERP"""
        self.assumptions = []
        self.confidence = 1.0
        self.parameters = []
        
        if parsed_query.action == 'SELECT':
            query = self._generate_select(parsed_query, schema)
        elif parsed_query.action == 'INSERT':
            query = self._generate_insert(parsed_query, schema)
        elif parsed_query.action == 'UPDATE':
            query = self._generate_update(parsed_query, schema)
        elif parsed_query.action == 'DELETE':
            query = self._generate_delete(parsed_query, schema)
        else:
            query = self._generate_select(parsed_query, schema)
        
        return {
            'query': query,
            'parameters': self.parameters,
            'assumptions': self.assumptions,
            'confidence': self.confidence,
            'parsed': parsed_query
        }
    
    def _generate_select(self, parsed: ParsedQuery, schema: Dict) -> str:
        """Generate SELECT query for Tally ERP"""
        query_parts = []
        
        # SELECT clause
        select_clause = self._build_select_clause(parsed)
        query_parts.append(select_clause)
        
        # FROM clause
        from_clause = self._build_from_clause(parsed)
        query_parts.append(from_clause)
        
        # JOIN clauses
        if parsed.joins:
            join_clauses = self._build_join_clauses(parsed)
            query_parts.extend(join_clauses)
        
        # WHERE clause (including mandatory user filters)
        where_clause = self._build_where_clause(parsed)
        query_parts.append(where_clause)
        
        # GROUP BY clause
        if parsed.group_by or parsed.aggregations:
            group_by_clause = self._build_group_by_clause(parsed)
            if group_by_clause:
                query_parts.append(group_by_clause)
        
        # ORDER BY clause
        if parsed.order_by:
            order_by_clause = self._build_order_by_clause(parsed)
            query_parts.append(order_by_clause)
        
        # LIMIT clause
        if parsed.limit:
            query_parts.append(f"LIMIT {parsed.limit}")
        
        return '\n'.join(query_parts)
    
    def _build_select_clause(self, parsed: ParsedQuery) -> str:
        """Build SELECT clause for Tally ERP"""
        if parsed.aggregations:
            select_items = []
            
            # Add aggregations
            for agg in parsed.aggregations:
                if agg['function'] == 'COUNT' and agg['column'] == '*':
                    select_items.append("COUNT(*)")
                else:
                    select_items.append(f"{agg['function']}({agg['column']})")
            
            # Add group by columns if any
            if parsed.group_by:
                for col in parsed.group_by:
                    if col not in [agg['column'] for agg in parsed.aggregations]:
                        select_items.append(col)
            
            return f"SELECT {', '.join(select_items)}"
        else:
            # Regular select
            if parsed.columns:
                if parsed.columns == ['*']:
                    return "SELECT *"
                else:
                    # Handle table prefixes for joins
                    if len(parsed.tables) > 1:
                        prefixed_columns = []
                        for col in parsed.columns:
                            if '.' not in col and col != '*':
                                # Try to determine which table the column belongs to
                                table_prefix = self._find_column_table(col, parsed.tables, schema)
                                if table_prefix:
                                    prefixed_columns.append(f"{table_prefix}.{col}")
                                else:
                                    prefixed_columns.append(col)
                            else:
                                prefixed_columns.append(col)
                        return f"SELECT {', '.join(prefixed_columns)}"
                    else:
                        return f"SELECT {', '.join(parsed.columns)}"
            else:
                self.assumptions.append("No specific columns mentioned, selecting all columns")
                self.confidence *= 0.8
                return "SELECT *"
    
    def _find_column_table(self, column: str, tables: List[str], schema: Dict) -> str:
        """Find which table contains a specific column"""
        for table in tables:
            if table in schema:
                for col_info in schema[table]['columns']:
                    if col_info['name'] == column:
                        return table
        return tables[0] if tables else ""  # Default to first table
    
    def _build_from_clause(self, parsed: ParsedQuery) -> str:
        """Build FROM clause"""
        if not parsed.tables:
            self.assumptions.append("No tables identified, query may fail")
            self.confidence *= 0.1
            return "FROM unknown_table"
        
        primary_table = parsed.tables[0]
        return f"FROM {primary_table}"
    
    def _build_join_clauses(self, parsed: ParsedQuery) -> List[str]:
        """Build JOIN clauses for Tally ERP"""
        join_clauses = []
        
        for join in parsed.joins:
            join_type = join.get('type', 'INNER')
            table2 = join['table2']
            on_condition = join['on']
            
            join_clause = f"{join_type} JOIN {table2} ON {on_condition}"
            join_clauses.append(join_clause)
            
            self.assumptions.append(f"Joined {join['table1']} with {table2} using {join_type} JOIN")
            self.confidence *= 0.9
        
        return join_clauses
    
    def _build_where_clause(self, parsed: ParsedQuery) -> str:
        """Build WHERE clause with mandatory Tally filters"""
        conditions = []
        
        # Mandatory user_id and company_name filters for Tally
        conditions.append("user_id = ?")
        self.parameters.append(parsed.user_filters['user_id'])
        
        conditions.append("company_name = ?")
        self.parameters.append(parsed.user_filters['company_name'])
        
        self.assumptions.append("Added mandatory user and company filters for data isolation")
        
        # Add parsed conditions
        for condition in parsed.conditions:
            field = condition['field']
            operator = condition['operator']
            value = condition['value']
            
            if operator == 'date_condition':
                date_condition = self._build_date_condition(condition)
                if date_condition:
                    conditions.append(date_condition)
            elif operator == 'raw_condition':
                # For GST and other raw conditions
                conditions.append(value)
            elif operator == 'IS NULL' or operator == 'IS NOT NULL':
                conditions.append(f"{field} {operator}")
            elif operator == 'LIKE':
                conditions.append(f"{field} LIKE ?")
                self.parameters.append(f"%{value}%")
                self.assumptions.append(f"Searching for partial match in {field}")
            elif operator == 'IN':
                values = [v.strip() for v in value.split(',')]
                placeholders = ', '.join(['?' for _ in values])
                conditions.append(f"{field} IN ({placeholders})")
                self.parameters.extend(values)
            elif operator == 'BETWEEN':
                if ' and ' in value.lower():
                    parts = value.lower().split(' and ')
                    conditions.append(f"{field} BETWEEN ? AND ?")
                    self.parameters.extend([parts[0].strip(), parts[1].strip()])
                else:
                    conditions.append(f"{field} {operator} ?")
                    self.parameters.append(value)
            else:
                conditions.append(f"{field} {operator} ?")
                self.parameters.append(value)
        
        return f"WHERE {' AND '.join(conditions)}"
    
    def _build_date_condition(self, condition: Dict) -> str:
        """Build date-based conditions for Tally ERP"""
        field = condition['field']
        value = condition['value']
        condition_type = condition.get('type', '')
        
        if condition_type == 'year':
            # Extract year from pattern like "in 2024"
            year_match = re.search(r'(\d{4})', value)
            if year_match:
                year = year_match.group(1)
                return f"strftime('%Y', {field}) = '{year}'"
        
        elif condition_type == 'month_year':
            # Extract month and year
            match = re.search(r'(january|february|march|april|may|june|july|august|september|october|november|december)\s+(\d{4})', value.lower())
            if match:
                month_name = match.group(1)
                year = match.group(2)
                month_num = {
                    'january': '01', 'february': '02', 'march': '03', 'april': '04',
                    'may': '05', 'june': '06', 'july': '07', 'august': '08',
                    'september': '09', 'october': '10', 'november': '11', 'december': '12'
                }.get(month_name, '01')
                return f"strftime('%Y-%m', {field}) = '{year}-{month_num}'"
        
        elif condition_type == 'relative_date':
            # Handle "in the last X days/months/years"
            match = re.search(r'(\d+)\s+(day|month|year)', value)
            if match:
                number = int(match.group(1))
                unit = match.group(2)
                
                self.assumptions.append(f"Interpreted '{value}' as date range from today")
                
                if unit == 'day':
                    return f"{field} >= date('now', '-{number} days')"
                elif unit == 'month':
                    return f"{field} >= date('now', '-{number} months')"
                elif unit == 'year':
                    return f"{field} >= date('now', '-{number} years')"
        
        elif condition_type == 'today':
            return f"date({field}) = date('now')"
        
        elif condition_type == 'current_period':
            if 'month' in value:
                return f"strftime('%Y-%m', {field}) = strftime('%Y-%m', 'now')"
            elif 'year' in value:
                return f"strftime('%Y', {field}) = strftime('%Y', 'now')"
        
        elif condition_type == 'last_period':
            if 'month' in value:
                return f"strftime('%Y-%m', {field}) = strftime('%Y-%m', date('now', '-1 month'))"
            elif 'year' in value:
                return f"strftime('%Y', {field}) = strftime('%Y', date('now', '-1 year'))"
        
        elif condition_type == 'since_date':
            self.parameters.append(value)
            return f"{field} >= ?"
        elif condition_type == 'before_date':
            self.parameters.append(value)
            return f"{field} < ?"
        elif condition_type == 'on_date':
            self.parameters.append(value)
            return f"date({field}) = ?"
        
        # Fallback
        self.parameters.append(value)
        return f"{field} = ?"
    
    def _build_group_by_clause(self, parsed: ParsedQuery) -> str:
        """Build GROUP BY clause for Tally ERP"""
        if parsed.group_by:
            return f"GROUP BY {', '.join(parsed.group_by)}"
        elif parsed.aggregations and parsed.columns:
            # If we have aggregations but no explicit group by,
            # group by non-aggregated columns
            non_agg_columns = []
            agg_columns = [agg['column'] for agg in parsed.aggregations]
            
            for col in parsed.columns:
                if col != '*' and col not in agg_columns:
                    # Don't group by expressions
                    if not ('(' in col and ')' in col):
                        non_agg_columns.append(col)
            
            if non_agg_columns:
                self.assumptions.append("Grouping by non-aggregated columns")
                self.confidence *= 0.9
                return f"GROUP BY {', '.join(non_agg_columns)}"
        return ""
    
    def _build_order_by_clause(self, parsed: ParsedQuery) -> str:
        """Build ORDER BY clause"""
        order_items = []
        
        for order in parsed.order_by:
            column = order['column']
            direction = order.get('direction', 'ASC')
            order_items.append(f"{column} {direction}")
        
        return f"ORDER BY {', '.join(order_items)}"
    
    def _generate_insert(self, parsed: ParsedQuery, schema: Dict) -> str:
        """Generate INSERT query for Tally ERP"""
        if not parsed.tables:
            self.confidence = 0.1
            return "-- Unable to generate INSERT query: no table specified"
        
        table = parsed.tables[0]
        self.assumptions.append("INSERT query requires values and mandatory user/company fields")
        self.confidence = 0.7
        
        if table in schema:
            # Get columns excluding auto-generated ones
            columns = []
            for col in schema[table]['columns']:
                col_name = col['name']
                # Skip GUID (primary key) but include user_id and company_name
                if col_name != 'guid' or col_name in ['user_id', 'company_name']:
                    columns.append(col_name)
            
            # Ensure user_id and company_name are included
            if 'user_id' not in columns:
                columns.append('user_id')
            if 'company_name' not in columns:
                columns.append('company_name')
            
            placeholders = ', '.join(['?' for _ in columns])
            return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
        
        return f"INSERT INTO {table} VALUES (?)"
    
    def _generate_update(self, parsed: ParsedQuery, schema: Dict) -> str:
        """Generate UPDATE query for Tally ERP"""
        if not parsed.tables:
            self.confidence = 0.1
            return "-- Unable to generate UPDATE query: no table specified"
        
        table = parsed.tables[0]
        self.assumptions.append("UPDATE query requires SET values and WHERE conditions")
        self.confidence = 0.7
        
        # Build SET clause (placeholder)
        set_clause = "SET column_name = ?"
        
        # Build WHERE clause with mandatory filters
        where_conditions = []
        
        # Add user filters first
        where_conditions.append("user_id = ?")
        self.parameters.append(parsed.user_filters['user_id'])
        where_conditions.append("company_name = ?")
        self.parameters.append(parsed.user_filters['company_name'])
        
        # Add parsed conditions
        if parsed.conditions:
            for condition in parsed.conditions:
                field = condition['field']
                operator = condition['operator']
                value = condition['value']
                
                if operator == 'date_condition':
                    date_condition = self._build_date_condition(condition)
                    if date_condition:
                        where_conditions.append(date_condition)
                elif operator == 'raw_condition':
                    where_conditions.append(value)
                elif operator in ['IS NULL', 'IS NOT NULL']:
                    where_conditions.append(f"{field} {operator}")
                else:
                    where_conditions.append(f"{field} {operator} ?")
                    self.parameters.append(value)
        else:
            self.assumptions.append("No specific WHERE conditions - will update all records for this user/company")
            self.confidence *= 0.5
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query_parts = [f"UPDATE {table}", set_clause, where_clause]
        return '\n'.join(query_parts)
    
    def _generate_delete(self, parsed: ParsedQuery, schema: Dict) -> str:
        """Generate DELETE query for Tally ERP"""
        if not parsed.tables:
            self.confidence = 0.1
            return "-- Unable to generate DELETE query: no table specified"
        
        table = parsed.tables[0]
        
        # Build WHERE clause with mandatory filters
        where_conditions = []
        
        # Add user filters first (mandatory for safety)
        where_conditions.append("user_id = ?")
        self.parameters.append(parsed.user_filters['user_id'])
        where_conditions.append("company_name = ?")
        self.parameters.append(parsed.user_filters['company_name'])
        
        # Add parsed conditions
        if parsed.conditions:
            for condition in parsed.conditions:
                field = condition['field']
                operator = condition['operator']
                value = condition['value']
                
                if operator == 'date_condition':
                    date_condition = self._build_date_condition(condition)
                    if date_condition:
                        where_conditions.append(date_condition)
                elif operator == 'raw_condition':
                    where_conditions.append(value)
                elif operator in ['IS NULL', 'IS NOT NULL']:
                    where_conditions.append(f"{field} {operator}")
                else:
                    where_conditions.append(f"{field} {operator} ?")
                    self.parameters.append(value)
        else:
            self.assumptions.append("No specific WHERE conditions - this would delete ALL records for this user/company")
            self.confidence *= 0.2
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}"
        
        query_parts = [f"DELETE FROM {table}", where_clause]
        return '\n'.join(query_parts)
    
    def generate_tally_report_query(self, report_type: str, parsed: ParsedQuery, schema: Dict) -> str:
        """Generate specialized Tally ERP reports"""
        
        if report_type == 'trial_balance':
            return self._generate_trial_balance(parsed)
        elif report_type == 'profit_loss':
            return self._generate_profit_loss(parsed)
        elif report_type == 'balance_sheet':
            return self._generate_balance_sheet(parsed)
        elif report_type == 'stock_summary':
            return self._generate_stock_summary(parsed)
        elif report_type == 'payroll_summary':
            return self._generate_payroll_summary(parsed)
        elif report_type == 'gst_report':
            return self._generate_gst_report(parsed)
        else:
            return self._generate_select(parsed, schema)
    
    def _generate_trial_balance(self, parsed: ParsedQuery) -> str:
        """Generate Trial Balance query"""
        self.assumptions.append("Generated comprehensive Trial Balance report")
        
        # Add mandatory parameters
        self.parameters.extend([parsed.user_filters['user_id'], parsed.user_filters['company_name']])
        
        return """
SELECT 
    l.name as ledger_name,
    l.opening_balance,
    COALESCE(SUM(CASE WHEN a.amount > 0 THEN a.amount ELSE 0 END), 0) as total_debit,
    COALESCE(SUM(CASE WHEN a.amount < 0 THEN ABS(a.amount) ELSE 0 END), 0) as total_credit,
    l.closing_balance
FROM mst_ledger l
LEFT JOIN trn_accounting a ON l.name = a.ledger 
    AND a.user_id = ? AND a.company_name = ?
WHERE l.user_id = ? AND l.company_name = ?
GROUP BY l.name, l.opening_balance, l.closing_balance
ORDER BY ABS(l.closing_balance) DESC
        """.strip()
    
    def _generate_stock_summary(self, parsed: ParsedQuery) -> str:
        """Generate Stock Summary query"""
        self.assumptions.append("Generated comprehensive Stock Summary report")
        
        # Add mandatory parameters
        self.parameters.extend([
            parsed.user_filters['user_id'], parsed.user_filters['company_name'],
            parsed.user_filters['user_id'], parsed.user_filters['company_name']
        ])
        
        return """
SELECT 
    s.name as item_name,
    s.opening_balance,
    s.opening_value,
    COALESCE(SUM(CASE WHEN i.quantity > 0 THEN i.quantity ELSE 0 END), 0) as total_inward,
    COALESCE(SUM(CASE WHEN i.quantity < 0 THEN ABS(i.quantity) ELSE 0 END), 0) as total_outward,
    s.closing_balance,
    s.closing_value,
    s.gst_rate,
    s.gst_hsn_code
FROM mst_stock_item s
LEFT JOIN trn_inventory i ON s.name = i.item 
    AND i.user_id = ? AND i.company_name = ?
WHERE s.user_id = ? AND s.company_name = ?
GROUP BY s.name, s.opening_balance, s.opening_value, s.closing_balance, s.closing_value, s.gst_rate, s.gst_hsn_code
ORDER BY s.closing_value DESC
        """.strip()
    
    def _generate_payroll_summary(self, parsed: ParsedQuery) -> str:
        """Generate Payroll Summary query"""
        self.assumptions.append("Generated Employee Payroll Summary report")
        
        # Add mandatory parameters  
        self.parameters.extend([
            parsed.user_filters['user_id'], parsed.user_filters['company_name'],
            parsed.user_filters['user_id'], parsed.user_filters['company_name'],
            parsed.user_filters['user_id'], parsed.user_filters['company_name']
        ])
        
        return """
SELECT 
    e.name as employee_name,
    e.designation,
    e.location,
    SUM(CASE WHEN p.payhead_name LIKE '%Basic%' THEN p.amount ELSE 0 END) as basic_pay,
    SUM(CASE WHEN p.payhead_name LIKE '%Allowance%' THEN p.amount ELSE 0 END) as allowances,
    SUM(CASE WHEN p.payhead_name LIKE '%Deduction%' THEN p.amount ELSE 0 END) as deductions,
    SUM(p.amount) as net_pay,
    COALESCE(SUM(a.time_value), 0) as total_hours
FROM mst_employee e
LEFT JOIN trn_payhead p ON e.name = p.employee_name 
    AND p.user_id = ? AND p.company_name = ?
LEFT JOIN trn_attendance a ON e.name = a.employee_name 
    AND a.user_id = ? AND a.company_name = ?
WHERE e.user_id = ? AND e.company_name = ?
GROUP BY e.name, e.designation, e.location
ORDER BY net_pay DESC
        """.strip()
        
    def _generate_gst_report(self, parsed: ParsedQuery) -> str:
        """Generate GST Report query"""
        self.assumptions.append("Generated GST Summary report")
        
        # Add mandatory parameters
        self.parameters.extend([
            parsed.user_filters['user_id'], parsed.user_filters['company_name'],
            parsed.user_filters['user_id'], parsed.user_filters['company_name']
        ])
        
        return """
SELECT 
    l.name as party_name,
    l.gstn,
    l.gst_registration_type,
    SUM(CASE WHEN a.amount > 0 THEN a.amount ELSE 0 END) as taxable_amount,
    SUM(CASE WHEN a.amount > 0 THEN a.amount * (l.tax_rate/100) ELSE 0 END) as gst_amount,
    l.tax_rate as gst_rate
FROM mst_ledger l
INNER JOIN trn_accounting a ON l.name = a.ledger 
    AND a.user_id = ? AND a.company_name = ?
WHERE l.user_id = ? AND l.company_name = ?
    AND l.gstn IS NOT NULL 
    AND l.gstn != ''
GROUP BY l.name, l.gstn, l.gst_registration_type, l.tax_rate
HAVING SUM(CASE WHEN a.amount > 0 THEN a.amount ELSE 0 END) > 0
ORDER BY SUM(CASE WHEN a.amount > 0 THEN a.amount ELSE 0 END) DESC
        """.strip()
