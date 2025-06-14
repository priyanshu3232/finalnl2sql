import sqlite3
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
# Add this debug function to your app.py after the imports

def debug_database_state(schema_manager, user_id, company_name):
    """Debug function to check database state"""
    conn = schema_manager.get_connection()
    if not conn:
        return "No database connection"
    
    debug_info = []
    cursor = conn.cursor()
    
    # Check if tables exist
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    debug_info.append(f"Tables in database: {[t[0] for t in tables]}")
    
    # Check data in key tables
    key_tables = ['mst_employee', 'mst_ledger', 'trn_voucher', 'mst_stock_item']
    
    for table in key_tables:
        try:
            # Check total records
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            
            # Check records for current user/company
            cursor.execute(f"""
                SELECT COUNT(*) FROM {table} 
                WHERE user_id = ? AND company_name = ?
            """, (user_id, company_name))
            user_records = cursor.fetchone()[0]
            
            debug_info.append(f"{table}: Total={total}, User/Company={user_records}")
            
            # Show sample data
            if user_records > 0:
                cursor.execute(f"""
                    SELECT * FROM {table} 
                    WHERE user_id = ? AND company_name = ? 
                    LIMIT 2
                """, (user_id, company_name))
                samples = cursor.fetchall()
                debug_info.append(f"  Sample data: {samples[0] if samples else 'None'}")
            
        except Exception as e:
            debug_info.append(f"{table}: Error - {str(e)}")
    
    cursor.close()
    return "\n".join(debug_info)

# Fix for the query executor parameter handling
def fix_query_executor_execute(self, sql_query: str, connection: Optional[sqlite3.Connection], 
            parameters: Optional[List[Any]] = None) -> Dict[str, Any]:
    """
    Fixed execute method with better parameter handling
    """
    # Validate query safety
    validation = self._validate_query(sql_query)
    if not validation['safe']:
        return {
            'success': False,
            'error': validation['reason'],
            'data': None
        }
    
    if not connection:
        return {
            'success': False,
            'error': 'No database connection available',
            'data': None
        }
    
    cursor = None
    try:
        cursor = connection.cursor()
        
        # Debug: Print query and parameters
        print(f"Executing SQL: {sql_query}")
        print(f"Parameters: {parameters}")
        
        # Execute with parameters if provided
        if parameters:
            # Ensure parameters is a list
            if not isinstance(parameters, list):
                parameters = [parameters]
            
            cursor.execute(sql_query, parameters)
        else:
            cursor.execute(sql_query)
        
        # Handle different query types
        if sql_query.strip().upper().startswith('SELECT'):
            columns = [description[0] for description in cursor.description]
            rows = cursor.fetchall()
            
            # Debug: Print result count
            print(f"Query returned {len(rows)} rows")
            
            data = []
            for row in rows:
                data.append(dict(zip(columns, row)))
            
            return {
                'success': True,
                'data': data,
                'error': None,
                'rows_affected': len(data)
            }
        
        else:
            connection.commit()
            return {
                'success': True,
                'data': None,
                'error': None,
                'rows_affected': cursor.rowcount
            }
            
    except Exception as e:
        print(f"Query execution error: {str(e)}")
        print(f"SQL: {sql_query}")
        print(f"Parameters: {parameters}")
        
        if connection:
            try:
                connection.rollback()
            except:
                pass
        
        return {
            'success': False,
            'error': f"Database error: {str(e)}",
            'data': None,
            'debug_info': {
                'sql': sql_query,
                'parameters': parameters,
                'error_type': type(e).__name__
            }
        }
    finally:
        if cursor:
            cursor.close()

# Fix for the SQL generator to ensure proper parameter order
def fix_build_where_clause(self, parsed: ParsedQuery) -> str:
    """Fixed WHERE clause builder with consistent parameter ordering"""
    conditions = []
    
    # First, find which tables have user_id and company_name columns
    # This is important for queries with JOINs
    tables_with_filters = []
    if hasattr(self, 'schema') and self.schema:
        for table in parsed.tables:
            if table in self.schema:
                columns = [col['name'] for col in self.schema[table]['columns']]
                if 'user_id' in columns and 'company_name' in columns:
                    tables_with_filters.append(table)
    else:
        # If no schema available, assume all tables have these columns
        tables_with_filters = parsed.tables[:1] if parsed.tables else []
    
    # Add mandatory filters for each relevant table
    for table in tables_with_filters:
        if len(parsed.tables) > 1:
            # For JOINs, specify table prefix
            conditions.append(f"{table}.user_id = ?")
            self.parameters.append(parsed.user_filters['user_id'])
            
            conditions.append(f"{table}.company_name = ?")
            self.parameters.append(parsed.user_filters['company_name'])
        else:
            # Single table query
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
        
        # Add table prefix if needed for joins
        if len(parsed.tables) > 1 and '.' not in field:
            # Try to find which table this field belongs to
            field_table = self._find_column_table(field, parsed.tables, self.schema if hasattr(self, 'schema') else {})
            if field_table:
                field = f"{field_table}.{field}"
        
        if operator == 'date_condition':
            date_condition = self._build_date_condition(condition)
            if date_condition:
                conditions.append(date_condition)
        elif operator == 'raw_condition':
            conditions.append(value)
        elif operator == 'IS NULL' or operator == 'IS NOT NULL':
            conditions.append(f"{field} {operator}")
        elif operator == 'LIKE':
            conditions.append(f"{field} LIKE ?")
            self.parameters.append(f"%{value}%")
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
    
    return f"WHERE {' AND '.join(conditions)}" if conditions else ""

# Add this function to test with a simple query
def test_simple_employee_query(schema_manager, user_id, company_name):
    """Test with a very simple query to verify data exists"""
    conn = schema_manager.get_connection()
    if not conn:
        return None
    
    try:
        cursor = conn.cursor()
        
        # First, check if ANY data exists
        cursor.execute("SELECT COUNT(*) FROM mst_employee")
        total_count = cursor.fetchone()[0]
        print(f"Total employees in database: {total_count}")
        
        # Then check for specific user/company
        cursor.execute("""
            SELECT name, designation, location, user_id, company_name
            FROM mst_employee
            WHERE user_id = ? AND company_name = ?
            LIMIT 5
        """, (user_id, company_name))
        
        results = cursor.fetchall()
        cursor.close()
        
        if results:
            print(f"Found {len(results)} employees for user: {user_id}, company: {company_name}")
            for row in results:
                print(f"  - {row}")
        else:
            print(f"No employees found for user: {user_id}, company: {company_name}")
            
            # Check what user_id and company_name values exist
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT user_id, company_name FROM mst_employee LIMIT 5")
            existing = cursor.fetchall()
            print("Existing user_id/company_name combinations:")
            for row in existing:
                print(f"  - User: {row[0]}, Company: {row[1]}")
            cursor.close()
        
        return results
        
    except Exception as e:
        print(f"Error in test query: {e}")
        return None
class QueryExecutor:
    def __init__(self):
        self.dangerous_keywords = [
            'DROP', 'TRUNCATE', 'EXEC', 'EXECUTE', 
            'SCRIPT', 'SHUTDOWN', 'GRANT', 'REVOKE'
        ]
        
    def execute(self, sql_query: str, connection: Optional[sqlite3.Connection], 
                parameters: Optional[List[Any]] = None) -> Dict[str, Any]:
        """
        Execute SQL query with proper parameterization
        """
        # Validate query safety
        validation = self._validate_query(sql_query)
        if not validation['safe']:
            return {
                'success': False,
                'error': validation['reason'],
                'data': None
            }
        
        if not connection:
            return {
                'success': False,
                'error': 'No database connection available',
                'data': None
            }
        
        try:
            cursor = connection.cursor()
            
            # Execute with parameters if provided
            if parameters:
                cursor.execute(sql_query, parameters)
            else:
                cursor.execute(sql_query)
            
            # Handle different query types
            if sql_query.strip().upper().startswith('SELECT'):
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))
                
                return {
                    'success': True,
                    'data': data,
                    'error': None,
                    'rows_affected': len(data)
                }
            
            else:
                connection.commit()
                return {
                    'success': True,
                    'data': None,
                    'error': None,
                    'rows_affected': cursor.rowcount
                }
                
        except sqlite3.Error as e:
            if connection:
                connection.rollback()
            
            return {
                'success': False,
                'error': f"Database error: {str(e)}",
                'data': None
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Unexpected error: {str(e)}",
                'data': None
            }
        finally:
            cursor.close()
    
    def _validate_query(self, sql_query: str) -> Dict[str, Any]:
        """Enhanced query validation"""
        query_upper = sql_query.upper()
        
        # Check for dangerous keywords
        for keyword in self.dangerous_keywords:
            if keyword in query_upper:
                return {
                    'safe': False,
                    'reason': f"Query contains potentially dangerous keyword: {keyword}"
                }
        
        # Check for multiple statements
        if self._has_multiple_statements(sql_query):
            return {
                'safe': False,
                'reason': "Multiple SQL statements detected. Only single statements are allowed."
            }
        
        # Check for comments
        if '--' in sql_query or '/*' in sql_query:
            return {
                'safe': False,
                'reason': "SQL comments detected. Comments are not allowed for security."
            }
        
        # Validate balanced quotes
        if not self._balanced_quotes(sql_query):
            return {
                'safe': False,
                'reason': "Unbalanced quotes detected. Possible SQL injection attempt."
            }
        
        # Check for suspicious patterns
        suspicious_patterns = [
            r'union\s+select',
            r';\s*drop',
            r';\s*delete',
            r';\s*update',
            r';\s*insert',
            r'1\s*=\s*1',
            r'\'.*or.*\'.*=.*\'',
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                return {
                    'safe': False,
                    'reason': f"Suspicious pattern detected: possible SQL injection attempt"
                }
        
        return {'safe': True, 'reason': None}
    
    def _has_multiple_statements(self, sql_query: str) -> bool:
        """Check if query contains multiple statements"""
        cleaned = self._remove_string_literals(sql_query)
        return ';' in cleaned and not cleaned.strip().endswith(';')
    
    def _balanced_quotes(self, sql_query: str) -> bool:
        """Check if quotes are balanced"""
        single_quotes = sql_query.count("'") - sql_query.count("\\'")
        double_quotes = sql_query.count('"') - sql_query.count('\\"')
        
        return single_quotes % 2 == 0 and double_quotes % 2 == 0
    
    def _remove_string_literals(self, sql_query: str) -> str:
        """Remove string literals for analysis"""
        result = re.sub(r"'[^']*'", "''", sql_query)
        result = re.sub(r'"[^"]*"', '""', result)
        return result
