import sqlite3
import re
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime

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
