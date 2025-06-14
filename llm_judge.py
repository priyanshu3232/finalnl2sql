import os
import json
import requests
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import re

class GroqLLMJudge:
    def __init__(self, api_key: Optional[str] = "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"):
        """
        Initialize Groq LLM Judge
        
        Args:
            api_key: Groq API key. If None, will look for GROQ_API_KEY environment variable
        """
        self.api_key = "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" or os.getenv('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')
        if not self.api_key:
            raise ValueError("Groq API key must be provided either as parameter or GROQ_API_KEY environment variable")
        
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
        self.model = "mixtral-8x7b-32768"  # Groq's Mixtral model
        self.max_tokens = 2048
        self.temperature = 0.1  # Low temperature for consistent judgment
        
        # Learning data storage
        self.learning_data_file = "llm_learning_data.json"
        self.load_learning_data()
    
    def load_learning_data(self):
        """Load existing learning data"""
        try:
            with open(self.learning_data_file, 'r') as f:
                self.learning_data = json.load(f)
        except FileNotFoundError:
            self.learning_data = {
                'evaluations': [],
                'improvements': [],
                'patterns': {},
                'success_metrics': {
                    'total_queries': 0,
                    'successful_queries': 0,
                    'llm_approved_queries': 0,
                    'user_positive_feedback': 0
                }
            }
    
    def save_learning_data(self):
        """Save learning data to file"""
        try:
            with open(self.learning_data_file, 'w') as f:
                json.dump(self.learning_data, f, indent=2)
        except Exception as e:
            print(f"Error saving learning data: {e}")
    
    def judge_query_quality(self, natural_query: str, generated_sql: str, 
                           schema_info: Dict, execution_result: Dict = None) -> Dict:
        """
        Use LLM to judge the quality of generated SQL query
        
        Returns:
            Dict with judgment results including score, feedback, and suggestions
        """
        
        # Prepare context for LLM
        context = self._prepare_judgment_context(natural_query, generated_sql, schema_info, execution_result)
        
        # Call Groq API
        try:
            judgment = self._call_groq_api(context)
            
            # Parse and structure the response
            parsed_judgment = self._parse_judgment_response(judgment)
            
            # Store evaluation for learning
            self._store_evaluation(natural_query, generated_sql, parsed_judgment)
            
            return parsed_judgment
            
        except Exception as e:
            return {
                'success': False,
                'error': f"LLM judgment failed: {str(e)}",
                'score': 0.5,  # Default neutral score
                'feedback': "Unable to get LLM judgment",
                'suggestions': []
            }
    
    def _prepare_judgment_context(self, natural_query: str, generated_sql: str, 
                                 schema_info: Dict, execution_result: Dict = None) -> str:
        """Prepare context prompt for LLM judgment"""
        
        # Get relevant schema information
        schema_summary = self._summarize_schema(schema_info)
        
        # Include execution results if available
        execution_info = ""
        if execution_result:
            if execution_result.get('success'):
                execution_info = f"\n\nExecution Result: SUCCESS - Returned {len(execution_result.get('data', []))} records"
            else:
                execution_info = f"\n\nExecution Result: FAILED - Error: {execution_result.get('error', 'Unknown error')}"
        
        context = f"""
You are an expert SQL judge evaluating the quality of automatically generated SQL queries for a Tally ERP system.

TASK: Evaluate how well the generated SQL query matches the natural language request.

NATURAL LANGUAGE QUERY:
{natural_query}

GENERATED SQL QUERY:
{generated_sql}

AVAILABLE SCHEMA SUMMARY:
{schema_summary}
{execution_info}

EVALUATION CRITERIA:
1. Correctness: Does the SQL correctly interpret the natural language intent?
2. Completeness: Does it include all necessary conditions and filters?
3. Security: Are there proper parameter bindings and user/company filters?
4. Efficiency: Is the query structure optimal?
5. Tally ERP Compliance: Does it follow Tally ERP conventions?

REQUIRED RESPONSE FORMAT (JSON):
{{
    "score": <float between 0.0 and 1.0>,
    "correctness": <float between 0.0 and 1.0>,
    "completeness": <float between 0.0 and 1.0>,
    "security": <float between 0.0 and 1.0>,
    "efficiency": <float between 0.0 and 1.0>,
    "tally_compliance": <float between 0.0 and 1.0>,
    "feedback": "<detailed explanation of strengths and weaknesses>",
    "suggestions": ["<list of specific improvement suggestions>"],
    "missing_elements": ["<list of missing query elements>"],
    "security_issues": ["<list of security concerns if any>"],
    "alternative_approach": "<suggest better SQL if current is poor>"
}}

Provide your evaluation:
"""
        return context
    
    def _summarize_schema(self, schema_info: Dict) -> str:
        """Create a concise schema summary for the LLM"""
        summary_parts = []
        
        for table_name, table_info in schema_info.items():
            if isinstance(table_info, dict) and 'columns' in table_info:
                key_columns = []
                for col in table_info['columns'][:5]:  # First 5 columns
                    key_columns.append(f"{col['name']} ({col['type']})")
                
                description = table_info.get('description', 'No description')
                summary_parts.append(f"Table {table_name}: {description}")
                summary_parts.append(f"  Key columns: {', '.join(key_columns)}")
                
                if len(table_info['columns']) > 5:
                    summary_parts.append(f"  ... and {len(table_info['columns'])-5} more columns")
        
        return '\n'.join(summary_parts)
    
    def _call_groq_api(self, context: str) -> str:
        """Make API call to Groq"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "You are an expert SQL query evaluator for Tally ERP systems. Always respond with valid JSON format."
                },
                {
                    "role": "user", 
                    "content": context
                }
            ],
            "max_tokens": self.max_tokens,
            "temperature": self.temperature
        }
        
        response = requests.post(self.base_url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        response_data = response.json()
        return response_data['choices'][0]['message']['content']
    
    def _parse_judgment_response(self, response: str) -> Dict:
        """Parse LLM response into structured judgment"""
        try:
            # Extract JSON from response
            json_match = re.search(r'\{.*\}', response, re.DOTALL)
            if json_match:
                judgment_data = json.loads(json_match.group())
            else:
                # Fallback parsing if no JSON found
                judgment_data = self._fallback_parse_response(response)
            
            # Validate and normalize the response
            judgment = {
                'success': True,
                'score': float(judgment_data.get('score', 0.5)),
                'correctness': float(judgment_data.get('correctness', 0.5)),
                'completeness': float(judgment_data.get('completeness', 0.5)),
                'security': float(judgment_data.get('security', 0.5)),
                'efficiency': float(judgment_data.get('efficiency', 0.5)),
                'tally_compliance': float(judgment_data.get('tally_compliance', 0.5)),
                'feedback': judgment_data.get('feedback', 'No feedback provided'),
                'suggestions': judgment_data.get('suggestions', []),
                'missing_elements': judgment_data.get('missing_elements', []),
                'security_issues': judgment_data.get('security_issues', []),
                'alternative_approach': judgment_data.get('alternative_approach', '')
            }
            
            return judgment
            
        except Exception as e:
            return {
                'success': False,
                'error': f"Failed to parse LLM response: {str(e)}",
                'score': 0.5,
                'feedback': f"Response parsing failed: {response[:200]}...",
                'suggestions': []
            }
    
    def _fallback_parse_response(self, response: str) -> Dict:
        """Fallback parser when JSON extraction fails"""
        # Simple fallback - extract key information using regex
        score_match = re.search(r'score["\s:]*([0-9.]+)', response, re.IGNORECASE)
        score = float(score_match.group(1)) if score_match else 0.5
        
        return {
            'score': score,
            'feedback': response[:500],  # First 500 chars as feedback
            'suggestions': [],
            'missing_elements': [],
            'security_issues': [],
            'alternative_approach': ''
        }
    
    def _store_evaluation(self, natural_query: str, sql_query: str, judgment: Dict):
        """Store evaluation for learning purposes"""
        evaluation_entry = {
            'timestamp': datetime.now().isoformat(),
            'natural_query': natural_query,
            'sql_query': sql_query,
            'judgment': judgment,
            'llm_model': self.model
        }
        
        self.learning_data['evaluations'].append(evaluation_entry)
        
        # Update metrics
        self.learning_data['success_metrics']['total_queries'] += 1
        if judgment.get('score', 0) > 0.7:
            self.learning_data['success_metrics']['llm_approved_queries'] += 1
        
        # Extract patterns for future improvement
        self._extract_patterns(natural_query, sql_query, judgment)
        
        # Save data
        self.save_learning_data()
    
    def _extract_patterns(self, natural_query: str, sql_query: str, judgment: Dict):
        """Extract patterns from evaluations for learning"""
        # Extract key phrases from natural query
        key_phrases = self._extract_key_phrases(natural_query)
        
        for phrase in key_phrases:
            if phrase not in self.learning_data['patterns']:
                self.learning_data['patterns'][phrase] = {
                    'count': 0,
                    'avg_score': 0,
                    'common_issues': [],
                    'successful_sql_patterns': []
                }
            
            pattern_data = self.learning_data['patterns'][phrase]
            pattern_data['count'] += 1
            
            # Update average score
            current_avg = pattern_data['avg_score']
            new_score = judgment.get('score', 0.5)
            pattern_data['avg_score'] = (current_avg * (pattern_data['count'] - 1) + new_score) / pattern_data['count']
            
            # Store common issues
            if judgment.get('missing_elements'):
                pattern_data['common_issues'].extend(judgment['missing_elements'])
            
            # Store successful patterns
            if new_score > 0.8:
                sql_pattern = self._generalize_sql_pattern(sql_query)
                if sql_pattern not in pattern_data['successful_sql_patterns']:
                    pattern_data['successful_sql_patterns'].append(sql_pattern)
    
    def _extract_key_phrases(self, query: str) -> List[str]:
        """Extract key phrases from natural language query"""
        # Simple implementation - can be enhanced
        words = query.lower().split()
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = [w for w in words if w not in stop_words]
        
        phrases = words.copy()
        
        # Add bigrams
        for i in range(len(words) - 1):
            phrases.append(f"{words[i]} {words[i+1]}")
        
        return phrases
    
    def _generalize_sql_pattern(self, sql_query: str) -> str:
        """Generalize SQL query to extract reusable pattern"""
        pattern = sql_query.lower()
        
        # Replace specific values with placeholders
        pattern = re.sub(r"'[^']*'", "'<STRING>'", pattern)
        pattern = re.sub(r'\b\d+\b', '<NUMBER>', pattern)
        
        return pattern
    
    def get_improvement_suggestions(self, natural_query: str) -> Dict:
        """Get improvement suggestions based on learning data"""
        key_phrases = self._extract_key_phrases(natural_query)
        suggestions = {
            'query_improvements': [],
            'common_patterns': [],
            'potential_issues': [],
            'confidence_adjustments': 1.0
        }
        
        for phrase in key_phrases:
            if phrase in self.learning_data['patterns']:
                pattern_data = self.learning_data['patterns'][phrase]
                
                # Adjust confidence based on historical performance
                if pattern_data['avg_score'] < 0.5:
                    suggestions['confidence_adjustments'] *= 0.8
                    suggestions['potential_issues'].extend(pattern_data['common_issues'][:3])
                elif pattern_data['avg_score'] > 0.8:
                    suggestions['confidence_adjustments'] *= 1.1
                    suggestions['common_patterns'].extend(pattern_data['successful_sql_patterns'][:2])
        
        return suggestions
    
    def provide_user_feedback(self, natural_query: str, sql_query: str, 
                             user_rating: str, user_comments: str = ""):
        """Record user feedback for continuous learning"""
        feedback_entry = {
            'timestamp': datetime.now().isoformat(),
            'natural_query': natural_query,
            'sql_query': sql_query,
            'user_rating': user_rating,  # 'positive', 'negative', 'neutral'
            'user_comments': user_comments
        }
        
        if 'user_feedback' not in self.learning_data:
            self.learning_data['user_feedback'] = []
        
        self.learning_data['user_feedback'].append(feedback_entry)
        
        # Update metrics
        if user_rating == 'positive':
            self.learning_data['success_metrics']['user_positive_feedback'] += 1
        
        # Store for future training
        self.save_learning_data()
    
    def generate_learning_report(self) -> Dict:
        """Generate comprehensive learning report"""
        total_evals = len(self.learning_data['evaluations'])
        
        if total_evals == 0:
            return {'message': 'No evaluations available yet'}
        
        # Calculate average scores
        scores = [eval_data['judgment'].get('score', 0) for eval_data in self.learning_data['evaluations']]
        avg_score = sum(scores) / len(scores) if scores else 0
        
        # Find most problematic patterns
        problematic_patterns = []
        for phrase, data in self.learning_data['patterns'].items():
            if data['avg_score'] < 0.6 and data['count'] > 1:
                problematic_patterns.append({
                    'phrase': phrase,
                    'avg_score': data['avg_score'],
                    'count': data['count'],
                    'issues': data['common_issues'][:3]
                })
        
        problematic_patterns.sort(key=lambda x: x['avg_score'])
        
        return {
            'total_evaluations': total_evals,
            'average_llm_score': avg_score,
            'success_rate': len([s for s in scores if s > 0.7]) / len(scores) if scores else 0,
            'top_problematic_patterns': problematic_patterns[:5],
            'metrics': self.learning_data['success_metrics'],
            'improvement_trends': self._calculate_improvement_trends()
        }
    
    def _calculate_improvement_trends(self) -> Dict:
        """Calculate improvement trends over time"""
        if len(self.learning_data['evaluations']) < 10:
            return {'message': 'Insufficient data for trend analysis'}
        
        recent_evals = self.learning_data['evaluations'][-10:]
        older_evals = self.learning_data['evaluations'][-20:-10] if len(self.learning_data['evaluations']) >= 20 else []
        
        recent_avg = sum(eval_data['judgment'].get('score', 0) for eval_data in recent_evals) / len(recent_evals)
        older_avg = sum(eval_data['judgment'].get('score', 0) for eval_data in older_evals) / len(older_evals) if older_evals else recent_avg
        
        return {
            'recent_average': recent_avg,
            'previous_average': older_avg,
            'improvement_rate': recent_avg - older_avg,
            'trend': 'improving' if recent_avg > older_avg else 'declining' if recent_avg < older_avg else 'stable'
        }
