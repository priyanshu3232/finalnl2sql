import json
import os
import re
from typing import Dict, List, Optional
from datetime import datetime
from collections import defaultdict

class FeedbackManager:
    def __init__(self, feedback_file: str = "feedback_log.json"):
        self.feedback_file = feedback_file
        self.feedback_data = self._load_feedback()
        self.pattern_corrections = defaultdict(list)
        self.confidence_adjustments = {}
        
    def _load_feedback(self) -> Dict:
        """Load existing feedback from file"""
        if os.path.exists(self.feedback_file):
            try:
                with open(self.feedback_file, 'r') as f:
                    data = json.load(f)
                    # Ensure new fields exist for LLM integration
                    if 'llm_evaluations' not in data:
                        data['llm_evaluations'] = []
                    if 'ai_learning_patterns' not in data:
                        data['ai_learning_patterns'] = {}
                    if 'user_ai_feedback_correlation' not in data:
                        data['user_ai_feedback_correlation'] = []
                    return data
            except:
                return self._initialize_feedback_structure()
        return self._initialize_feedback_structure()
    
    def _initialize_feedback_structure(self) -> Dict:
        """Initialize feedback data structure with LLM support"""
        return {
            'sessions': [],
            'patterns': {},
            'corrections': {},
            'llm_evaluations': [],
            'ai_learning_patterns': {},
            'user_ai_feedback_correlation': []
        }
    
    def add_feedback(self, natural_query: str, sql_query: str, 
                    feedback_type: str, correction: Optional[str] = None,
                    ai_judgment: Optional[Dict] = None) -> None:
        """
        Add feedback for a query with optional AI judgment
        feedback_type: 'positive', 'negative', 'corrected'
        ai_judgment: LLM judge evaluation results
        """
        feedback_entry = {
            'timestamp': datetime.now().isoformat(),
            'natural_query': natural_query,
            'sql_query': sql_query,
            'feedback_type': feedback_type,
            'correction': correction,
            'ai_judgment': ai_judgment
        }
        
        self.feedback_data['sessions'].append(feedback_entry)
        
        # Update patterns based on feedback
        if feedback_type == 'positive':
            self._update_positive_pattern(natural_query, sql_query, ai_judgment)
        elif feedback_type == 'corrected' and correction:
            self._update_correction_pattern(natural_query, sql_query, correction, ai_judgment)
        elif feedback_type == 'negative':
            self._update_negative_pattern(natural_query, sql_query, ai_judgment)
        
        # Store AI judgment correlation
        if ai_judgment:
            self._store_ai_correlation(feedback_type, ai_judgment)
        
        # Save feedback
        self._save_feedback()
    
    def add_llm_evaluation(self, natural_query: str, sql_query: str, 
                          ai_judgment: Dict, execution_result: Optional[Dict] = None) -> None:
        """Store LLM evaluation separately for analysis"""
        llm_eval_entry = {
            'timestamp': datetime.now().isoformat(),
            'natural_query': natural_query,
            'sql_query': sql_query,
            'ai_judgment': ai_judgment,
            'execution_result': execution_result
        }
        
        self.feedback_data['llm_evaluations'].append(llm_eval_entry)
        
        # Extract AI learning patterns
        self._extract_ai_patterns(natural_query, ai_judgment)
        
        self._save_feedback()
    
    def _update_positive_pattern(self, natural_query: str, sql_query: str, 
                                ai_judgment: Optional[Dict] = None) -> None:
        """Update patterns for successful queries with AI insights"""
        # Extract key phrases from natural query
        key_phrases = self._extract_key_phrases(natural_query)
        
        for phrase in key_phrases:
            if phrase not in self.feedback_data['patterns']:
                self.feedback_data['patterns'][phrase] = {
                    'success_count': 0,
                    'fail_count': 0,
                    'common_sql_patterns': [],
                    'ai_success_scores': [],
                    'avg_ai_score': 0.0
                }
            
            pattern_data = self.feedback_data['patterns'][phrase]
            pattern_data['success_count'] += 1
            
            # Store SQL pattern
            sql_pattern = self._generalize_sql_pattern(sql_query)
            if sql_pattern not in pattern_data['common_sql_patterns']:
                pattern_data['common_sql_patterns'].append(sql_pattern)
            
            # Store AI judgment if available
            if ai_judgment and ai_judgment.get('success'):
                ai_score = ai_judgment.get('score', 0.5)
                pattern_data['ai_success_scores'].append(ai_score)
                
                # Update average AI score
                pattern_data['avg_ai_score'] = sum(pattern_data['ai_success_scores']) / len(pattern_data['ai_success_scores'])
    
    def _update_negative_pattern(self, natural_query: str, sql_query: str,
                                ai_judgment: Optional[Dict] = None) -> None:
        """Update patterns for negative feedback with AI insights"""
        key_phrases = self._extract_key_phrases(natural_query)
        
        for phrase in key_phrases:
            if phrase not in self.feedback_data['patterns']:
                self.feedback_data['patterns'][phrase] = {
                    'success_count': 0,
                    'fail_count': 0,
                    'common_sql_patterns': [],
                    'ai_success_scores': [],
                    'avg_ai_score': 0.0,
                    'failure_reasons': []
                }
            
            pattern_data = self.feedback_data['patterns'][phrase]
            pattern_data['fail_count'] += 1
            
            # Store AI judgment insights
            if ai_judgment and ai_judgment.get('success'):
                if 'failure_reasons' not in pattern_data:
                    pattern_data['failure_reasons'] = []
                
                # Extract failure reasons from AI feedback
                ai_feedback = ai_judgment.get('feedback', '')
                missing_elements = ai_judgment.get('missing_elements', [])
                security_issues = ai_judgment.get('security_issues', [])
                
                failure_reasons = []
                if missing_elements:
                    failure_reasons.extend([f"Missing: {elem}" for elem in missing_elements])
                if security_issues:
                    failure_reasons.extend([f"Security: {issue}" for issue in security_issues])
                if ai_feedback and len(ai_feedback) > 20:
                    failure_reasons.append(f"AI Feedback: {ai_feedback[:100]}...")
                
                pattern_data['failure_reasons'].extend(failure_reasons)
    
    def _update_correction_pattern(self, natural_query: str, 
                                  wrong_sql: str, correct_sql: str,
                                  ai_judgment: Optional[Dict] = None) -> None:
        """Update patterns for corrected queries with AI insights"""
        # Store correction mapping
        query_hash = hash(natural_query)
        
        if str(query_hash) not in self.feedback_data['corrections']:
            self.feedback_data['corrections'][str(query_hash)] = []
        
        correction_entry = {
            'wrong': wrong_sql,
            'correct': correct_sql,
            'timestamp': datetime.now().isoformat(),
            'ai_judgment': ai_judgment
        }
        
        self.feedback_data['corrections'][str(query_hash)].append(correction_entry)
        
        # Update fail count for patterns
        key_phrases = self._extract_key_phrases(natural_query)
        for phrase in key_phrases:
            if phrase in self.feedback_data['patterns']:
                self.feedback_data['patterns'][phrase]['fail_count'] += 1
    
    def _store_ai_correlation(self, user_feedback: str, ai_judgment: Dict) -> None:
        """Store correlation between user feedback and AI judgment"""
        if ai_judgment.get('success'):
            correlation_entry = {
                'timestamp': datetime.now().isoformat(),
                'user_feedback': user_feedback,
                'ai_score': ai_judgment.get('score', 0.5),
                'ai_correctness': ai_judgment.get('correctness', 0.5),
                'ai_completeness': ai_judgment.get('completeness', 0.5),
                'ai_security': ai_judgment.get('security', 0.5),
                'correlation_score': self._calculate_correlation_score(user_feedback, ai_judgment)
            }
            
            self.feedback_data['user_ai_feedback_correlation'].append(correlation_entry)
    
    def _calculate_correlation_score(self, user_feedback: str, ai_judgment: Dict) -> float:
        """Calculate correlation between user feedback and AI judgment"""
        ai_score = ai_judgment.get('score', 0.5)
        
        if user_feedback == 'positive':
            # High AI score should correlate with positive user feedback
            return ai_score
        elif user_feedback == 'negative':
            # Low AI score should correlate with negative user feedback
            return 1.0 - ai_score
        else:  # neutral
            # Neutral feedback should correlate with mid-range AI scores
            return 1.0 - abs(ai_score - 0.5) * 2
    
    def _extract_ai_patterns(self, natural_query: str, ai_judgment: Dict) -> None:
        """Extract patterns from AI judgments for learning"""
        if not ai_judgment.get('success'):
            return
        
        key_phrases = self._extract_key_phrases(natural_query)
        ai_score = ai_judgment.get('score', 0.5)
        
        for phrase in key_phrases:
            if phrase not in self.feedback_data['ai_learning_patterns']:
                self.feedback_data['ai_learning_patterns'][phrase] = {
                    'evaluation_count': 0,
                    'avg_ai_score': 0.0,
                    'score_history': [],
                    'common_suggestions': [],
                    'common_issues': []
                }
            
            pattern_data = self.feedback_data['ai_learning_patterns'][phrase]
            pattern_data['evaluation_count'] += 1
            pattern_data['score_history'].append(ai_score)
            
            # Update average
            pattern_data['avg_ai_score'] = sum(pattern_data['score_history']) / len(pattern_data['score_history'])
            
            # Store common suggestions and issues
            suggestions = ai_judgment.get('suggestions', [])
            missing_elements = ai_judgment.get('missing_elements', [])
            
            for suggestion in suggestions:
                if suggestion not in pattern_data['common_suggestions']:
                    pattern_data['common_suggestions'].append(suggestion)
            
            for issue in missing_elements:
                if issue not in pattern_data['common_issues']:
                    pattern_data['common_issues'].append(issue)
    
    def get_confidence_adjustment(self, natural_query: str) -> float:
        """Get confidence adjustment based on past feedback and AI patterns"""
        key_phrases = self._extract_key_phrases(natural_query)
        
        total_adjustment = 1.0
        ai_adjustment = 1.0
        
        # User feedback based adjustment
        for phrase in key_phrases:
            if phrase in self.feedback_data['patterns']:
                pattern = self.feedback_data['patterns'][phrase]
                success_rate = pattern['success_count'] / (pattern['success_count'] + pattern['fail_count'] + 1)
                
                # Adjust confidence based on success rate
                if success_rate > 0.8:
                    total_adjustment *= 1.1
                elif success_rate < 0.5:
                    total_adjustment *= 0.8
                
                # Factor in AI success scores
                if pattern.get('avg_ai_score', 0) > 0:
                    ai_factor = pattern['avg_ai_score']
                    total_adjustment *= (0.8 + 0.4 * ai_factor)  # Scale between 0.8 and 1.2
        
        # AI learning patterns based adjustment
        for phrase in key_phrases:
            if phrase in self.feedback_data['ai_learning_patterns']:
                ai_pattern = self.feedback_data['ai_learning_patterns'][phrase]
                avg_ai_score = ai_pattern.get('avg_ai_score', 0.5)
                
                if avg_ai_score > 0.8:
                    ai_adjustment *= 1.15
                elif avg_ai_score < 0.4:
                    ai_adjustment *= 0.7
        
        final_adjustment = (total_adjustment + ai_adjustment) / 2
        return min(max(final_adjustment, 0.3), 1.8)  # Clamp between 0.3 and 1.8
    
    def get_similar_corrections(self, natural_query: str) -> List[Dict]:
        """Get similar queries that were corrected with AI insights"""
        query_hash = hash(natural_query)
        
        if str(query_hash) in self.feedback_data['corrections']:
            return self.feedback_data['corrections'][str(query_hash)]
        
        # Look for similar queries
        similar_corrections = []
        key_phrases = set(self._extract_key_phrases(natural_query))
        
        for session in self.feedback_data['sessions']:
            if session['feedback_type'] == 'corrected' and session['correction']:
                session_phrases = set(self._extract_key_phrases(session['natural_query']))
                
                # Calculate similarity
                similarity = len(key_phrases.intersection(session_phrases)) / len(key_phrases.union(session_phrases))
                
                if similarity > 0.6:  # 60% similarity threshold
                    correction_info = {
                        'natural_query': session['natural_query'],
                        'original_sql': session['sql_query'],
                        'corrected_sql': session['correction'],
                        'similarity': similarity,
                        'ai_judgment': session.get('ai_judgment', {})
                    }
                    similar_corrections.append(correction_info)
        
        # Sort by similarity
        similar_corrections.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_corrections[:3]  # Return top 3
    
    def get_ai_insights_for_query(self, natural_query: str) -> Dict:
        """Get AI insights and predictions for a query based on learning data"""
        key_phrases = self._extract_key_phrases(natural_query)
        insights = {
            'predicted_issues': [],
            'suggested_improvements': [],
            'confidence_prediction': 0.5,
            'historical_performance': {},
            'ai_recommendations': []
        }
        
        total_evaluations = 0
        total_score = 0
        
        for phrase in key_phrases:
            # Check AI learning patterns
            if phrase in self.feedback_data['ai_learning_patterns']:
                ai_pattern = self.feedback_data['ai_learning_patterns'][phrase]
                
                total_evaluations += ai_pattern['evaluation_count']
                total_score += ai_pattern['avg_ai_score'] * ai_pattern['evaluation_count']
                
                # Add common issues and suggestions
                insights['predicted_issues'].extend(ai_pattern['common_issues'][:2])
                insights['suggested_improvements'].extend(ai_pattern['common_suggestions'][:2])
            
            # Check user feedback patterns
            if phrase in self.feedback_data['patterns']:
                pattern = self.feedback_data['patterns'][phrase]
                
                if pattern.get('failure_reasons'):
                    insights['predicted_issues'].extend(pattern['failure_reasons'][:2])
        
        # Calculate confidence prediction
        if total_evaluations > 0:
            insights['confidence_prediction'] = total_score / total_evaluations
        
        # Remove duplicates
        insights['predicted_issues'] = list(set(insights['predicted_issues']))
        insights['suggested_improvements'] = list(set(insights['suggested_improvements']))
        
        return insights
    
    def _extract_key_phrases(self, query: str) -> List[str]:
        """Extract key phrases from natural language query"""
        # Simple implementation - extract significant words and bigrams
        words = query.lower().split()
        
        # Remove common words
        stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'}
        words = [w for w in words if w not in stop_words]
        
        # Extract single words and bigrams
        phrases = words.copy()
        
        # Add bigrams
        for i in range(len(words) - 1):
            phrases.append(f"{words[i]} {words[i+1]}")
        
        return phrases
    
    def _generalize_sql_pattern(self, sql_query: str) -> str:
        """Generalize SQL query to extract pattern"""
        # Replace specific values with placeholders
        pattern = sql_query
        
        # Replace string literals
        pattern = re.sub(r"'[^']*'", "'<STRING>'", pattern)
        pattern = re.sub(r'"[^"]*"', '"<STRING>"', pattern)
        
        # Replace numbers
        pattern = re.sub(r'\b\d+\b', '<NUMBER>', pattern)
        
        # Replace table/column names with generic versions (simplified)
        pattern = pattern.lower()
        
        return pattern
    
    def _save_feedback(self) -> None:
        """Save feedback data to file"""
        try:
            with open(self.feedback_file, 'w') as f:
                json.dump(self.feedback_data, f, indent=2)
        except Exception as e:
            print(f"Error saving feedback: {e}")
    
    def get_statistics(self) -> Dict:
        """Get feedback statistics with AI metrics"""
        total_queries = len(self.feedback_data['sessions'])
        positive = sum(1 for s in self.feedback_data['sessions'] if s['feedback_type'] == 'positive')
        negative = sum(1 for s in self.feedback_data['sessions'] if s['feedback_type'] == 'negative')
        corrected = sum(1 for s in self.feedback_data['sessions'] if s['feedback_type'] == 'corrected')
        
        # AI evaluation statistics
        total_ai_evals = len(self.feedback_data['llm_evaluations'])
        ai_scores = [eval_data['ai_judgment'].get('score', 0.5) for eval_data in self.feedback_data['llm_evaluations'] 
                    if eval_data['ai_judgment'].get('success')]
        avg_ai_score = sum(ai_scores) / len(ai_scores) if ai_scores else 0
        
        # User-AI correlation analysis
        correlations = self.feedback_data['user_ai_feedback_correlation']
        avg_correlation = sum(c['correlation_score'] for c in correlations) / len(correlations) if correlations else 0
        
        return {
            'total_queries': total_queries,
            'positive_feedback': positive,
            'negative_feedback': negative,
            'corrections': corrected,
            'success_rate': (positive / total_queries * 100) if total_queries > 0 else 0,
            'pattern_count': len(self.feedback_data['patterns']),
            'correction_mappings': len(self.feedback_data['corrections']),
            'ai_evaluations': total_ai_evals,
            'avg_ai_score': avg_ai_score,
            'user_ai_correlation': avg_correlation,
            'ai_learning_patterns': len(self.feedback_data['ai_learning_patterns'])
        }
    
    def export_learning_data(self) -> Dict:
        """Export learning data for analysis with AI insights"""
        return {
            'statistics': self.get_statistics(),
            'top_patterns': self._get_top_patterns(),
            'common_corrections': self._get_common_corrections(),
            'learning_timeline': self._get_learning_timeline(),
            'ai_insights': self._get_ai_learning_insights(),
            'user_ai_correlation_analysis': self._analyze_user_ai_correlation()
        }
    
    def _get_top_patterns(self, limit: int = 10) -> List[Dict]:
        """Get most successful patterns with AI insights"""
        patterns_with_stats = []
        
        for phrase, data in self.feedback_data['patterns'].items():
            total = data['success_count'] + data['fail_count']
            if total > 0:
                pattern_info = {
                    'phrase': phrase,
                    'success_rate': data['success_count'] / total,
                    'total_uses': total,
                    'sql_patterns': data['common_sql_patterns'][:3],
                    'avg_ai_score': data.get('avg_ai_score', 0.0),
                    'ai_evaluation_count': len(data.get('ai_success_scores', []))
                }
                patterns_with_stats.append(pattern_info)
        
        # Sort by total uses and success rate
        patterns_with_stats.sort(key=lambda x: (x['total_uses'], x['success_rate']), reverse=True)
        return patterns_with_stats[:limit]
    
    def _get_common_corrections(self, limit: int = 5) -> List[Dict]:
        """Get most common corrections with AI analysis"""
        correction_counts = defaultdict(int)
        correction_details = {}
        
        for corrections in self.feedback_data['corrections'].values():
            for correction in corrections:
                key = (correction['wrong'], correction['correct'])
                correction_counts[key] += 1
                
                if key not in correction_details:
                    correction_details[key] = {
                        'ai_judgments': [],
                        'avg_ai_score_before': 0.0,
                        'common_ai_issues': []
                    }
                
                # Store AI judgment details
                if correction.get('ai_judgment'):
                    ai_judgment = correction['ai_judgment']
                    correction_details[key]['ai_judgments'].append(ai_judgment)
                    
                    # Collect common issues identified by AI
                    missing_elements = ai_judgment.get('missing_elements', [])
                    correction_details[key]['common_ai_issues'].extend(missing_elements)
        
        # Calculate averages and prepare results
        results = []
        sorted_corrections = sorted(correction_counts.items(), key=lambda x: x[1], reverse=True)
        
        for (wrong, correct), count in sorted_corrections[:limit]:
            details = correction_details[(wrong, correct)]
            ai_judgments = details['ai_judgments']
            
            avg_ai_score = 0.0
            if ai_judgments:
                scores = [aj.get('score', 0.5) for aj in ai_judgments if aj.get('success')]
                avg_ai_score = sum(scores) / len(scores) if scores else 0.0
            
            results.append({
                'wrong_pattern': wrong,
                'correct_pattern': correct,
                'frequency': count,
                'avg_ai_score_before': avg_ai_score,
                'common_ai_issues': list(set(details['common_ai_issues']))[:3]
            })
        
        return results
    
    def _get_learning_timeline(self) -> List[Dict]:
        """Get learning progress over time with AI metrics"""
        # Group by day
        daily_stats = defaultdict(lambda: {
            'positive': 0, 'negative': 0, 'corrected': 0, 
            'ai_evaluations': 0, 'ai_scores': []
        })
        
        # Process user feedback sessions
        for session in self.feedback_data['sessions']:
            date = session['timestamp'][:10]  # Extract date part
            daily_stats[date][session['feedback_type']] += 1
        
        # Process AI evaluations
        for eval_data in self.feedback_data['llm_evaluations']:
            date = eval_data['timestamp'][:10]
            daily_stats[date]['ai_evaluations'] += 1
            
            if eval_data['ai_judgment'].get('success'):
                score = eval_data['ai_judgment'].get('score', 0.5)
                daily_stats[date]['ai_scores'].append(score)
        
        # Calculate success rate per day
        timeline = []
        for date, stats in sorted(daily_stats.items()):
            user_total = stats['positive'] + stats['negative'] + stats['corrected']
            success_rate = (stats['positive'] / user_total * 100) if user_total > 0 else 0
            
            avg_ai_score = sum(stats['ai_scores']) / len(stats['ai_scores']) if stats['ai_scores'] else 0
            
            timeline.append({
                'date': date,
                'total_user_feedback': user_total,
                'success_rate': success_rate,
                'ai_evaluations': stats['ai_evaluations'],
                'avg_ai_score': avg_ai_score,
                **{k: v for k, v in stats.items() if k not in ['ai_scores']}
            })
        
        return timeline
    
    def _get_ai_learning_insights(self) -> Dict:
        """Get insights from AI learning patterns"""
        if not self.feedback_data['ai_learning_patterns']:
            return {'message': 'No AI learning data available'}
        
        # Find patterns with improving AI scores
        improving_patterns = []
        declining_patterns = []
        
        for phrase, data in self.feedback_data['ai_learning_patterns'].items():
            score_history = data.get('score_history', [])
            if len(score_history) >= 3:
                recent_avg = sum(score_history[-3:]) / 3
                older_avg = sum(score_history[:-3]) / len(score_history[:-3]) if len(score_history) > 3 else recent_avg
                
                improvement = recent_avg - older_avg
                
                if improvement > 0.1:
                    improving_patterns.append({
                        'phrase': phrase,
                        'improvement': improvement,
                        'recent_score': recent_avg,
                        'evaluation_count': data['evaluation_count']
                    })
                elif improvement < -0.1:
                    declining_patterns.append({
                        'phrase': phrase,
                        'decline': abs(improvement),
                        'recent_score': recent_avg,
                        'evaluation_count': data['evaluation_count']
                    })
        
        return {
            'improving_patterns': sorted(improving_patterns, key=lambda x: x['improvement'], reverse=True)[:5],
            'declining_patterns': sorted(declining_patterns, key=lambda x: x['decline'], reverse=True)[:5],
            'total_ai_patterns': len(self.feedback_data['ai_learning_patterns']),
            'avg_pattern_score': sum(p.get('avg_ai_score', 0) for p in self.feedback_data['ai_learning_patterns'].values()) / len(self.feedback_data['ai_learning_patterns'])
        }
    
    def _analyze_user_ai_correlation(self) -> Dict:
        """Analyze correlation between user feedback and AI judgments"""
        correlations = self.feedback_data['user_ai_feedback_correlation']
        
        if not correlations:
            return {'message': 'No correlation data available'}
        
        # Group by feedback type
        positive_correlations = [c for c in correlations if c['user_feedback'] == 'positive']
        negative_correlations = [c for c in correlations if c['user_feedback'] == 'negative']
        neutral_correlations = [c for c in correlations if c['user_feedback'] == 'neutral']
        
        def calc_avg_scores(corr_list):
            if not corr_list:
                return {}
            return {
                'avg_ai_score': sum(c['ai_score'] for c in corr_list) / len(corr_list),
                'avg_correlation': sum(c['correlation_score'] for c in corr_list) / len(corr_list),
                'count': len(corr_list)
            }
        
        return {
            'positive_feedback_analysis': calc_avg_scores(positive_correlations),
            'negative_feedback_analysis': calc_avg_scores(negative_correlations),
            'neutral_feedback_analysis': calc_avg_scores(neutral_correlations),
            'overall_correlation': sum(c['correlation_score'] for c in correlations) / len(correlations),
            'total_correlations': len(correlations)
        }
