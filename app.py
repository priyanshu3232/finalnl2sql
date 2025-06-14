import streamlit as st
import json
import sqlite3
import re
from datetime import datetime
import pandas as pd
from typing import Dict, List, Tuple, Optional
import os

# Import our custom modules
from query_parser import NaturalLanguageParser
from sql_generator import SQLGenerator
from query_executor import QueryExecutor
from feedback_manager import FeedbackManager
from schema_manager import SchemaManager

# Try to import LLM judge, but don't fail if it's not available
try:
    from llm_judge import GroqLLMJudge
    LLM_JUDGE_AVAILABLE = True
except ImportError as e:
    print(f"LLM Judge not available: {e}")
    LLM_JUDGE_AVAILABLE = False
    GroqLLMJudge = None

# Initialize session state
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'feedback_manager' not in st.session_state:
    st.session_state.feedback_manager = FeedbackManager()
if 'schema_manager' not in st.session_state:
    st.session_state.schema_manager = SchemaManager()
if 'parser' not in st.session_state:
    st.session_state.parser = NaturalLanguageParser()
if 'sql_generator' not in st.session_state:
    st.session_state.sql_generator = SQLGenerator()
if 'executor' not in st.session_state:
    st.session_state.executor = QueryExecutor()
if 'current_user' not in st.session_state:
    st.session_state.current_user = "demo_user"
if 'current_company' not in st.session_state:
    st.session_state.current_company = "Demo Company Ltd"

# Initialize LLM Judge (only if available and API key is provided)
if 'llm_judge' not in st.session_state:
    try:
        if LLM_JUDGE_AVAILABLE:
            # Try to get API key from various sources
            groq_api_key = (
                os.getenv('GROQ_API_KEY') or 
                st.secrets.get('GROQ_API_KEY', None) if hasattr(st, 'secrets') else None
            )
            
            if groq_api_key:
                st.session_state.llm_judge = GroqLLMJudge(api_key=groq_api_key)
                st.session_state.llm_enabled = True
            else:
                st.session_state.llm_judge = None
                st.session_state.llm_enabled = False
        else:
            st.session_state.llm_judge = None
            st.session_state.llm_enabled = False
    except Exception as e:
        st.session_state.llm_judge = None
        st.session_state.llm_enabled = False

# Page config
st.set_page_config(
    page_title="Tally ERP Text-to-SQL Agent" + (" with AI Judge" if st.session_state.get('llm_enabled', False) else ""),
    page_icon="💼",
    layout="wide"
)

# Title and description
title = "💼 Tally ERP Text-to-SQL Agent"
if st.session_state.get('llm_enabled', False):
    title += " with AI Judge"

st.title(title)
st.markdown("### Convert natural language queries to SQL for Tally ERP database" + 
           (" with LLM-powered quality assessment" if st.session_state.get('llm_enabled', False) else ""))

# Sidebar with configuration
with st.sidebar:
    st.header("🔧 Configuration")
    
    # API Key input (only show if LLM Judge is available)
    if LLM_JUDGE_AVAILABLE:
        api_key_input = st.text_input(
            "Groq API Key", 
            type="password",
            value="",
            help="Enter your Groq API key to enable LLM judge functionality"
        )
        
        if api_key_input and not st.session_state.llm_enabled:
            try:
                st.session_state.llm_judge = GroqLLMJudge(api_key=api_key_input)
                st.session_state.llm_enabled = True
                st.success("✅ LLM Judge enabled!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Failed to initialize LLM Judge: {str(e)}")
        
        # LLM Status
        if st.session_state.llm_enabled:
            st.success("🤖 AI Judge: ACTIVE")
            
            # Show LLM statistics
            if st.button("📊 View Learning Report"):
                if st.session_state.llm_judge:
                    try:
                        report = st.session_state.llm_judge.generate_learning_report()
                        with st.expander("📈 Learning Report", expanded=True):
                            if 'message' in report:
                                st.info(report['message'])
                            else:
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Evaluations", report.get('total_evaluations', 0))
                                    st.metric("Success Rate", f"{report.get('success_rate', 0):.1%}")
                                with col2:
                                    st.metric("Avg LLM Score", f"{report.get('average_llm_score', 0):.2f}")
                                    
                                trends = report.get('improvement_trends', {})
                                if 'trend' in trends:
                                    trend_emoji = "📈" if trends['trend'] == 'improving' else "📉" if trends['trend'] == 'declining' else "➡️"
                                    st.info(f"{trend_emoji} Trend: {trends['trend'].title()}")
                    except Exception as e:
                        st.error(f"Error generating learning report: {e}")
        else:
            st.warning("🤖 AI Judge: DISABLED")
            st.info("💡 Add your Groq API key above to enable AI-powered query evaluation")
    else:
        st.info("📦 LLM Judge module not available. Install required dependencies to enable AI features.")
    
    st.markdown("---")
    
    st.header("👤 User Context")
    
    # User selection
    st.session_state.current_user = st.text_input(
        "User ID", 
        value=st.session_state.current_user,
        help="User ID for data isolation"
    )
    
    st.session_state.current_company = st.text_input(
        "Company Name", 
        value=st.session_state.current_company,
        help="Company name for data filtering"
    )
    
    st.markdown("---")
    
    st.header("📊 Tally ERP Database")
    
    # Database status
    schema = st.session_state.schema_manager.get_schema()
    if schema:
        st.success("✅ Database Schema Loaded")
    else:
        st.warning("⚠️ Database Schema Not Loaded")
    
    # Load Tally schema
    if st.button("🔄 Load/Reload Tally ERP Schema"):
        with st.spinner("Loading Tally ERP database structure..."):
            try:
                st.session_state.schema_manager.load_tally_schema()
                st.success("✅ Tally ERP schema loaded successfully!")
                st.rerun()
            except Exception as e:
                st.error(f"❌ Error loading schema: {str(e)}")
    
    # Display current schema (only if loaded)
    schema = st.session_state.schema_manager.get_schema()
    if schema:
        # Show database statistics with error handling
        try:
            stats = st.session_state.schema_manager.get_table_statistics()
            
            st.subheader("📈 Database Statistics")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Tables", len(schema))
            with col2:
                total_records = sum(stat.get('row_count', 0) for stat in stats.values() if isinstance(stat, dict))
                st.metric("Total Records", total_records)
            
            # Schema browser
            st.subheader("🗂️ Schema Browser")
            
            # Group tables by type
            master_tables = {k: v for k, v in schema.items() if k.startswith('mst_')}
            transaction_tables = {k: v for k, v in schema.items() if k.startswith('trn_')}
            other_tables = {k: v for k, v in schema.items() if not k.startswith(('mst_', 'trn_'))}
            
            # Master Tables
            if master_tables:
                with st.expander("📋 Master Tables", expanded=False):
                    for table_name, table_info in master_tables.items():
                        st.markdown(f"**{table_name}**")
                        st.write(f"Description: {table_info.get('description', 'No description')}")
                        
                        # Safe access to stats
                        table_stats = stats.get(table_name, {})
                        row_count = table_stats.get('row_count', 0) if isinstance(table_stats, dict) else 0
                        st.write(f"Records: {row_count}")
                        
                        st.markdown("Key Columns:")
                        key_columns = [col for col in table_info['columns'][:5]]  # Show first 5
                        for col in key_columns:
                            st.text(f"• {col['name']} ({col['type']})")
                        
                        if len(table_info['columns']) > 5:
                            st.text(f"... and {len(table_info['columns'])-5} more columns")
                        st.markdown("---")
            
            # Transaction Tables
            if transaction_tables:
                with st.expander("💱 Transaction Tables", expanded=False):
                    for table_name, table_info in transaction_tables.items():
                        st.markdown(f"**{table_name}**")
                        st.write(f"Description: {table_info.get('description', 'No description')}")
                        
                        table_stats = stats.get(table_name, {})
                        row_count = table_stats.get('row_count', 0) if isinstance(table_stats, dict) else 0
                        st.write(f"Records: {row_count}")
                        
                        st.markdown("Key Columns:")
                        key_columns = [col for col in table_info['columns'][:5]]
                        for col in key_columns:
                            st.text(f"• {col['name']} ({col['type']})")
                        st.markdown("---")
            
            # Other Tables  
            if other_tables:
                with st.expander("⚙️ Configuration Tables", expanded=False):
                    for table_name, table_info in other_tables.items():
                        st.markdown(f"**{table_name}**")
                        
                        table_stats = stats.get(table_name, {})
                        row_count = table_stats.get('row_count', 0) if isinstance(table_stats, dict) else 0
                        st.write(f"Records: {row_count}")
                        
                        st.markdown("Key Columns:")
                        for col in table_info['columns'][:3]:
                            st.text(f"• {col['name']} ({col['type']})")
                        st.markdown("---")
                        
        except Exception as e:
            st.error(f"Error loading database statistics: {str(e)}")
            st.info("The database may not be properly initialized. Try reloading the schema.")

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    # Sample queries section
    st.header("🎯 Try These Sample Queries")
    
    if schema:
        try:
            sample_queries = st.session_state.schema_manager.get_sample_queries()
            
            # Create columns for sample query buttons
            query_cols = st.columns(3)
            for i, query in enumerate(sample_queries[:6]):  # Show first 6
                with query_cols[i % 3]:
                    if st.button(query, key=f"sample_{i}", help="Click to use this query"):
                        st.session_state.sample_query = query
        except Exception as e:
            st.warning(f"Error loading sample queries: {e}")
    else:
        st.info("📥 Load the database schema first to see sample queries")
    
    # Query input
    st.header("💬 Enter Your Query")
    
    # Use sample query if selected
    default_query = st.session_state.get('sample_query', '')
    if default_query:
        # Clear the sample query after using it
        del st.session_state.sample_query
    
    user_query = st.text_area(
        "Natural Language Query",
        value=default_query,
        placeholder="e.g., Show me all employees with their salary details for January 2024",
        height=100,
        help="Ask questions about employees, ledgers, stock items, vouchers, etc."
    )
    
    # Check if schema is loaded before allowing query processing
    if not schema:
        st.warning("⚠️ Please load the database schema first using the 'Load/Reload Tally ERP Schema' button in the sidebar.")
    else:
        # Query processing buttons
        if st.session_state.llm_enabled:
            col_btn1, col_btn2, col_btn3, col_btn4 = st.columns(4)
        else:
            col_btn1, col_btn2, col_btn3 = st.columns(3)
        
        with col_btn1:
            generate_clicked = st.button("🔮 Generate SQL", type="primary", disabled=not user_query.strip())
        
        with col_btn2:
            if st.button("📊 Generate Report", disabled=not user_query.strip()):
                if user_query:
                    try:
                        # Detect report type from query
                        query_lower = user_query.lower()
                        if 'trial balance' in query_lower:
                            report_type = 'trial_balance'
                        elif 'stock summary' in query_lower or 'inventory report' in query_lower:
                            report_type = 'stock_summary'
                        elif 'payroll' in query_lower or 'salary' in query_lower:
                            report_type = 'payroll_summary'
                        elif 'gst' in query_lower:
                            report_type = 'gst_report'
                        else:
                            report_type = 'custom'
                        
                        with st.spinner("Generating specialized report..."):
                            parsed_result = st.session_state.parser.parse(
                                user_query, 
                                st.session_state.schema_manager.get_schema(),
                                st.session_state.current_user,
                                st.session_state.current_company
                            )
                            
                            if report_type != 'custom':
                                sql_query = st.session_state.sql_generator.generate_tally_report_query(
                                    report_type, parsed_result, st.session_state.schema_manager.get_schema()
                                )
                                sql_result = {
                                    'query': sql_query,
                                    'parameters': st.session_state.sql_generator.parameters,
                                    'assumptions': st.session_state.sql_generator.assumptions,
                                    'confidence': 0.95
                                }
                            else:
                                sql_result = st.session_state.sql_generator.generate(
                                    parsed_result,
                                    st.session_state.schema_manager.get_schema()
                                )
                            
                            st.session_state.current_sql = sql_result
                            st.session_state.current_query = user_query
                            st.session_state.current_parsed = parsed_result
                            
                    except Exception as e:
                        st.error(f"Error generating report: {str(e)}")
        
        with col_btn3:
            if st.button("🧹 Clear"):
                # Clear current data
                for key in ['current_sql', 'current_query', 'current_judgment', 'execution_judgment']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        if st.session_state.llm_enabled:
            with col_btn4:
                if st.button("🤖 AI Judge", disabled=not hasattr(st.session_state, 'current_sql')):
                    if hasattr(st.session_state, 'current_sql') and hasattr(st.session_state, 'current_query'):
                        with st.spinner("AI Judge evaluating query..."):
                            try:
                                judgment = st.session_state.llm_judge.judge_query_quality(
                                    st.session_state.current_query,
                                    st.session_state.current_sql['query'],
                                    st.session_state.schema_manager.get_schema()
                                )
                                st.session_state.current_judgment = judgment
                            except Exception as e:
                                st.error(f"AI Judge evaluation failed: {str(e)}")
        
        # Process generate SQL button
        if generate_clicked and user_query:
            try:
                with st.spinner("Parsing your query..."):
                    # Parse the natural language query
                    parsed_result = st.session_state.parser.parse(
                        user_query, 
                        st.session_state.schema_manager.get_schema(),
                        st.session_state.current_user,
                        st.session_state.current_company
                    )
                    
                    # Generate SQL
                    sql_result = st.session_state.sql_generator.generate(
                        parsed_result,
                        st.session_state.schema_manager.get_schema()
                    )
                    
                    # Store in session state for confirmation
                    st.session_state.current_sql = sql_result
                    st.session_state.current_query = user_query
                    st.session_state.current_parsed = parsed_result
                    
                    # Auto-judge if LLM is enabled
                    if st.session_state.llm_enabled:
                        with st.spinner("AI Judge evaluating query..."):
                            try:
                                judgment = st.session_state.llm_judge.judge_query_quality(
                                    user_query,
                                    sql_result['query'],
                                    st.session_state.schema_manager.get_schema()
                                )
                                st.session_state.current_judgment = judgment
                            except Exception as e:
                                st.warning(f"AI Judge evaluation failed: {str(e)}")
            
            except Exception as e:
                st.error(f"Error processing query: {str(e)}")
                st.info("Please check your query and try again. Make sure the database schema is loaded.")
    
    # Display generated SQL and assumptions
    if hasattr(st.session_state, 'current_sql'):
        st.header("🔧 Generated SQL Query")
        
        # Show confidence and AI scores
        if st.session_state.llm_enabled:
            col_conf, col_ass, col_judge = st.columns(3)
        else:
            col_conf, col_ass = st.columns(2)
        
        with col_conf:
            confidence = st.session_state.current_sql.get('confidence', 0.0)
            st.metric("Confidence Score", f"{confidence:.0%}")
            
            # Color code based on confidence
            if confidence >= 0.8:
                st.success("High confidence")
            elif confidence >= 0.6:
                st.warning("Medium confidence") 
            else:
                st.error("Low confidence - please review carefully")
        
        with col_ass:
            assumptions = st.session_state.current_sql.get('assumptions', [])
            if assumptions:
                with st.expander("📋 Assumptions Made", expanded=True):
                    for i, assumption in enumerate(assumptions):
                        st.info(f"{i+1}. {assumption}")
            else:
                st.info("No specific assumptions were made.")
        
        if st.session_state.llm_enabled:
            with col_judge:
                # Display AI Judge results if available
                if hasattr(st.session_state, 'current_judgment') and st.session_state.current_judgment.get('success'):
                    judgment = st.session_state.current_judgment
                    ai_score = judgment.get('score', 0.0)
                    st.metric("AI Judge Score", f"{ai_score:.0%}")
                    
                    if ai_score >= 0.8:
                        st.success("🤖 AI Approved")
                    elif ai_score >= 0.6:
                        st.warning("🤖 AI Caution")
                    else:
                        st.error("🤖 AI Concern")
                    
                    # Show detailed judgment in expander
                    with st.expander("🤖 AI Judge Details", expanded=False):
                        st.write("**Feedback:**")
                        st.write(judgment.get('feedback', 'No feedback'))
                        
                        if judgment.get('suggestions'):
                            st.write("**Suggestions:**")
                            for i, suggestion in enumerate(judgment['suggestions']):
                                st.write(f"{i+1}. {suggestion}")
                        
                        if judgment.get('missing_elements'):
                            st.write("**Missing Elements:**")
                            for element in judgment['missing_elements']:
                                st.warning(f"⚠️ {element}")
                        
                        if judgment.get('security_issues'):
                            st.write("**Security Issues:**")
                            for issue in judgment['security_issues']:
                                st.error(f"🔒 {issue}")
                        
                        # Detailed scores
                        st.write("**Detailed Scores:**")
                        score_cols = st.columns(5)
                        with score_cols[0]:
                            st.metric("Correctness", f"{judgment.get('correctness', 0):.1f}")
                        with score_cols[1]:
                            st.metric("Completeness", f"{judgment.get('completeness', 0):.1f}")
                        with score_cols[2]:
                            st.metric("Security", f"{judgment.get('security', 0):.1f}")
                        with score_cols[3]:
                            st.metric("Efficiency", f"{judgment.get('efficiency', 0):.1f}")
                        with score_cols[4]:
                            st.metric("Tally Compliance", f"{judgment.get('tally_compliance', 0):.1f}")
        
        # SQL Query display with editing capability
        st.subheader("📝 SQL Query")
        edited_sql = st.text_area(
            "You can edit the SQL if needed:",
            value=st.session_state.current_sql['query'],
            height=200,
            help="Review and modify the generated SQL query before execution"
        )
        
        # Show parameters if any
        parameters = st.session_state.current_sql.get('parameters', [])
        if parameters:
            with st.expander("🔒 Query Parameters", expanded=False):
                st.write("The following parameters will be used for secure execution:")
                for i, param in enumerate(parameters):
                    st.code(f"Parameter {i+1}: {param}")
        
        # Execution buttons
        if st.session_state.llm_enabled:
            col1_1, col1_2, col1_3, col1_4 = st.columns(4)
        else:
            col1_1, col1_2, col1_3 = st.columns(3)
        
        with col1_1:
            if st.button("✅ Execute Query", type="primary"):
                try:
                    # Get database connection
                    connection = st.session_state.schema_manager.get_connection()
                    if not connection:
                        st.error("❌ Database connection not available. Please reload the schema.")
                    else:
                        # Execute the query with parameters
                        result = st.session_state.executor.execute(
                            edited_sql,
                            connection,
                            parameters
                        )
                        
                        # Get AI judgment on execution results if enabled
                        if st.session_state.llm_enabled and result['success']:
                            try:
                                post_execution_judgment = st.session_state.llm_judge.judge_query_quality(
                                    st.session_state.current_query,
                                    edited_sql,
                                    st.session_state.schema_manager.get_schema(),
                                    result
                                )
                                st.session_state.execution_judgment = post_execution_judgment
                            except Exception as e:
                                st.warning(f"Post-execution AI evaluation failed: {str(e)}")
                        
                        if result['success']:
                            st.success("Query executed successfully!")
                            
                            # Display results
                            if result['data']:
                                st.subheader("📊 Query Results")
                                df = pd.DataFrame(result['data'])
                                
                                # Format numeric columns
                                for col in df.columns:
                                    if df[col].dtype in ['float64', 'int64']:
                                        if 'amount' in col.lower() or 'balance' in col.lower() or 'value' in col.lower():
                                            df[col] = df[col].apply(lambda x: f"₹{x:,.2f}" if pd.notnull(x) else "")
                                
                                st.dataframe(df, use_container_width=True)
                                
                                # Show export options
                                col_exp1, col_exp2, col_exp3 = st.columns(3)
                                with col_exp1:
                                    csv = df.to_csv(index=False)
                                    st.download_button(
                                        label="📥 Download CSV",
                                        data=csv,
                                        file_name=f"tally_query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                                        mime="text/csv"
                                    )
                                
                                with col_exp2:
                                    st.info(f"Found {len(df)} records")
                                
                                with col_exp3:
                                    if hasattr(st.session_state, 'execution_judgment'):
                                        exec_judgment = st.session_state.execution_judgment
                                        exec_score = exec_judgment.get('score', 0.0)
                                        st.metric("Post-Exec AI Score", f"{exec_score:.0%}")
                                
                                # Show parameter info if any were used
                                if parameters:
                                    with st.expander("🔒 Security Info"):
                                        st.success(f"Query executed securely with {len(parameters)} parameters")
                                
                                # Log successful query
                                query_log = {
                                    'timestamp': datetime.now().isoformat(),
                                    'natural_query': st.session_state.current_query,
                                    'sql_query': edited_sql,
                                    'parameters': parameters,
                                    'success': True,
                                    'result_count': len(df),
                                    'feedback': 'positive'
                                }
                                
                                # Add AI judgments if available
                                if st.session_state.llm_enabled:
                                    query_log['ai_judgment'] = st.session_state.get('current_judgment', {})
                                    query_log['execution_judgment'] = st.session_state.get('execution_judgment', {})
                                
                                st.session_state.query_history.append(query_log)
                                
                                # Update feedback manager
                                st.session_state.feedback_manager.add_feedback(
                                    st.session_state.current_query,
                                    edited_sql,
                                    'positive',
                                    ai_judgment=st.session_state.get('current_judgment')
                                )
                                
                                # User feedback section for AI learning
                                if st.session_state.llm_enabled:
                                    st.subheader("📝 Help AI Learn")
                                    feedback_col1, feedback_col2 = st.columns(2)
                                    
                                    with feedback_col1:
                                        user_rating = st.selectbox(
                                            "Rate this query result:",
                                            ["", "positive", "neutral", "negative"],
                                            help="Your feedback helps improve the AI judge"
                                        )
                                    
                                    with feedback_col2:
                                        user_comments = st.text_input(
                                            "Comments (optional):",
                                            placeholder="Any specific feedback about the query or results"
                                        )
                                    
                                    if st.button("Submit Feedback") and user_rating:
                                        try:
                                            st.session_state.llm_judge.provide_user_feedback(
                                                st.session_state.current_query,
                                                edited_sql,
                                                user_rating,
                                                user_comments
                                            )
                                            st.success("Thank you! Your feedback helps improve the AI judge.")
                                        except Exception as e:
                                            st.error(f"Error submitting feedback: {e}")
                            else:
                                st.info("Query executed successfully but returned no results.")
                                st.info("💡 Try adjusting your search criteria or date ranges.")
                        else:
                            st.error(f"❌ Error: {result['error']}")
                            
                            # Log failed query
                            query_log = {
                                'timestamp': datetime.now().isoformat(),
                                'natural_query': st.session_state.current_query,
                                'sql_query': edited_sql,
                                'parameters': parameters,
                                'success': False,
                                'error': result['error']
                            }
                            
                            if st.session_state.llm_enabled:
                                query_log['ai_judgment'] = st.session_state.get('current_judgment', {})
                            
                            st.session_state.query_history.append(query_log)
                            
                except Exception as e:
                    st.error(f"❌ Execution error: {str(e)}")
        
        with col1_2:
            if st.button("🔄 Regenerate"):
                # Clear current SQL to regenerate
                for key in ['current_sql', 'current_judgment']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        with col1_3:
            if st.button("❌ Cancel"):
                # Clear current SQL
                for key in ['current_sql', 'current_judgment', 'execution_judgment']:
                    if key in st.session_state:
                        del st.session_state[key]
                st.rerun()
        
        if st.session_state.llm_enabled:
            with col1_4:
                if st.button("🔧 Improve Query"):
                    if hasattr(st.session_state, 'current_judgment'):
                        judgment = st.session_state.current_judgment
                        if judgment.get('alternative_approach'):
                            st.subheader("🔧 AI Suggested Improvement")
                            st.code(judgment['alternative_approach'], language='sql')
                            
                            if st.button("Use AI Suggestion"):
                                st.session_state.current_sql['query'] = judgment['alternative_approach']
                                st.rerun()
                        else:
                            st.info("No alternative approach suggested by AI judge.")

with col2:
    # Query history with AI insights
    st.header("📜 Query History")
    
    if st.session_state.query_history:
        # Show last 5 queries
        for i, query in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                st.text(f"⏰ {query['timestamp'][:19]}")
                st.text(f"💬 {query['natural_query'][:50]}...")
                
                if query['success']:
                    st.success(f"✅ Success - {query.get('result_count', 0)} records")
                    
                    # Show AI judgment if available
                    if query.get('ai_judgment') and query['ai_judgment'].get('success'):
                        ai_score = query['ai_judgment'].get('score', 0)
                        st.info(f"🤖 AI Score: {ai_score:.0%}")
                else:
                    st.error("❌ Failed")
                    if query.get('error'):
                        st.text(f"Error: {query['error'][:100]}...")
                
                # Show SQL in expandable section
                with st.expander("View SQL"):
                    st.code(query['sql_query'], language='sql')
                
                # Feedback buttons
                col_fb1, col_fb2 = st.columns(2)
                with col_fb1:
                    if st.button("👍", key=f"thumb_up_{i}"):
                        # Record positive feedback for AI learning
                        if st.session_state.llm_enabled:
                            try:
                                st.session_state.llm_judge.provide_user_feedback(
                                    query['natural_query'],
                                    query['sql_query'],
                                    'positive'
                                )
                            except Exception as e:
                                st.warning(f"Error recording feedback: {e}")
                        st.success("Thanks for the feedback!")
                with col_fb2:
                    if st.button("👎", key=f"thumb_down_{i}"):
                        # Record negative feedback for AI learning
                        if st.session_state.llm_enabled:
                            try:
                                st.session_state.llm_judge.provide_user_feedback(
                                    query['natural_query'],
                                    query['sql_query'],
                                    'negative'
                                )
                            except Exception as e:
                                st.warning(f"Error recording feedback: {e}")
                        st.info("Feedback noted for improvement!")
    else:
        st.info("No queries executed yet. Try the sample queries above!")
    
    # Quick actions
    st.markdown("---")
    st.header("⚡ Quick Actions")
    
    quick_queries = [
        "Show all employees",
        "Total sales this month", 
        "Stock items below 10 units",
        "GST registered customers",
        "Recent vouchers"
    ]
    
    for query in quick_queries:
        if st.button(query, key=f"quick_{query}"):
            st.session_state.sample_query = query
            st.rerun()

# Footer with enhanced statistics
st.markdown("---")
if st.session_state.llm_enabled:
    col_f1, col_f2, col_f3, col_f4, col_f5 = st.columns(5)
else:
    col_f1, col_f2, col_f3, col_f4 = st.columns(4)

with col_f1:
    total_queries = len(st.session_state.query_history)
    st.metric("Total Queries", total_queries)

with col_f2:
    successful_queries = sum(1 for q in st.session_state.query_history if q['success'])
    st.metric("Successful Queries", successful_queries)

with col_f3:
    if total_queries > 0:
        success_rate = (successful_queries / total_queries) * 100
        st.metric("Success Rate", f"{success_rate:.1f}%")
    else:
        st.metric("Success Rate", "N/A")

with col_f4:
    if st.session_state.query_history:
        total_records = sum(q.get('result_count', 0) for q in st.session_state.query_history if q['success'])
        st.metric("Records Fetched", total_records)
    else:
        st.metric("Records Fetched", 0)

if st.session_state.llm_enabled:
    with col_f5:
        # AI metrics
        if st.session_state.query_history:
            ai_evaluated = sum(1 for q in st.session_state.query_history if q.get('ai_judgment'))
            st.metric("AI Evaluated", ai_evaluated)
        else:
            st.metric("AI Evaluated", 0)

# Help section
with st.expander("❓ Help & Tips", expanded=False):
    help_content = """
    ### 🚀 How to use this Tally ERP Text-to-SQL Agent:
    
    **1. Setup:**
    - Click "Load/Reload Tally ERP Schema" in the sidebar to initialize the database
    - Add your Groq API key to enable AI Judge functionality (optional)
    
    **2. Query Generation:**
    - Use natural language: "Show me all employees with salary details"
    - The system generates SQL automatically
    - Review the generated query before execution
    
    **3. Sample Query Types:**
    - **Employee queries:** "Show employees in Mumbai", "Payroll summary for John"
    - **Ledger queries:** "Customers with outstanding balance", "GST registered parties"
    - **Stock queries:** "Items with low stock", "Inventory movements this month"
    - **Voucher queries:** "Recent sales transactions", "Purchase vouchers from Supplier B"
    - **Reports:** "Trial balance", "Stock summary", "Payroll report"
    
    **⚠️ Important Notes:**
    - All queries are automatically filtered by user and company for data security
    - The system uses parameterized queries to prevent SQL injection
    - Generated SQL can be edited before execution
    - Database must be loaded before generating queries
    """
    
    if st.session_state.llm_enabled:
        help_content += """
    
    **🤖 AI Judge Features:**
    - Real-time query quality evaluation
    - Detailed feedback on correctness, security, and efficiency
    - Improvement suggestions and alternative approaches
    - Learning from user feedback to improve over time
    
    **AI Judge Scoring:**
    - **0.8-1.0**: AI Approved (High quality)
    - **0.6-0.8**: AI Caution (Review recommended)
    - **0.0-0.6**: AI Concern (Needs improvement)
    """
    
    st.markdown(help_content)

# Developer info
st.markdown("---")
footer_text = "🔧 Tally ERP Text-to-SQL Agent"
if st.session_state.llm_enabled:
    footer_text += " with AI Judge | Built with Streamlit + Groq LLM"
else:
    footer_text += " | Built with Streamlit"
footer_text += " | Secure • Multi-tenant • Production-ready"

st.markdown(
    f"""
    <div style='text-align: center; color: #666; font-size: 12px;'>
    {footer_text}
    </div>
    """, 
    unsafe_allow_html=True
)
