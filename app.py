# Importing Dependencies
import json
import pandas as pd
import streamlit as st

# Page Configuration
st.set_page_config(
    page_title="Alert Transactions - Agents",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Main Title
st.title("📊 Alert Transactions - Agents")
st.markdown("---")

# Data Source Selection
st.sidebar.markdown("### 📊 Data Source")
data_source = st.sidebar.radio(
    "Choose data source:",
    ["Upload CSV File", "Generate Sample Data"]
)

transaction_data = None

if data_source == "Upload CSV File":
    # File Upload Section
    st.sidebar.markdown("### 📁 Upload Historical Transactions")
    uploaded_file = st.sidebar.file_uploader(
        "Upload your data file",
        type=['csv'],
        help="Upload a CSV file of Historical Transactions",
    )
    
    if uploaded_file:
        # Preprocess uploaded data
        try:
            # Lazy import to avoid circular import
            transaction_data = pd.read_csv(uploaded_file)
            st.sidebar.success("File uploaded successfully!")
        except Exception as e:
            st.sidebar.error(f"Error reading file: {e}")
            transaction_data = None

else:  # Generate Sample Data
    st.sidebar.markdown("### 🎲 Generate Sample Data")
    
    # Sample data type selection
    sample_data_type = st.sidebar.selectbox(
        "Select data type:",
        ["Normal Data", "Velocity Data"]
    )

    # Generate data button
    if st.sidebar.button("🎲 Generate Sample Data", type="primary"):
        with st.spinner("Generating sample data..."):
            # Lazy import to avoid circular import
            from src.utils.data_generator import SampleTransactionsDataGenerator
            
            data_generator = SampleTransactionsDataGenerator()
            
            if sample_data_type == "Normal Data":
                transaction_data = data_generator.generate_data(num_users=1, transactions_per_user=1000)
            else:  # Velocity Data
                transaction_data = data_generator.generate_velocity_data(num_users=1, transactions_per_user=1000)
            
            # Store in session state
            st.session_state.transaction_data = transaction_data
            st.session_state.data_generation_timestamp = pd.Timestamp.now()
            st.sidebar.success(f"✅ {sample_data_type} generated successfully!")
            
            # Add download button for generated data
            csv = transaction_data.to_csv(index=False)
            st.sidebar.download_button(
                label="📥 Download Sample Data",
                data=csv,
                file_name=f"sample_{sample_data_type.lower().replace(' ', '_')}.csv",
                mime="text/csv"
            )
            
            # Refresh the pipeline when new data is generated
            st.rerun()
    
    # Use data from session state if available
    if 'transaction_data' in st.session_state:
        transaction_data = st.session_state.transaction_data
        st.sidebar.success(f"✅ Sample data available")
        
        # Add download button for session data
        csv = transaction_data.to_csv(index=False)
        st.sidebar.download_button(
            label="📥 Download Sample Data",
            data=csv,
            file_name=f"sample_data.csv",
            mime="text/csv"
        )

# Sidebar Navigation
st.sidebar.title("🔧 Navigation")
analysis_type = st.sidebar.selectbox(
    "Choose Analysis Type:",
    ["Velocity Analysis", "Time & Day Analysis"]
)

# Track configuration changes
def track_config_changes(config_key, config_value):
    """Track configuration changes and trigger refresh if needed"""
    if config_key not in st.session_state:
        st.session_state[config_key] = config_value
    elif st.session_state[config_key] != config_value:
        st.session_state[config_key] = config_value
        st.session_state.config_changed = True

def display_tool_results(tool_result):
    """Display tool results in a formatted way"""
    if not tool_result or not hasattr(tool_result, 'result'):
        st.error("No tool results to display")
        return
    
    data = tool_result.result
    
    # Display Analysis Metrics as Key Metrics
    if 'analysis_metrics' in data:
        st.subheader("📊 Analysis Metrics")
        metrics = data['analysis_metrics']
        
        # Determine analysis type based on available metrics
        is_velocity_analysis = 'velocity_violations_count' in metrics
        is_timeday_analysis = 'time_window' in metrics
        
        if is_velocity_analysis:
            # Velocity Analysis Metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Transactions", metrics.get('total_transactions_analyzed', 'N/A'))
            with col2:
                st.metric("Violations Count", metrics.get('velocity_violations_count', 'N/A'))
            with col3:
                st.metric("Max Severity", metrics.get('max_velocity_severity', 'N/A'))
            with col4:
                avg_gap = metrics.get('avg_gap_minutes', 0)
                st.metric("Avg Gap (min)", f"{avg_gap:.2f}" if isinstance(avg_gap, (int, float)) else "N/A")
            
            # Additional velocity metrics in a second row
            col5, col6, col7, col8 = st.columns(4)
            with col5:
                st.metric("Gap Violation", "Yes" if metrics.get('gap_violation', False) else "No")
            with col6:
                st.metric("Unusual Hours", "Yes" if metrics.get('unusual_hours_detected', False) else "No")
            with col7:
                st.metric("Recent Transactions", metrics.get('recent_transaction_count', 'N/A'))
            with col8:
                st.metric("Anomalies Count", metrics.get('multidimensional_anomalies_count', 'N/A'))
        
        elif is_timeday_analysis:
            # Time & Day Analysis Metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Transactions", metrics.get('total_transactions_analyzed', 'N/A'))
            with col2:
                st.metric("Time Window", metrics.get('time_window', 'N/A').title())
            with col3:
                st.metric("Day Type", metrics.get('day_type', 'N/A').title())
            with col4:
                st.metric("Transactions in Window", metrics.get('transactions_in_window', 'N/A'))
            
            # Additional time-day metrics in a second row
            col5, col6, col7, col8 = st.columns(4)
            with col5:
                window_avg = metrics.get('window_avg_amount', 0)
                st.metric("Window Avg Amount", f"{window_avg:,.2f}" if window_avg else "N/A")
            with col6:
                st.metric("Similar Amounts Found", metrics.get('similar_amounts_found', 'N/A'))
            with col7:
                variability = metrics.get('amount_variability_threshold', 0)
                st.metric("Variability Threshold", f"{variability:.1%}" if variability else "N/A")
            with col8:
                limit = metrics.get('absolute_amount_limit', 0)
                st.metric("Amount Limit", f"{limit:,.2f}" if limit else "N/A")
        
        else:
            # Generic metrics display for unknown analysis types
            st.write("**Analysis Metrics:**")
            for key, value in metrics.items():
                if isinstance(value, (int, float)):
                    if 'amount' in key.lower():
                        st.metric(key.replace('_', ' ').title(), f"{value:,.2f}")
                    elif 'threshold' in key.lower() and value < 1:
                        st.metric(key.replace('_', ' ').title(), f"{value:.1%}")
                    else:
                        st.metric(key.replace('_', ' ').title(), f"{value:,.2f}" if isinstance(value, float) else value)
                else:
                    st.metric(key.replace('_', ' ').title(), str(value))
    
    # Display Overall Assessment
    if 'overall_assessment' in data:
        st.subheader("🎯 Overall Assessment")
        assessment = data['overall_assessment']
        
        # Result with color coding
        result = assessment.get('result', 'Unknown')
        if 'Fraud' in result and 'High' in result:
            st.error(f"**Result:** {result}")
        elif 'Fraud' in result:
            st.warning(f"**Result:** {result}")
        else:
            st.success(f"**Result:** {result}")
        
        # Rationale
        if 'rationale' in assessment:
            st.markdown("**Rationale:**")
            if isinstance(assessment['rationale'], list):
                for i, reason in enumerate(assessment['rationale'], 1):
                    st.markdown(f"- {reason}")
            else:
                st.markdown(f"- {assessment['rationale']}")
    
    # Display Scenario Analysis as Table
    if 'scenario_analysis' in data:
        st.subheader("🔍 Scenario Analysis")
        scenarios_df = pd.DataFrame(data['scenario_analysis'])
        
        # Format the dataframe for better display
        if not scenarios_df.empty:
            # Create a copy for display purposes
            display_df = scenarios_df.copy()
            display_df.columns = [col.replace('_', ' ').title() for col in display_df.columns]
            
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True
            )
    
    # Raw Data in Expandable Section
    with st.expander("📋 Raw Tool Output", expanded=False):
        st.json(data)

def display_agent_results(agent_result):
    """Display agent results in a formatted way"""
    if not agent_result or not hasattr(agent_result, 'messages'):
        st.error("No agent results to display")
        return
    
    # Get the last message content
    try:
        data = agent_result.messages[-1].content.model_dump()
    except:
        st.error("Unable to parse agent results")
        return
    
    # Agent Header
    agent_name = data.get('agent_name', 'Unknown Agent')
    st.subheader(f"🤖 {agent_name}")
    
    # Key Metrics Row
    col1, col2, col3 = st.columns(3)
    with col1:
        confidence = data.get('confidence_score', 0)
        st.metric("Confidence Score", f"{confidence:.1%}")
    with col2:
        is_false_positive = data.get('alert_is_false_positive', None)
        if is_false_positive is not None:
            status = "False Positive" if is_false_positive else "Valid Alert"
            color = "🟢" if is_false_positive else "🔴"
            st.metric("Alert Status", f"{color} {status}")
    with col3:
        st.metric("Agent Type", agent_name.replace(' Agent', ''))
    
    # Findings Section
    if 'findings' in data:
        st.markdown("### 🔍 Key Findings")
        st.info(data['findings'])
    
    # Detailed Explanation
    if 'detailed_explanation' in data:
        st.markdown("### 📝 Detailed Analysis")
        st.markdown(data['detailed_explanation'])
    
    # Recommendations
    if 'recommendations' in data and data['recommendations']:
        st.markdown("### 💡 Recommendations")
        for i, rec in enumerate(data['recommendations'], 1):
            st.markdown(f"{i}. {rec}")
    
    # Raw Data in Expandable Section
    with st.expander("📋 Raw Agent Output", expanded=False):
        st.json(data)

async def velocity(sample_alert, transaction_data):
    st.header("🚀 Velocity Analysis")
    st.subheader("Configuration")
    
    # Velocity parameters
    avg_time_gap = st.number_input("Average Time Gap between Transactions (minutes)", min_value=0.0, value=2.0, step=0.5)
    
    # Track configuration changes
    track_config_changes("velocity_avg_time_gap", avg_time_gap)
    
    # Check if configuration changed and refresh agents
    if st.session_state.get("config_changed", False):
        st.session_state.config_changed = False
        # Clear any cached agents
        if "velocity_agent" in st.session_state:
            del st.session_state.velocity_agent
        if "velocity_tool" in st.session_state:
            del st.session_state.velocity_tool
    
    if st.button("Run Velocity Analysis", type="primary"):
        with st.spinner("Analyzing velocity patterns..."):
            # Lazy imports to avoid circular import
            from src.agents.velocity import VelocityAnalysisAgent
            from src.tools.transactions import VelocityTransactions
            
            # Instantiate fresh agents with current configuration
            agent = VelocityAnalysisAgent(sample_alert, transaction_data)
            tool = VelocityTransactions(transaction_data, avg_time_gap)
            
            # Store in session state for potential reuse
            st.session_state.velocity_agent = agent
            st.session_state.velocity_tool = tool
            
            # Process the analysis
            results = await agent.execute_task()
            
            # Display results using custom formatters
            st.markdown("---")
            st.subheader("🔧 Internal Analysis Results (Tool Output)")
            tool_result = await tool.execute(
                customer_id=sample_alert['customer_id'],
                transaction_timestamp=sample_alert['transaction_date']
            )
            display_tool_results(tool_result)
            
            st.markdown("---")
            display_agent_results(results)

async def time_day(sample_alert, transaction_data):
    st.header("🕒 Time & Day Analysis")
    st.subheader("Configuration")
    
    # Time & Day parameters
    lookback_days = st.number_input("Lookback Days", min_value=30, value=60, step=30, help="Number of past days to analyze")
    amount_variability_threshold = st.number_input("Amount Variability Threshold", min_value=0.0, value=0.3, step=0.1, help="Maximum allowed variation in transaction amounts")
    absolute_amount_limit = st.number_input("Absolute Amount Limit", min_value=0.0, value=10000.0, step=1000.0, help="Maximum allowed transaction amount")
    
    # Track configuration changes
    track_config_changes("timeday_lookback_days", lookback_days)
    track_config_changes("timeday_amount_variability_threshold", amount_variability_threshold)
    track_config_changes("timeday_absolute_amount_limit", absolute_amount_limit)
    
    # Check if configuration changed and refresh agents
    if st.session_state.get("config_changed", False):
        st.session_state.config_changed = False
        # Clear any cached agents
        if "timeday_agent" in st.session_state:
            del st.session_state.timeday_agent
        if "timeday_tool" in st.session_state:
            del st.session_state.timeday_tool
    
    if st.button("Run Time & Day Analysis", type="primary"):
        with st.spinner("Analyzing time and day patterns..."):
            # Lazy imports to avoid circular import
            from src.agents.time_day import TimeDayAnalysisAgent
            from src.tools.transactions import TimeDayTransactions
            
            # Instantiate fresh agents with current configuration
            agent = TimeDayAnalysisAgent(sample_alert, transaction_data)
            tool = TimeDayTransactions(transaction_data, lookback_days,
                                        amount_variability_threshold, absolute_amount_limit)
            
            # Store in session state for potential reuse
            st.session_state.timeday_agent = agent
            st.session_state.timeday_tool = tool
            
            # Process the analysis
            results = await agent.execute_task()
            
            # Display results using custom formatters
            st.markdown("---")
            st.subheader("🔧 Internal Analysis Results (Tool Output)")
            tool_result = await tool.execute(
                customer_id=sample_alert['customer_id'],
                transaction_timestamp=sample_alert['transaction_date'],
                transaction_amount=sample_alert['amount']
            )
            display_tool_results(tool_result)
            
            st.markdown("---")
            display_agent_results(results)

async def main():
    if transaction_data is not None:
        # Sample Alert Generation in Expander
        with st.expander("Generate Sample Alert"):
            customer_id = st.text_input("Enter Customer ID for Sample Alert", value="cust_1")
            transaction_amount = st.number_input("Enter Transaction Amount", min_value=0, value=1000, step=100)
            
            # Track alert configuration changes
            track_config_changes("alert_customer_id", customer_id)
            track_config_changes("alert_transaction_amount", transaction_amount)
            
            # Lazy import to avoid circular import
            from src.utils.data_generator import SampleTransactionsDataGenerator
            
            data_generator = SampleTransactionsDataGenerator()
            sample_alert = data_generator._generate_transaction(
                customer_id=customer_id,
                transaction_id=1,
                transaction_date=pd.Timestamp.now(),
                transaction_amount=transaction_amount
            )
            
            st.write("Sample Alert:")
            st.json(sample_alert)
        
        # Clear agents if data was regenerated
        if st.session_state.get("data_generation_timestamp") and \
           st.session_state.get("last_processed_timestamp") != st.session_state.get("data_generation_timestamp"):
            # Clear all cached agents when new data is generated
            for key in ["velocity_agent", "velocity_tool", "timeday_agent", "timeday_tool"]:
                if key in st.session_state:
                    del st.session_state[key]
            st.session_state.last_processed_timestamp = st.session_state.data_generation_timestamp
        
        if analysis_type == "Velocity Analysis":
            await velocity(sample_alert, transaction_data)
        elif analysis_type == "Time & Day Analysis":
            await time_day(sample_alert, transaction_data)
        else:
            st.warning("Please select a valid analysis type from the sidebar.")
    else:
        st.warning("Please upload a data file or generate sample data to proceed with the analysis.")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
