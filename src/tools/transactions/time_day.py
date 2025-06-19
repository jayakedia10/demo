# Importing Dependencies
import json
import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, timedelta

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class TimeDayTransactions(BaseTool):
    """
    Time/Day of Week analysis tool with corrected logic.
    
    Purpose: Analyze if current transaction amount is consistent with historical 
    transaction patterns in the same time window (hour range + day type).
    
    Key Logic:
    1. Find historical transactions in same time window (e.g., weekday afternoons)
    2. If history exists: Calculate average amount and check similarity
    3. If no history: Use absolute threshold to classify high/low value
    4. Apply 4 fraud scenarios with corrected logic
    """

    def __init__(self, transaction_data: pd.DataFrame, lookback_days = None, amount_variability_threshold = None, absolute_amount_limit = None):
        super().__init__(
            name="Time and Day of Week Analysis Tool",
            description="Analyzes temporal transaction patterns and amount consistency",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Historical Transactions"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())
        
        # Load configuration
        with open('configs/sample_config.json', 'r') as f:
            config = json.load(f)
        
        time_day_config = config.get("thresholds", {}).get("time_day", {})
        self.lookback_days = lookback_days if lookback_days is not None else time_day_config.get("lookback_days", 60)
        # Determines what % difference constitutes "similar" amounts
        self.amount_variability_threshold = amount_variability_threshold if amount_variability_threshold is not None else time_day_config.get("amount_variability", 0.10)
        # Absolute threshold for when no history exists
        self.absolute_amount_limit = absolute_amount_limit if absolute_amount_limit is not None else time_day_config.get("absolute_amount_limit", 10000.0)

        # Time windows: split day into 4 periods
        self.time_windows = {
            'night': (0, 6),       # 12 AM - 6 AM
            'morning': (6, 12),    # 6 AM - 12 PM  
            'afternoon': (12, 18), # 12 PM - 6 PM
            'evening': (18, 24)    # 6 PM - 12 AM
        }

    async def initialize(self, **kwargs) -> bool:
        """Filter transactions for specific customer"""
        try:
            data = self.transaction_data
            self.user_transactions = data[
                (data['customer_id'] == kwargs.get('customer_id'))
            ].to_dict('records')
            self._is_initialized = True
            return True
        except Exception as e:
            self._logger.error(f"Failed to initialize: {e}")
            return False

    async def execute(self, customer_id: str, transaction_timestamp: str, transaction_amount: float) -> ToolResult:
        """Main execution method"""
        try:
            await self.initialize(customer_id=customer_id)
            alert_time = datetime.fromisoformat(str(transaction_timestamp))
            
            # Step 1: Get historical transactions within lookback period
            historical_transactions = self._get_historical_transactions(alert_time)
            
            # Step 2: Determine current transaction's time characteristics
            time_info = self._get_time_info(alert_time)
            
            # Step 3: Find transactions in same time window and analyze amounts
            window_analysis = self._analyze_time_window(historical_transactions, time_info, transaction_amount)
            
            # Step 4: Apply the 4 fraud detection scenarios with corrected logic
            scenario_results = self._apply_scenarios(window_analysis, transaction_amount)
            
            # Step 5: Generate final result
            result = self._generate_result(time_info, window_analysis, scenario_results, len(historical_transactions))
            
            return ToolResult(tool_name=self.name, success=True, result=result)
            
        except Exception as e:
            self._logger.error(f"Analysis failed: {str(e)}")
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={
                    "scenario_analysis": [],
                    "overall_assessment": {"result": "Error", "rationale": [f"Analysis failed: {str(e)}"]}
                },
                error=str(e)
            )

    def _get_historical_transactions(self, alert_time: datetime) -> List[Dict]:
        """Get customer's transactions within lookback period"""
        lookback_start = alert_time - timedelta(days=self.lookback_days)
        return [
            tx for tx in self.user_transactions 
            if lookback_start <= datetime.fromisoformat(str(tx['transaction_date'])) <= alert_time
        ]

    def _get_time_info(self, alert_time: datetime) -> Dict:
        """Extract time characteristics: hour, day type, time window with hours"""
        hour = alert_time.hour
        day_type = 'weekday' if alert_time.weekday() < 5 else 'weekend'
        
        # Determine which time window this hour falls into and format with hours
        time_window = None
        time_window_with_hours = None
        for window_name, (start, end) in self.time_windows.items():
            if start <= hour < end:
                time_window = window_name
                time_window_with_hours = f"{window_name} ({start:02d}:00-{end:02d}:00)"
                break
        
        return {
            'hour': hour,
            'time_window': time_window,
            'time_window_with_hours': time_window_with_hours,
            'day_type': day_type
        }

    def _analyze_time_window(self, transactions: List[Dict], time_info: Dict, current_amount: float) -> Dict:
        """
        Find transactions in same time window and analyze amount patterns
        
        Logic:
        1. Filter transactions by same time window + day type
        2. If history exists: Calculate average amount for this time window
        3. If no history: Will use absolute threshold for classification
        4. Count how many historical amounts are similar to current amount
        """
        window_name = time_info['time_window']
        day_type = time_info['day_type']
        window_start, window_end = self.time_windows[window_name]
        
        # Find matching transactions (same time window + day type)
        matching_transactions = []
        for tx in transactions:
            tx_time = datetime.fromisoformat(str(tx['transaction_date']))
            tx_hour = tx_time.hour
            tx_day_type = 'weekday' if tx_time.weekday() < 5 else 'weekend'
            
            if window_start <= tx_hour < window_end and tx_day_type == day_type:
                matching_transactions.append(tx)
        
        # If no historical transactions in this time window
        if not matching_transactions:
            return {
                'has_history': False,
                'transaction_count': 0,
                'window_avg_amount': 0.0,
                'similar_amount_count': 0,
                'amounts': []
            }
        
        # Calculate statistics for this time window
        amounts = [float(tx.get('amount', 0)) for tx in matching_transactions]
        window_avg_amount = sum(amounts) / len(amounts)
        
        # Count similar amounts (within variability threshold of current amount)
        similar_count = 0
        if current_amount > 0:
            for amount in amounts:
                # Calculate percentage difference
                percentage_diff = abs(amount - current_amount) / current_amount
                if percentage_diff <= self.amount_variability_threshold:
                    similar_count += 1
        
        return {
            'has_history': True,
            'transaction_count': len(matching_transactions),
            'window_avg_amount': window_avg_amount,
            'similar_amount_count': similar_count,
            'amounts': amounts
        }

    def _apply_scenarios(self, window_analysis: Dict, current_amount: float) -> List[Dict]:
        """
        Apply the 4 fraud detection scenarios with CORRECTED LOGIC
        
        Scenario Logic (FIXED):
        2.9:  No history + High amount (vs absolute threshold) = Probable Fraud (High)
        2.10: No history + Low amount (vs absolute threshold) = Probable Fraud (Less)  
        2.11: Has history + Similar amounts found = NOT FRAUD (normal behavior)
        2.12: Has history + No similar amounts + High amount (vs window avg) = Probable Fraud (High)
        """
        scenarios = []
        
        has_history = window_analysis['has_history']
        transaction_count = window_analysis['transaction_count']
        window_avg = window_analysis['window_avg_amount']
        similar_count = window_analysis['similar_amount_count']
        
        # FIXED LOGIC: Different classification logic based on whether history exists
        if has_history:
            # Use window average for high/low classification when history exists
            deviation = (current_amount - window_avg) / window_avg if window_avg > 0 else 0.0
            is_high_amount = current_amount > window_avg * (1 + self.amount_variability_threshold)
            is_low_amount = current_amount < window_avg * (1 - self.amount_variability_threshold)
        else:
            # Use absolute threshold when no history exists
            deviation = 0.0  # Can't calculate deviation without history
            is_high_amount = current_amount > self.absolute_amount_limit
            is_low_amount = current_amount < (self.absolute_amount_limit * 0.1)  # 10% of threshold
        
        # Scenario 2.9: No history + High amount (vs absolute threshold)
        triggered_2_9 = (not has_history and is_high_amount)
        scenarios.append({
            'scenario_id': '2.9',
            'triggered': triggered_2_9,
            'rationale': (
                f"No historical transactions in time window with high-value transaction: {current_amount:,.2f} (threshold: {self.absolute_amount_limit:,.2f})"
                if triggered_2_9 else
                f"Has {transaction_count} historical transactions or amount {current_amount:,.2f} not high vs threshold {self.absolute_amount_limit:,.2f}"
            )
        })
        
        # Scenario 2.10: No history + Low amount (vs absolute threshold)
        triggered_2_10 = (not has_history and is_low_amount)
        scenarios.append({
            'scenario_id': '2.10',
            'triggered': triggered_2_10,
            'rationale': (
                f"No historical transactions in time window with low-value transaction: {current_amount:,.2f} (low threshold: {self.absolute_amount_limit * 0.1:,.2f})"
                if triggered_2_10 else
                f"Has {transaction_count} historical transactions or amount {current_amount:,.2f} not low vs threshold {self.absolute_amount_limit * 0.1:,.2f}"
            )
        })
        
        # Scenario 2.11: Has history + Similar amounts found = NOT FRAUD (FIXED)
        triggered_2_11 = (has_history and similar_count > 0)
        scenarios.append({
            'scenario_id': '2.11',
            'triggered': triggered_2_11,
            'rationale': (
                f"{similar_count} similar amounts (±{self.amount_variability_threshold:.0%}) found in {transaction_count} historical transactions - normal behavior"
                if triggered_2_11 else
                f"No similar amounts found: 0 out of {transaction_count} historical transactions"
            )
        })
        
        # Scenario 2.12: Has history + No similar amounts + High amount (vs window average)
        triggered_2_12 = (has_history and similar_count == 0 and is_high_amount)
        scenarios.append({
            'scenario_id': '2.12',
            'triggered': triggered_2_12,
            'rationale': (
                f"High-value transaction {current_amount:,.2f} with no similar amounts in {transaction_count} historical transactions (window avg: {window_avg:,.2f})"
                if triggered_2_12 else
                f"Conditions not met: has_history={has_history}, similar_count={similar_count}, is_high_amount={is_high_amount}"
            )
        })
        
        return scenarios

    def _generate_result(self, time_info: Dict, window_analysis: Dict, scenario_results: List[Dict], total_transactions: int) -> Dict:
        """Generate final result with CORRECTED scenario mappings"""
        
        # Scenario configurations
        scenario_configs = {
            '2.9': {
                'description': 'No past transactions in time range with high-value current transaction',
                'fraud_result': 'Probable Fraud (High)',
                'normal_result': 'Not Fraud'
            },
            '2.10': {
                'description': 'No past transactions in time range with low-value current transaction',
                'fraud_result': 'Probable Fraud (Less)',
                'normal_result': 'Not Fraud'
            },
            '2.11': {
                'description': 'Past transactions with similar amounts found in time range',
                'fraud_result': 'Not Fraud',
                'normal_result': 'Probable Fraud (Less)'
            },
            '2.12': {
                'description': 'Only low-value historical transactions with high-value current transaction',
                'fraud_result': 'Probable Fraud (High)',
                'normal_result': 'Not Fraud'
            }
        }
        
        # Build scenario analysis
        scenario_analysis = []
        for result in scenario_results:
            scenario_id = result['scenario_id']
            config = scenario_configs[scenario_id]
            
            scenario_analysis.append({
                "scenario_id": scenario_id,
                "scenario_description": config['description'],
                "scenario_result": config['fraud_result'] if result['triggered'] else config['normal_result'],
                "rationale": result['rationale']
            })
        
        # FIXED: Determine overall assessment with corrected logic
        triggered_scenarios = [s for s in scenario_results if s['triggered']]
        
        # Check for "Not Fraud" scenarios first (2.11)
        not_fraud_scenarios = [s for s in triggered_scenarios if s['scenario_id'] == '2.11']
        if not_fraud_scenarios:
            overall_result = 'Not Fraud'
        # Then check for high risk scenarios
        elif any(s['scenario_id'] in ['2.9', '2.12'] for s in triggered_scenarios):
            overall_result = 'Probable Fraud (High)'
        # Then medium risk scenarios
        elif any(s['scenario_id'] == '2.10' for s in triggered_scenarios):
            overall_result = 'Probable Fraud (Less)'
        # Check for untriggered 2.11 (no similar amounts found)
        elif any(s['scenario_id'] == '2.11' and not s['triggered'] for s in scenario_results):
            overall_result = 'Probable Fraud (Less)'
        else:
            overall_result = 'Not Fraud'
        
        # Build rationale based on overall result
        if overall_result == 'Not Fraud':
            overall_rationale = [s['rationale'] for s in triggered_scenarios if s['scenario_id'] == '2.11']
        else:
            overall_rationale = [s['rationale'] for s in triggered_scenarios]
            # Add untriggered 2.11 rationale if relevant
            if not any(s['scenario_id'] == '2.11' for s in triggered_scenarios):
                scenario_2_11 = next(s for s in scenario_results if s['scenario_id'] == '2.11')
                if not scenario_2_11['triggered']:
                    overall_rationale.append(scenario_2_11['rationale'])
        
        return {
            "scenario_analysis": scenario_analysis,
            "overall_assessment": {
                "result": overall_result,
                "rationale": overall_rationale
            },
            "analysis_metrics": {
                "total_transactions_analyzed": total_transactions,
                "time_window": time_info['time_window_with_hours'],
                "day_type": time_info['day_type'],
                "transactions_in_window": window_analysis['transaction_count'],
                "window_avg_amount": window_analysis['window_avg_amount'],
                "similar_amounts_found": window_analysis['similar_amount_count'],
                "amount_variability_threshold": self.amount_variability_threshold,
                "absolute_amount_limit": self.absolute_amount_limit
            }
        }

    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs"""
        return all(field in kwargs for field in self.required_fields)

    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "transaction_timestamp": {"type": "string", "description": "Current transaction timestamp (ISO format)"},
            "transaction_amount": {"type": "number", "description": "Current transaction amount"}
        }

    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "scenario_analysis": {
                "type": "array",
                "description": "List of individual scenario analyses with their IDs, descriptions, results, and rationales.",
                "items": {
                    "type": "object",
                    "properties": {
                        "scenario_id": {
                            "type": "string",
                            "description": "Unique identifier for the scenario, e.g., '2.9', '2.10', '2.11', '2.12'."
                        },
                        "scenario_description": {
                            "type": "string",
                            "description": "Detailed description of the scenario being evaluated for fraud detection."
                        },
                        "scenario_result": {
                            "type": "string",
                            "description": "Outcome of the scenario evaluation: 'Probable Fraud (High)', 'Probable Fraud (Less)', or 'Not Fraud'."
                        },
                        "rationale": {
                            "type": "string",
                            "description": "Explanation or reasoning behind the scenario result with specific metrics and findings."
                        }
                    },
                    "required": ["scenario_id", "scenario_description", "scenario_result", "rationale"]
                }
            },
            "overall_assessment": {
                "type": "object",
                "description": "Overall assessment of the alert based on all scenario analyses.",
                "properties": {
                    "result": {
                        "type": "string",
                        "description": "Final overall result: 'Probable Fraud (High)', 'Probable Fraud (Less)', or 'Not Fraud'."
                    },
                    "rationale": {
                        "type": "array",
                        "description": "List of key rationales from triggered scenarios supporting the overall assessment.",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["result", "rationale"]
            },
            "analysis_metrics": {
                "type": "object",
                "description": "Numerical and factual metrics derived from the analysis to aid AI model decision-making.",
                "properties": {
                    "total_transactions_analyzed": {
                        "type": "integer",
                        "description": "Total number of historical transactions analyzed within the lookback period."
                    },
                    "time_window": {
                        "type": "string",
                        "description": "Time window during which the current transaction occurred with hours, e.g., 'afternoon (12:00-18:00)'."
                    },
                    "day_type": {
                        "type": "string",
                        "description": "Type of day when the transaction occurred: 'weekday' or 'weekend'."
                    },
                    "transactions_in_window": {
                        "type": "integer",
                        "description": "Number of historical transactions found in the same time window and day type as the current transaction."
                    },
                    "window_avg_amount": {
                        "type": "number",
                        "description": "Average transaction amount for historical transactions in the same time window and day type."
                    },
                    "similar_amounts_found": {
                        "type": "integer",
                        "description": "Count of historical transactions with amounts similar to current transaction within the configured variability threshold."
                    },
                    "amount_variability_threshold": {
                        "type": "number",
                        "description": "Configured threshold for amount variability used to determine similarity, expressed as a decimal (e.g., 0.10 for 10%)."
                    },
                    "absolute_amount_limit": {
                        "type": "number",
                        "description": "Absolute amount threshold used to classify high/low value transactions when no historical data exists in the time window."
                    }
                },
                "required": ["total_transactions_analyzed", "time_window", "day_type", "transactions_in_window", "window_avg_amount", "similar_amounts_found", "amount_variability_threshold", "absolute_amount_limit"]
            }
        }