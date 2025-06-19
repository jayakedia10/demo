# Importing Dependencies
import json
import pandas as pd
from collections import Counter
from typing import List, Dict, Any
from datetime import datetime, timedelta

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class RiskyMerchantTransactions(BaseTool):
    """
    Risky Merchant Analysis Tool implementing requirements 6.1, 6.2, 2.5, 2.6, 2.7, and 2.8.
    
    Purpose: Analyze transactions against risky merchant categories and IDs, 
    detect high-value transactions on same merchants, and perform amount grouping analysis.
    
    Key Logic:
    1. Check if current transaction MCC/MID matches risky lists
    2. Find historical transactions for same merchant
    3. Group historical transactions by amount and check for matches
    4. Apply risk assessment based on MCC risk level and amount patterns
    """
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Risky Merchant Analysis Tool",
            description="Analyzes transactions against risky merchant categories and performs amount grouping analysis",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Historical Transactions", "Risky MCC List", "Risky Merchants List"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())
        
        # Load configuration
        with open('configs/sample_config.json', 'r') as f:
            config = json.load(f)
        
        risky_merchant_config = config.get("thresholds", {}).get("risk_merchant", {})
        self.lookback_months = risky_merchant_config.get("lookback_months", 6)
        # Amount variability threshold for amount matching (similar to time_day tool)
        self.amount_variability_threshold = risky_merchant_config.get("amount_variability", 0.10)
        
        # Risky MCC and MID lists from config
        self.risky_mccs = risky_merchant_config.get("risky_mccs", [])
        self.risky_mids = risky_merchant_config.get("risky_mids", [])

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
            self._logger.error(f"Failed to initialize risky MCC/MID tool: {e}")
            return False

    async def execute(self, customer_id: str, 
                      merchant_id: str, 
                      mcc: str, 
                      transaction_timestamp: str,
                      transaction_amount: float) -> ToolResult:
        """Main execution method for risky MCC/MID analysis"""
        try:
            await self.initialize(customer_id=customer_id)
            alert_time = datetime.fromisoformat(str(transaction_timestamp))
            
            # Step 1: Get historical transactions within lookback period
            historical_transactions = self._get_historical_transactions(alert_time)
            
            # Step 2: Extract current transaction details
            current_mcc = mcc
            current_mid = merchant_id
            current_amount = transaction_amount

            # Step 3: Perform risky MCC/MID analysis
            risky_analysis = self._analyze_risky_mcc_mid(current_mcc, current_mid)
            
            # Step 4: Analyze same merchant transactions
            merchant_analysis = self._analyze_same_merchant_transactions(
                historical_transactions, current_mid, current_amount
            )
            
            # Step 5: Apply fraud detection scenarios
            scenario_results = self._apply_scenarios(
                risky_analysis, merchant_analysis, current_mcc, current_amount
            )
            
            # Step 6: Generate final result
            result = self._generate_result(
                risky_analysis, merchant_analysis, scenario_results, 
                len(historical_transactions)
            )
            
            return ToolResult(tool_name=self.name, success=True, result=result)
            
        except Exception as e:
            self._logger.error(f"Risky MCC/MID analysis failed: {str(e)}")
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
        """Get customer's transactions within lookback period (3-6 months)"""
        lookback_start = alert_time - timedelta(days=self.lookback_months * 30)
        return [
            tx for tx in self.user_transactions 
            if lookback_start <= datetime.fromisoformat(str(tx['transaction_date'])) <= alert_time
        ]

    def _analyze_risky_mcc_mid(self, current_mcc: str, current_mid: str) -> Dict:
        """
        Requirements 6.1 & 6.2: Check if current MCC/MID matches risky lists
        
        Logic:
        1. Check if current MCC is in risky MCC list
        2. Check if current MID is in risky MID list
        3. Return match status and details
        """
        is_risky_mcc = current_mcc in self.risky_mccs if current_mcc else False
        is_risky_mid = current_mid in self.risky_mids if current_mid else False
        
        return {
            'is_risky_mcc': is_risky_mcc,
            'is_risky_mid': is_risky_mid,
            'is_risky': is_risky_mcc or is_risky_mid,
            'current_mcc': current_mcc,
            'current_mid': current_mid,
            'risky_factor': 'MCC' if is_risky_mcc else 'MID' if is_risky_mid else None
        }

    def _analyze_same_merchant_transactions(self, transactions: List[Dict], 
                                          current_mid: str, current_amount: float) -> Dict:
        """
        Requirements 2.5, 2.6, 2.7, 2.8: Analyze same merchant transactions and amount grouping
        
        Logic:
        1. Filter transactions for same merchant
        2. Group transactions by amount (with variability tolerance)
        3. Check if current amount matches any historical amount group
        4. Determine amount pattern consistency
        """
        # Find transactions for same merchant
        same_merchant_transactions = [
            tx for tx in transactions 
            if tx.get('merchant_id') == current_mid
        ]
        
        if not same_merchant_transactions:
            return {
                'has_merchant_history': False,
                'transaction_count': 0,
                'amount_groups': {},
                'has_matching_amount': False,
                'similar_amount_count': 0
            }
        
        # Extract amounts and create groups
        merchant_amounts = [float(tx.get('amount', 0)) for tx in same_merchant_transactions]
        
        # Count similar amounts (within variability threshold of current amount)
        similar_amount_count = 0
        if current_amount > 0:
            for amount in merchant_amounts:
                percentage_diff = abs(amount - current_amount) / current_amount
                if percentage_diff <= self.amount_variability_threshold:
                    similar_amount_count += 1
        
        # Group amounts by exact values for additional analysis
        amount_groups = Counter(round(amount, 2) for amount in merchant_amounts)
        
        # Check for exact amount matches (within 1 cent tolerance)
        has_exact_match = any(
            abs(amount - current_amount) < 0.01 
            for amount in amount_groups.keys()
        )
        
        return {
            'has_merchant_history': True,
            'transaction_count': len(same_merchant_transactions),
            'amount_groups': dict(amount_groups),
            'has_matching_amount': similar_amount_count > 0 or has_exact_match,
            'similar_amount_count': similar_amount_count,
            'merchant_amounts': merchant_amounts
        }

    def _apply_scenarios(self, risky_analysis: Dict, merchant_analysis: Dict, 
                        current_mcc: str, current_amount: float) -> List[Dict]:
        """
        Apply risky MCC/MID fraud detection scenarios
        
        Scenario Logic:
        6.1: MCC/MID not in risky list = No Fraud
        6.2: MCC/MID in risky list = Probable Fraud
        2.5: No past transactions for same merchant = No Match Found
        2.6: Amount matches historical amounts = No Fraud (normal behavior)
        2.7: No amount match + MCC not high risk = Probable Fraud (Less)
        2.8: No amount match + MCC is high risk = Probable Fraud (High)
        """
        scenarios = []
        
        # Scenario 6.1 & 6.2: Risky MCC/MID Check
        triggered_6_1_6_2 = risky_analysis['is_risky']
        
        if triggered_6_1_6_2:
            risky_factor = risky_analysis['risky_factor']
            rationale_6 = [
                f"Transaction MCC/MID matches risky list",
                f"Risky {risky_factor}: {risky_analysis['current_mcc'] if risky_factor == 'MCC' else risky_analysis['current_mid']}"
            ]
        else:
            rationale_6 = [
                f"Transaction MCC/MID does not match risky list",
                f"Current MCC: {risky_analysis['current_mcc']}, Current MID: {risky_analysis['current_mid']}"
            ]
        
        scenarios.append({
            'scenario_id': '6.1/6.2',
            'triggered': triggered_6_1_6_2,
            'rationale': rationale_6
        })
        
        # Scenario 2.5: Same Merchant Transaction Check
        has_merchant_history = merchant_analysis['has_merchant_history']
        
        if has_merchant_history:
            rationale_2_5 = [
                f"Past transactions found for same merchant",
                f"{merchant_analysis['transaction_count']} historical transactions for merchant {risky_analysis['current_mid']}"
            ]
        else:
            rationale_2_5 = [
                f"No past transactions found for same merchant",
                f"Merchant {risky_analysis['current_mid']} has no transaction history"
            ]
        
        scenarios.append({
            'scenario_id': '2.5',
            'triggered': has_merchant_history,
            'rationale': rationale_2_5
        })
        
        # Scenario 2.6: Amount Matching Analysis
        triggered_2_6 = has_merchant_history and merchant_analysis['has_matching_amount']
        
        if triggered_2_6:
            rationale_2_6 = [
                f"Current amount matches historical amounts for same merchant",
                f"{merchant_analysis['similar_amount_count']} similar amounts found (±{self.amount_variability_threshold:.0%}) for amount {current_amount}"
            ]
        else:
            if has_merchant_history:
                rationale_2_6 = [
                    f"Current amount does not match historical amounts for same merchant",
                    f"No similar amounts found (±{self.amount_variability_threshold:.0%}) for amount {current_amount} in {merchant_analysis['transaction_count']} historical transactions"
                ]
            else:
                rationale_2_6 = [
                    f"Cannot check amount matching - no merchant history",
                    f"No historical transactions available for comparison"
                ]
        
        scenarios.append({
            'scenario_id': '2.6',
            'triggered': triggered_2_6,
            'rationale': rationale_2_6
        })
        
        # Scenario 2.7 & 2.8: Amount Mismatch + MCC Risk Analysis
        triggered_2_7_2_8 = has_merchant_history and not merchant_analysis['has_matching_amount']
        is_high_risk_mcc = risky_analysis['is_risky_mcc']
        
        if triggered_2_7_2_8:
            if is_high_risk_mcc:
                rationale_2_7_2_8 = [
                    f"Amount mismatch with high-risk MCC",
                    f"No matching amounts found and MCC {current_mcc} is in high-risk category",
                    f"Current amount {current_amount} vs historical amounts in merchant transactions"
                ]
            else:
                rationale_2_7_2_8 = [
                    f"Amount mismatch with normal-risk MCC", 
                    f"No matching amounts found but MCC {current_mcc} is not in high-risk category",
                    f"Current amount {current_amount} vs historical amounts in merchant transactions"
                ]
        else:
            rationale_2_7_2_8 = [
                f"Conditions not met for amount mismatch analysis",
                f"Either merchant history missing or amounts match historical patterns"
            ]
        
        scenarios.append({
            'scenario_id': '2.7/2.8',
            'triggered': triggered_2_7_2_8,
            'high_risk_mcc': is_high_risk_mcc,
            'rationale': rationale_2_7_2_8
        })
        
        return scenarios

    def _generate_result(self, risky_analysis: Dict, merchant_analysis: Dict, 
                        scenario_results: List[Dict], total_transactions: int) -> Dict:
        """Generate final result with scenario analysis"""
        
        # Scenario configurations
        scenario_configs = {
            '6.1/6.2': {
                'description': 'Transaction done on Risky MCC/MID',
                'fraud_result': 'Probable Fraud',
                'normal_result': 'No Fraud'
            },
            '2.5': {
                'description': 'High value POS/ECOM transaction happening on the same merchant',
                'fraud_result': 'Match Found',
                'normal_result': 'No Match Found'
            },
            '2.6': {
                'description': 'Grouping of same value past transactions by Amount matches the Amount of the Current Alert',
                'fraud_result': 'No Fraud',  # Similar amounts = normal behavior
                'normal_result': 'Probable Fraud (Less)'  # No similar amounts = suspicious
            },
            '2.7/2.8': {
                'description': 'Grouping of same value past transactions by Amount does not match the Amount of the Current Alert with MCC risk consideration',
                'fraud_result_high': 'Probable Fraud (High)',  # High risk MCC + no amount match
                'fraud_result_low': 'Probable Fraud (Less)',   # Normal MCC + no amount match
                'normal_result': 'No Fraud'
            }
        }
        
        # Build scenario analysis
        scenario_analysis = []
        for result in scenario_results:
            scenario_id = result['scenario_id']
            config = scenario_configs[scenario_id]
            
            # Handle different scenario types
            if scenario_id == '2.7/2.8':
                if result['triggered']:
                    if result.get('high_risk_mcc', False):
                        scenario_result = config['fraud_result_high']
                    else:
                        scenario_result = config['fraud_result_low']
                else:
                    scenario_result = config['normal_result']
            else:
                scenario_result = config['fraud_result'] if result['triggered'] else config['normal_result']
            
            # Convert rationale list to string for display
            rationale_display = "; ".join(result['rationale'])
            
            scenario_analysis.append({
                "scenario_id": scenario_id,
                "scenario_description": config['description'],
                "scenario_result": scenario_result,
                "rationale": rationale_display
            })
        
        # Determine overall assessment with proper priority
        triggered_scenarios = [s for s in scenario_results if s['triggered']]
        
        # Priority logic:
        # 1. Risky MCC/MID match = Probable Fraud
        # 2. Amount match (2.6) = No Fraud (normal behavior)
        # 3. No merchant history = No Match Found
        # 4. Amount mismatch + High risk MCC = Probable Fraud (High)
        # 5. Amount mismatch + Normal MCC = Probable Fraud (Less)
        
        risky_mcc_mid_triggered = any(s['scenario_id'] == '6.1/6.2' for s in triggered_scenarios)
        amount_match_triggered = any(s['scenario_id'] == '2.6' for s in triggered_scenarios)
        merchant_history_found = any(s['scenario_id'] == '2.5' for s in triggered_scenarios)
        amount_mismatch_triggered = any(s['scenario_id'] == '2.7/2.8' for s in triggered_scenarios)
        
        if risky_mcc_mid_triggered:
            overall_result = 'Probable Fraud'
        elif amount_match_triggered:
            overall_result = 'No Fraud'
        elif not merchant_history_found:
            overall_result = 'No Match Found'
        elif amount_mismatch_triggered:
            # Check if high risk MCC
            amount_mismatch_scenario = next(s for s in scenario_results if s['scenario_id'] == '2.7/2.8')
            if amount_mismatch_scenario.get('high_risk_mcc', False):
                overall_result = 'Probable Fraud (High)'
            else:
                overall_result = 'Probable Fraud (Less)'
        else:
            overall_result = 'No Fraud'
        
        # Flatten all rationale lists into overall rationale
        overall_rationale = []
        for scenario in triggered_scenarios:
            overall_rationale.extend(scenario['rationale'])
        
        return {
            "scenario_analysis": scenario_analysis,
            "overall_assessment": {
                "result": overall_result,
                "rationale": overall_rationale
            },
            "analysis_metrics": {
                "total_transactions_analyzed": total_transactions,
                "is_risky_mcc": risky_analysis['is_risky_mcc'],
                "is_risky_mid": risky_analysis['is_risky_mid'],
                "current_mcc": risky_analysis['current_mcc'],
                "current_mid": risky_analysis['current_mid'],
                "merchant_transaction_count": merchant_analysis['transaction_count'],
                "has_matching_amounts": merchant_analysis['has_matching_amount'],
                "similar_amount_count": merchant_analysis['similar_amount_count'],
                "amount_variability_threshold": self.amount_variability_threshold,
                "lookback_months": self.lookback_months
            }
        }

    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs"""
        return all(field in kwargs for field in self.required_fields)

    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "merchant_id": {"type": "string", "description": "Current transaction merchant identifier"},
            "mcc": {"type": "string", "description": "Current transaction merchant category code"},
            "transaction_timestamp": {"type": "string", "description": "Current transaction timestamp"},
            "transaction_amount": {"type": "number", "description": "Current transaction amount"}
        }

    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "scenario_analysis": {
                "type": "array",
                "description": "List of individual risky MCC/MID scenario analyses with their IDs, descriptions, results, and rationales.",
                "items": {
                    "type": "object",
                    "properties": {
                        "scenario_id": {
                            "type": "string",
                            "description": "Scenario identifier: '6.1/6.2', '2.5', '2.6', or '2.7/2.8'."
                        },
                        "scenario_description": {
                            "type": "string",
                            "description": "Detailed description of the risky MCC/MID scenario being evaluated."
                        },
                        "scenario_result": {
                            "type": "string",
                            "description": "Outcome: 'Probable Fraud (High)', 'Probable Fraud', 'No Fraud', 'Match Found', or 'No Match Found'."
                        },
                        "rationale": {
                            "type": "string",
                            "description": "Explanation with specific MCC/MID and amount analysis findings."
                        }
                    },
                    "required": ["scenario_id", "scenario_description", "scenario_result", "rationale"]
                }
            },
            "overall_assessment": {
                "type": "object",
                "description": "Overall risky MCC/MID assessment based on all scenario analyses.",
                "properties": {
                    "result": {
                        "type": "string",
                        "description": "Final result: 'Probable Fraud (High)', 'Probable Fraud', 'No Fraud', or 'No Match Found'."
                    },
                    "rationale": {
                        "type": "array",
                        "description": "List of key rationales from triggered scenarios.",
                        "items": {"type": "string"}
                    }
                },
                "required": ["result", "rationale"]
            },
            "analysis_metrics": {
                "type": "object",
                "description": "Numerical risky MCC/MID metrics for AI model decision-making.",
                "properties": {
                    "total_transactions_analyzed": {
                        "type": "integer",
                        "description": "Total historical transactions analyzed within lookback period."
                    },
                    "is_risky_mcc": {
                        "type": "boolean",
                        "description": "Whether current transaction MCC is in the risky MCC list."
                    },
                    "is_risky_mid": {
                        "type": "boolean",
                        "description": "Whether current transaction MID is in the risky MID list."
                    },
                    "current_mcc": {
                        "type": "string",
                        "description": "Current transaction merchant category code."
                    },
                    "current_mid": {
                        "type": "string",
                        "description": "Current transaction merchant identifier."
                    },
                    "merchant_transaction_count": {
                        "type": "integer",
                        "description": "Number of historical transactions found for the same merchant."
                    },
                    "has_matching_amounts": {
                        "type": "boolean",
                        "description": "Whether current transaction amount matches historical amounts for same merchant."
                    },
                    "similar_amount_count": {
                        "type": "integer",
                        "description": "Count of historical transactions with similar amounts within variability threshold."
                    },
                    "amount_variability_threshold": {
                        "type": "number",
                        "description": "Configured threshold for amount similarity, expressed as decimal (e.g., 0.10 for 10%)."
                    },
                    "lookback_months": {
                        "type": "integer",
                        "description": "Number of months used for historical transaction lookback."
                    }
                },
                "required": ["total_transactions_analyzed", "is_risky_mcc", "is_risky_mid", "current_mcc", "current_mid", "merchant_transaction_count", "has_matching_amounts", "similar_amount_count", "amount_variability_threshold", "lookback_months"]
            }
        }
