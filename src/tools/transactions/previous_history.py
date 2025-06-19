# Importing Dependencies
import pandas as pd
from datetime import datetime
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class PreviousHistoryTransactions(BaseTool):
    """Analyzes customer's transaction history with specific merchant."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Previous Historical Transactions Analysis Tool",
            description="Retrieve and summarize transaction history with a specific merchant.",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Transactions Data Wrapper"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())
        self.user_transactions = []  # Initialize empty list

    async def initialize(self, **kwargs) -> bool:
        """Initialize the resources for the tool"""
        try:
            data = self.transaction_data
            self.user_transactions = data[
                (data['customer_id'] == kwargs.get('customer_id')) & 
                (data['merchant_id'] == kwargs.get('merchant_id'))
            ].to_dict('records')
            self._is_initialized = True  # Set initialization flag
            return True
        except Exception as e:
            self._logger.error(f"Failed to initialize data generator: {str(e)}")
            return False

    async def execute(self, customer_id: str, merchant_id: str) -> ToolResult:
        """
        Compare the current alerted transaction to the historical transactions for consistency."
        """
        try:
            # Step: 0: Initialize Tool
            init_success = await self.initialize(customer_id=customer_id,
                                               merchant_id=merchant_id)
            
            if not init_success:
                return ToolResult(
                    tool_name=self.name,
                    success=False,
                    result={},
                    error="Failed to initialize tool"
                )

            # Step 1: Calculate relationship metrics
            relationship_metrics = self._calculate_relationship_metrics(self.user_transactions)
            
            # Step 2: Assess merchant familiarity
            familiarity_score = self._calculate_familiarity_score(relationship_metrics)
            
            # Step 3: Determine risk level
            risk_assessment = self._assess_risk(relationship_metrics, familiarity_score)
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "previous_history_check",
                    "customer_id": customer_id,
                    "merchant_id": merchant_id,
                    "relationship_status": relationship_metrics['status'],
                    "familiarity_score": familiarity_score,
                    "metrics": relationship_metrics,
                    "risk_assessment": risk_assessment['level'],
                    "risk_score": risk_assessment['score'],
                }
            )

        except Exception as e:
            self._logger.error(f"Analysis failed: {str(e)}")
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Analysis failed: {str(e)}"
            )
    def _calculate_relationship_metrics(self, merchant_history: List[Dict]) -> Dict[str, Any]:
        """
        Calculate core relationship metrics with simple formulas
        
        Formulas:
        - Transaction Count: N = len(transactions)
        - Interaction Span: S = (last_date - first_date).days
        - Frequency Rate: F = N / max(S, 1) * 30  (transactions per month)
        """
        if not merchant_history:
            return {
                'status': 'FIRST_TIME',
                'transaction_count': 0,
                'interaction_span_days': 0,
                'frequency_rate': 0,
                'recency_days': None
            }
        
        # Basic metrics
        N = len(merchant_history)  # Transaction count
        dates = [datetime.fromisoformat(str(tx['transaction_date'])) for tx in merchant_history]
        if N == 1:
            S = 1  # Interaction span for single transaction
            recency_days = (datetime.now() - dates[0]).days
        else:
            S = (max(dates) - min(dates)).days + 1  # Interaction span
            recency_days = (datetime.now() - max(dates)).days
        
        # Frequency rate (transactions per month)
        F = (N / max(S, 1)) * 30
        
        # Relationship status
        if N >= 10 and S >= 90:
            status = 'ESTABLISHED'
        elif N >= 2:
            status = 'MINIMAL'
        else:
            status = 'FIRST_TIME'
        
        return {
            'status': status,
            'transaction_count': N,
            'interaction_span_days': S,
            'frequency_rate': round(F, 2),
            'recency_days': recency_days
        }
    
    def _calculate_familiarity_score(self, metrics: Dict[str, Any]) -> float:
        """
        Calculate merchant familiarity score using weighted formula
        
        Formula:
        Familiarity Score = (0.4 * N_norm) + (0.3 * S_norm) + (0.3 * R_norm)
        
        Where:
        - N_norm = min(transaction_count / 10, 1.0)  # Normalized transaction count
        - S_norm = min(interaction_span / 365, 1.0)  # Normalized span (1 year max)
        - R_norm = max(0, 1 - recency_days / 365)    # Normalized recency (fresher = higher)
        """
        N = metrics['transaction_count']
        S = metrics['interaction_span_days']
        recency = metrics['recency_days']
        
        if N == 0:
            return 0.0
        
        # Normalize components
        N_norm = min(N / 10, 1.0)                    # Transaction count (max at 10 transactions)
        S_norm = min(S / 365, 1.0)                   # Interaction span (max at 1 year)
        R_norm = max(0, 1 - recency / 365) if recency is not None else 0  # Recency factor
        
        # Weighted familiarity score
        familiarity_score = (0.4 * N_norm) + (0.3 * S_norm) + (0.3 * R_norm)
        
        return round(familiarity_score, 3)
    
    def _assess_risk(self, metrics: Dict[str, Any], familiarity_score: float) -> Dict[str, Any]:
        """
        Simple risk assessment based on merchant relationship
        
        Risk Formula:
        Risk Score = 1.0 - familiarity_score
        
        Risk Levels:
        - HIGH (0.7-1.0): First-time or very unfamiliar merchant
        - MEDIUM (0.4-0.69): Limited relationship
        - LOW (0.0-0.39): Established relationship
        """
        status = metrics['status']
        
        # Calculate risk score (inverse of familiarity)
        risk_score = round(1.0 - familiarity_score, 3)
        
        # Determine risk level
        if risk_score >= 0.7:
            risk_level = 'HIGH'
        elif risk_score >= 0.4:
            risk_level = 'MEDIUM' 
        else:
            risk_level = 'LOW'
        
        # Adjust for first-time merchants
        if status == 'FIRST_TIME':
            risk_level = 'HIGH'
            risk_score = max(risk_score, 0.8)
        
        return {
            'level': risk_level,
            'score': risk_score,
        }

    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)

    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "merchant_id": {"type": "string", "description": "Merchant identiifier"},
        }
 
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "analysis_type": {"type": "string"},
            "relationship_status": {"type": "string"},
            "familiarity_score": {"type": "number"},
            "risk_assessment": {"type": "string"},
            "risk_score": {"type": "number"},
        }