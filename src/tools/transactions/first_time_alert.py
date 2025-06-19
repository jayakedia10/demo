# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class FirstTimeAlertTransactions(BaseTool):
    """Verifies if this is the user's first alert in the system."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="First Time Alert Analysis Tool",
            description="Provide user tenure and overall activity level context.",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Transactions Data Wrapper"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())
    
    async def initialize(self, **kwargs) -> bool:
        """Initialize and check alert history for customer"""
        try:
            data = self.transaction_data
            customer_id = kwargs.get('customer_id')
            
            # Get customer transactions to check for alert history
            self.user_transactions = data[
                (data['customer_id'] == customer_id)
            ].to_dict('records')
            
            # Check for alert history value in transactions
            self.has_alert_history = self._check_alert_history(customer_id)
            
            self._is_initialized = True
            return True
        except Exception as e:
            self._logger.error(f"Failed to initialize first time alert tool: {e}")
            return False
    
    async def execute(self, customer_id: str) -> ToolResult:
        """Execute first time alert verification"""
        try:
            await self.initialize(customer_id=customer_id)
            
            is_first_time_alert = not self.has_alert_history
            
            # Risk assessment
            risk_level = "LOW" if is_first_time_alert else "HIGH"
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "first_time_alert",
                    "customer_id": customer_id,
                    "is_first_time_alert": is_first_time_alert,
                    "has_alert_history": self.has_alert_history,
                    "risk_assessment": risk_level,
                    "reasoning": "First time alerts have lower suspicion threshold" if is_first_time_alert else "Previous alert history indicates pattern"
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"First time alert analysis failed: {str(e)}"
            )
    
    def _check_alert_history(self, customer_id: str) -> bool:
        """
        Check if customer has alert history
        
        Logic:
        - Look for alert_history field in transaction data
        - Return True if customer has previous alerts, False if first time
        """
        if not self.user_transactions:
            return False  # No transactions = no alert history
        
        # Check if any transaction has alert_history field set to True
        # or if there are any records indicating previous alerts
        for transaction in self.user_transactions:
            # Check for alert_history field in transaction data
            if transaction.get('alert_history', False):
                return True
            
            # Alternative: Check for any alert-related fields
            if transaction.get('previous_alerts', 0) > 0:
                return True
        
        # If no alert history found, this is first time
        return False
    
    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)
    
    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"}
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "is_first_time_alert": {"type": "boolean"},
            "has_alert_history": {"type": "boolean"},
            "risk_assessment": {"type": "string"}
        }
