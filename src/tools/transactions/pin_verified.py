# Importing Dependencies
import pandas as pd
from typing import Any, Dict

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class PinVerifiedTransactions(BaseTool):
    """Confirm PIN Verified status and assess contextual factors despite PIN usage."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Pin Verified Analysis Tool",
            description="Analyze contextual factors of the PIN-verified transaction against historical patterns.",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Transactions Data Wrapper"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())
    
    async def initialize(self, **kwargs) -> bool:
        try:
            data = self.transaction_data
            self.user_transactions = data[
                (data['customer_id'] == kwargs.get('customer_id'))
            ].to_dict('records')
            self._is_initialized = True
            return True
        except Exception as e:
            self._logger.error(f"Failed to initialize pin verified tool: {e}")
            return False
    
    async def execute(self, customer_id: str, pin_verified: bool, 
                     location: str, amount: float, merchant_id: str) -> ToolResult:
        try:
            await self.initialize(customer_id=customer_id)
            
            if not pin_verified:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "pin_verified",
                        "is_pin_verified": False,
                        "risk_level": "LOW"
                    }
                )
            
            # Analyze PIN verified patterns
            pin_txns = [tx for tx in self.user_transactions if tx.get('pin_verified')]
            total_txns = len(self.user_transactions)
            pin_count = len(pin_txns)
            pin_rate = pin_count / total_txns if total_txns > 0 else 0
            
            # Check consistencies
            locations = [tx.get('location') for tx in pin_txns if tx.get('location')]
            location_consistent = location in locations if locations else True
            
            amounts = [tx.get('amount') for tx in pin_txns if tx.get('amount')]
            avg_amount = sum(amounts) / len(amounts) if amounts else 0
            amount_consistent = (0.5 * avg_amount <= amount <= 2 * avg_amount) if avg_amount > 0 else True
            
            merchants = [tx.get('merchant_id') for tx in pin_txns if tx.get('merchant_id')]
            merchant_consistent = merchant_id in merchants if merchants else True
            
            # Simple risk assessment
            risk_factors = []
            if pin_count == 0:
                risk_factors.append("No PIN verified history")
            if pin_rate < 0.2 and total_txns > 5:
                risk_factors.append("Low PIN usage rate")
            if not location_consistent:
                risk_factors.append("Unusual location")
            if not amount_consistent:
                risk_factors.append("Unusual amount")
            if not merchant_consistent:
                risk_factors.append("Unusual merchant")
            
            risk_level = "HIGH" if len(risk_factors) >= 2 else "MEDIUM" if risk_factors else "LOW"
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "pin_verified",
                    "is_pin_verified": True,
                    "pin_transaction_count": pin_count,
                    "pin_usage_rate": round(pin_rate, 3),
                    "location_consistent": location_consistent,
                    "amount_consistent": amount_consistent,
                    "merchant_consistent": merchant_consistent,
                    "risk_level": risk_level,
                    "risk_factors": risk_factors
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Pin verified analysis failed: {str(e)}"
            )
    
    def validate_inputs(self, **kwargs) -> bool:
        return all(field in kwargs for field in self.required_fields)
    
    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Unique identifier for the customer"},
            "merchant_id": {"type": "string", "description": "Unique identifier for the merchant"},
            "pin_verified": {"type": "boolean", "description": "Whether the transaction PIN was verified"},
            "location": {"type": "string", "description": "Location where the transaction occurred"},
            "amount": {"type": "number", "description": "Transaction amount"},
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "is_pin_verified": {"type": "boolean"},
            "risk_level": {"type": "string"}
        }
