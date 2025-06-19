# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class RiskyCountryCurrencyTransactions(BaseTool):
    """Simple check for risky countries and currencies."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Risky Country Currency Analysis Tool",
            description="Check user's transaction history with the current risky country/currency and others",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Transactions Data Wrapper"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())

    async def initialize(self, **kwargs) -> bool:
        """Initialize the data generator for the tool"""
        try:
            data = self.transaction_data
            self.user_transactions = data[
                (data['customer_id'] == kwargs.get('customer_id'))
            ].to_dict('records')
            self._is_initialized = True
            return True
        except Exception as e:
            self._logger.error(f"Failed to initialize risky country/currency tool: {e}")
            return False
    
    async def execute(self, customer_id: str, country: str, 
                     currency: str, risky_countries: List[str], risky_currencies: List[str]) -> ToolResult:
        """
        Execute simple risky country/currency check
        
        TASK: "Identify transaction's country/currency and check if they are on a risky list"
        
        SIMPLE SCOPE:
        1. Check if current country/currency is risky
        2. Calculate basic exposure rate
        3. Simple risk assessment
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Simple risk check
            is_risky_country = country in risky_countries
            is_risky_currency = currency in risky_currencies
            
            # Calculate basic exposure
            exposure_metrics = self._calculate_exposure(risky_countries)
            
            # Simple risk level determination
            risk_level = self._determine_risk_level(
                is_risky_country, is_risky_currency, exposure_metrics['exposure_rate']
            )
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "risky_country_currency",
                    "customer_id": customer_id,
                    "country": country,
                    "currency": currency,
                    "is_risky_country": is_risky_country,
                    "is_risky_currency": is_risky_currency,
                    "risky_transaction_count": exposure_metrics['risky_count'],
                    "total_transactions": exposure_metrics['total_count'],
                    "exposure_rate": exposure_metrics['exposure_rate'],
                    "risk_level": risk_level
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Risky country/currency analysis failed: {str(e)}"
            )
    
    def _calculate_exposure(self, risky_countries: List[str]) -> Dict[str, Any]:
        """Calculate simple exposure to risky countries"""
        total_transactions = len(self.user_transactions)
        
        if total_transactions == 0:
            return {
                'risky_count': 0,
                'total_count': 0,
                'exposure_rate': 0.0
            }
        
        # Count risky transactions
        risky_transactions = [
            tx for tx in self.user_transactions 
            if tx.get('country') in risky_countries
        ]
        risky_count = len(risky_transactions)
        exposure_rate = risky_count / total_transactions
        
        return {
            'risky_count': risky_count,
            'total_count': total_transactions,
            'exposure_rate': round(exposure_rate, 3)
        }
    
    def _determine_risk_level(self, is_risky_country: bool, 
                            is_risky_currency: bool, exposure_rate: float) -> str:
        """
        Simple risk level determination
        
        Logic from search results:
        - If risky country OR risky currency:
          - If exposure > 20%: HIGH
          - Else: MEDIUM
        - Else: LOW
        """
        if is_risky_country or is_risky_currency:
            if exposure_rate > 0.01:
                return 'HIGH'
            else:
                return 'MEDIUM'
        else:
            return 'LOW'
    
    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)
    
    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "country": {"type": "string", "description": "Transaction country code"},
            "currency": {"type": "string", "description": "Transaction currency code"},
            "risky_countries": {"type": "array", "items": {"type": "string"}, "description": "List of risky countries"},
            "risky_currencies": {"type": "array", "items": {"type": "string"}, "description": "List of risky currencies"},
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "country": {"type": "string"},
            "currency": {"type": "string"},
            "is_risky_country": {"type": "boolean"},
            "is_risky_currency": {"type": "boolean"},
            "risky_transaction_count": {"type": "integer"},
            "total_transactions": {"type": "integer"},
            "exposure_rate": {"type": "number"},
            "risk_level": {"type": "string"}
        }
