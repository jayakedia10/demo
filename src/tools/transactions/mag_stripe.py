# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class MagStripeTransactions(BaseTool):
    """Confirm Mag Stripe usage and assess if it's typical for the user and context."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Mag Stripe Payment Analysis Tool",
            description="Report on user's magstripe usage patterns and typicality of current transaction.",
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
            self._logger.error(f"Failed to initialize mag stripe tool: {e}")
            return False
    
    async def execute(self, customer_id: str, payment_sub_type: str, 
                     location: str) -> ToolResult:
        """
        Execute mag stripe usage consistency check
        
        TASK: "Confirm Mag Stripe usage and assess if it's typical for the user and context"
        
        SIMPLE SCOPE:
        1. Verify mag stripe payment method
        2. Analyze mag stripe vs EMV usage frequency
        3. Check location consistency for mag stripe usage
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Check if payment sub type is Mag Stripe
            is_mag_stripe = payment_sub_type == "Mag Stripe"
            
            if not is_mag_stripe:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "mag_stripe",
                        "customer_id": customer_id,
                        "is_mag_stripe": False,
                        "risk_level": "LOW",
                        "assessment": "Not a mag stripe transaction"
                    }
                )
            
            # Analyze mag stripe patterns
            mag_stripe_patterns = self._analyze_mag_stripe_patterns(
                self.user_transactions, location
            )
            
            # Simple risk assessment
            risk_assessment = self._assess_mag_stripe_risk(mag_stripe_patterns)
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "mag_stripe",
                    "customer_id": customer_id,
                    "is_mag_stripe": True,
                    "mag_stripe_count": mag_stripe_patterns['mag_stripe_count'],
                    "emv_count": mag_stripe_patterns['emv_count'],
                    "mag_stripe_rate": mag_stripe_patterns['mag_stripe_rate'],
                    "location_consistent": mag_stripe_patterns['location_consistent'],
                    "risk_level": risk_assessment['level'],
                    "risk_factors": risk_assessment['risk_factors']
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Mag stripe analysis failed: {str(e)}"
            )
    
    def _analyze_mag_stripe_patterns(self, transactions: List[Dict], 
                                   current_location: str) -> Dict[str, Any]:
        """
        Simple analysis of mag stripe vs EMV usage patterns
        """
        if not transactions:
            return {
                'mag_stripe_count': 0,
                'emv_count': 0,
                'mag_stripe_rate': 0.0,
                'location_consistent': False
            }
        
        # Count mag stripe and EMV transactions
        mag_stripe_transactions = [
            tx for tx in transactions 
            if tx.get('payment_sub_type') == 'Mag Stripe'
        ]
        
        emv_transactions = [
            tx for tx in transactions 
            if tx.get('payment_sub_type') == 'EMV Chip'
        ]
        
        mag_stripe_count = len(mag_stripe_transactions)
        emv_count = len(emv_transactions)
        total_card_present = mag_stripe_count + emv_count
        
        # Calculate mag stripe usage rate
        mag_stripe_rate = mag_stripe_count / total_card_present if total_card_present > 0 else 0.0
        
        # Check location consistency
        mag_stripe_locations = [tx.get('location') for tx in mag_stripe_transactions]
        location_consistent = current_location in mag_stripe_locations
        
        return {
            'mag_stripe_count': mag_stripe_count,
            'emv_count': emv_count,
            'mag_stripe_rate': round(mag_stripe_rate, 3),
            'location_consistent': location_consistent
        }
    
    def _assess_mag_stripe_risk(self, patterns: Dict) -> Dict[str, Any]:
        """
        Simple risk assessment for mag stripe usage
        """
        risk_factors = []
        
        mag_stripe_count = patterns.get('mag_stripe_count', 0)
        mag_stripe_rate = patterns.get('mag_stripe_rate', 0.0)
        location_consistent = patterns.get('location_consistent', False)
        
        # No mag stripe history
        if mag_stripe_count == 0:
            risk_factors.append("No previous mag stripe transaction history")
            return {
                'level': 'HIGH',
                'risk_factors': risk_factors
            }
        
        # Low mag stripe usage or inconsistent location
        if mag_stripe_rate < 0.1:
            risk_factors.append(f"Low mag stripe usage rate: {mag_stripe_rate:.1%}")
        
        if not location_consistent:
            risk_factors.append("Unusual location for mag stripe transaction")
        
        # Determine risk level
        risk_count = len(risk_factors)
        if risk_count >= 2:
            risk_level = 'HIGH'
        elif risk_count == 1:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        return {
            'level': risk_level,
            'risk_factors': risk_factors
        }
    
    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)
    
    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "payment_sub_type": {"type": "string", "description": "Payment sub type"},
            "location": {"type": "string", "description": "Transaction location"}
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "is_mag_stripe": {"type": "boolean"},
            "mag_stripe_count": {"type": "integer"},
            "emv_count": {"type": "integer"},
            "mag_stripe_rate": {"type": "number"},
            "location_consistent": {"type": "boolean"},
            "risk_level": {"type": "string"},
            "risk_factors": {"type": "array"}
        }
