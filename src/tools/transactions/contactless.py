# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class ContactlessTransactions(BaseTool):
    """Confirm Contactless status and assess consistency with typical contactless usage."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Contactless Payment Analysis Tool",
            description="Analyze consistency of current contactless transaction with historical usage.",
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
            self._logger.error(f"Failed to initialize contactless tool: {e}")
            return False
    
    async def execute(self, customer_id: str, merchant_id: str, payment_method: str, 
                     transaction_amount: float) -> ToolResult:
        """
        Execute contactless payment consistency check
        
        TASK: "Confirm Contactless status and assess consistency with typical contactless usage"
        
        UNIQUE SCOPE:
        1. Contactless payment method verification
        2. Contactless usage frequency analysis
        3. Amount consistency for contactless payments
        4. Merchant consistency for contactless transactions
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Check if payment method is contactless
            is_contactless = payment_method == "Contactless"
            
            if not is_contactless:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "contactless",
                        "customer_id": customer_id,
                        "is_contactless": False,
                        "risk_level": "LOW",
                        "assessment": "Not a contactless transaction - check not applicable"
                    }
                )
            
            # Analyze contactless patterns
            contactless_patterns = self._analyze_contactless_patterns(
                self.user_transactions, transaction_amount, merchant_id
            )
            
            # Assess risk based on contactless behavior
            risk_assessment = self._assess_contactless_risk(contactless_patterns)
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "contactless",
                    "customer_id": customer_id,
                    "is_contactless": True,
                    "contactless_transaction_count": contactless_patterns['contactless_count'],
                    "contactless_rate": contactless_patterns['contactless_rate'],
                    "average_contactless_amount": contactless_patterns['avg_amount'],
                    "amount_consistent": contactless_patterns['amount_consistent'],
                    "typical_merchant": contactless_patterns['typical_merchant'],
                    "risk_level": risk_assessment['level'],
                    "risk_factors": risk_assessment['risk_factors']
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Contactless analysis failed: {str(e)}"
            )
    
    def _analyze_contactless_patterns(self, transactions: List[Dict], 
                                    current_amount: float, current_merchant: str) -> Dict[str, Any]:
        """
        Analyze customer's contactless payment patterns
        
        Contactless Analysis:
        - Frequency of contactless usage
        - Typical amount ranges for contactless
        - Common merchants for contactless payments
        """
        if not transactions:
            return {
                'contactless_count': 0,
                'contactless_rate': 0.0,
                'avg_amount': 0.0,
                'amount_consistent': True,
                'typical_merchant': False
            }
        
        total_transactions = len(transactions)
        
        # Filter contactless transactions
        contactless_transactions = [
            tx for tx in transactions 
            if tx.get('payment_method') == 'Contactless'
        ]
        
        contactless_count = len(contactless_transactions)
        contactless_rate = contactless_count / total_transactions if total_transactions > 0 else 0.0
        
        # Calculate average contactless amount
        if contactless_count > 0:
            amounts = [float(tx['amount']) for tx in contactless_transactions]
            avg_amount = sum(amounts) / len(amounts)
            
            # Check amount consistency (within 0.5x to 2x average)
            amount_consistent = (0.5 * avg_amount) <= current_amount <= (2 * avg_amount)
        else:
            avg_amount = 0.0
            amount_consistent = True  # No history to compare against
        
        # Check merchant consistency
        contactless_merchants = [tx.get('merchant_id') for tx in contactless_transactions]
        typical_merchant = current_merchant in contactless_merchants
        
        return {
            'contactless_count': contactless_count,
            'contactless_rate': round(contactless_rate, 3),
            'avg_amount': round(avg_amount, 2),
            'amount_consistent': amount_consistent,
            'typical_merchant': typical_merchant
        }
    
    def _assess_contactless_risk(self, patterns: Dict) -> Dict[str, Any]:
        """
        Assess risk based on contactless usage patterns
        
        Risk Factors:
        - Low contactless usage rate (unfamiliar with contactless)
        - Amount inconsistent with typical contactless usage
        - Merchant not typical for contactless payments
        - No contactless history
        """
        risk_factors = []
        
        contactless_count = patterns.get('contactless_count', 0)
        contactless_rate = patterns.get('contactless_rate', 0.0)
        amount_consistent = patterns.get('amount_consistent', True)
        typical_merchant = patterns.get('typical_merchant', False)
        
        # No contactless history
        if contactless_count == 0:
            risk_factors.append("No previous contactless transaction history")
            return {
                'level': 'HIGH',
                'risk_factors': risk_factors
            }
        
        # Low contactless usage rate
        if contactless_rate < 0.1 and contactless_count >= 3:
            risk_factors.append(f"Low contactless usage rate: {contactless_rate:.1%}")
        
        # Amount inconsistency
        if not amount_consistent:
            risk_factors.append("Transaction amount inconsistent with typical contactless amounts")
        
        # Merchant inconsistency
        if not typical_merchant:
            risk_factors.append("Merchant not typical for contactless transactions")
        
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
            "payment_method": {"type": "string", "description": "Payment method"},
            "transaction_amount": {"type": "number", "description": "Transaction amount"},
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "is_contactless": {"type": "boolean"},
            "contactless_transaction_count": {"type": "integer"},
            "contactless_rate": {"type": "number"},
            "average_contactless_amount": {"type": "number"},
            "amount_consistent": {"type": "boolean"},
            "typical_merchant": {"type": "boolean"},
            "risk_level": {"type": "string"},
            "risk_factors": {"type": "array"}
        }
