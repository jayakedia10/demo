# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class CardPresentTransactions(BaseTool):
    """Confirms card present status and assesses consistency with typical card-present spending behavior."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Card Present Analysis Tool",
            description="Analyze current card-present transaction against historical card-present patterns and recent activity.",
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
            self._logger.error(f"Failed to initialize card present tool: {e}")
            return False
    
    async def execute(self, customer_id: str, card_present: bool, 
                     location: str, merchant_id: str) -> ToolResult:
        """
        Execute card present consistency check
        
        TASK: "Confirm card present status and assess consistency with typical 
        card-present spending behavior"
        
        UNIQUE SCOPE:
        1. Card present vs card not present behavior analysis
        2. Location consistency for card present transactions
        3. Merchant consistency for card present transactions
        4. Recent CNP activity consideration
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # If transaction is not card present, return early
            if not card_present:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "card_present",
                        "customer_id": customer_id,
                        "is_card_present": card_present,
                        "risk_level": "LOW",
                        "assessment": "Card not present transaction - check not applicable"
                    }
                )
            
            # Analyze card present patterns
            cp_patterns = self._analyze_card_present_patterns(self.user_transactions)
            
            # Check consistency with current transaction
            consistency_check = self._check_consistency(
                cp_patterns, location, merchant_id
            )
            
            # Assess risk based on card present behavior
            risk_assessment = self._assess_card_present_risk(
                cp_patterns, consistency_check
            )
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "card_present",
                    "customer_id": customer_id,
                    "is_card_present": card_present,
                    "cp_rate": cp_patterns['cp_rate'],
                    "location_consistent": consistency_check['location_consistent'],
                    "merchant_consistent": consistency_check['merchant_consistent'],
                    "risk_level": risk_assessment['level'],
                    "risk_factors": risk_assessment['risk_factors']
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Card present analysis failed: {str(e)}"
            )
    
    def _analyze_card_present_patterns(self, transactions: List[Dict]) -> Dict[str, Any]:
        """
        Analyze customer's card present vs card not present patterns
        
        Card Present Payment Methods: Card Present, Contactless, Pin Verified, Mag Stripe
        Card Not Present Payment Methods: Card Not Present, Online
        """
        if not transactions:
            return {
                'cp_count': 0,
                'cnp_count': 0,
                'cp_locations': [],
                'cp_merchants': [],
                'cp_rate': 0.0
            }
        
        # Define card present payment methods
        card_present_methods = {
            'Card Present', 'Contactless', 'Pin Verified', 'Mag Stripe'
        }
        
        # Separate card present and card not present transactions
        cp_transactions = [
            tx for tx in transactions 
            if tx.get('payment_method') in card_present_methods
        ]
        
        cnp_transactions = [
            tx for tx in transactions 
            if tx.get('payment_method') not in card_present_methods
        ]
        
        cp_count = len(cp_transactions)
        cnp_count = len(cnp_transactions)
        total_count = len(transactions)
        
        # Calculate card present rate
        cp_rate = cp_count / total_count if total_count > 0 else 0.0
        
        # Extract card present locations and merchants
        cp_locations = [tx.get('location') for tx in cp_transactions if tx.get('location')]
        cp_merchants = [tx.get('merchant_id') for tx in cp_transactions if tx.get('merchant_id')]
        
        return {
            'cp_count': cp_count,
            'cnp_count': cnp_count,
            'cp_rate': round(cp_rate, 3),
            'cp_locations': cp_locations,
            'cp_merchants': cp_merchants
        }
    
    def _check_consistency(self, cp_patterns: Dict, current_location: str, 
                         current_merchant: str) -> Dict[str, Any]:
        """
        Check if current card present transaction is consistent with patterns
        
        Consistency Checks:
        - Location: Is current location common in card present history?
        - Merchant: Is current merchant common in card present history?
        """
        cp_locations = cp_patterns.get('cp_locations', [])
        cp_merchants = cp_patterns.get('cp_merchants', [])
        
        # Check location consistency
        location_consistent = False
        if cp_locations:
            # Count location frequency
            location_counts = {}
            for loc in cp_locations:
                location_counts[loc] = location_counts.get(loc, 0) + 1
            
            # Check if current location appears in top locations
            total_cp = len(cp_locations)
            current_location_count = location_counts.get(current_location, 0)
            location_frequency = current_location_count / total_cp if total_cp > 0 else 0
            
            # Consider consistent if location appears in at least 10% of CP transactions
            location_consistent = location_frequency >= 0.1
        
        # Check merchant consistency
        merchant_consistent = False
        if cp_merchants:
            # Count merchant frequency
            merchant_counts = {}
            for merchant in cp_merchants:
                merchant_counts[merchant] = merchant_counts.get(merchant, 0) + 1
            
            # Check if current merchant appears in CP history
            current_merchant_count = merchant_counts.get(current_merchant, 0)
            total_cp = len(cp_merchants)
            merchant_frequency = current_merchant_count / total_cp if total_cp > 0 else 0
            
            # Consider consistent if merchant appears in CP history
            merchant_consistent = current_merchant_count > 0
        
        return {
            'location_consistent': location_consistent,
            'merchant_consistent': merchant_consistent,
            'location_frequency': location_frequency if cp_locations else 0.0,
            'merchant_frequency': merchant_frequency if cp_merchants else 0.0
        }
    
    def _assess_card_present_risk(self, cp_patterns: Dict, 
                                consistency_check: Dict) -> Dict[str, Any]:
        """
        Assess risk based on card present behavior patterns
        
        Risk Factors:
        - Low card present usage rate (mostly CNP user trying CP)
        - Inconsistent location for CP transaction
        - Inconsistent merchant for CP transaction
        - No card present history
        """
        risk_factors = []
        
        cp_count = cp_patterns.get('cp_count', 0)
        cnp_count = cp_patterns.get('cnp_count', 0)
        cp_rate = cp_patterns.get('cp_rate', 0.0)
        
        location_consistent = consistency_check.get('location_consistent', False)
        merchant_consistent = consistency_check.get('merchant_consistent', False)
        
        # Check for no card present history
        if cp_count == 0:
            risk_factors.append("No previous card present transaction history")
            return {
                'level': 'HIGH',
                'risk_factors': risk_factors
            }
        
        # Check for low card present usage (predominantly CNP user)
        if cp_rate < 0.2 and cnp_count > 5:  # Less than 20% CP usage with significant CNP history
            risk_factors.append(f"Low card present usage rate: {cp_rate:.1%}")
        
        # Check location consistency
        if not location_consistent:
            risk_factors.append("Unusual location for card present transaction")
        
        # Check merchant consistency
        if not merchant_consistent:
            risk_factors.append("Unusual merchant for card present transaction")
        
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
            "merchant_id": {"type": "string", "description": "Merchant identifier"},
            "card_present": {"type": "boolean", "description": "Is transaction card present"},
            "location": {"type": "string", "description": "Transaction location"},
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "is_card_present": {"type": "boolean"},
            "cp_transaction_count": {"type": "integer"},
            "cnp_transaction_count": {"type": "integer"},
            "location_consistent": {"type": "boolean"},
            "merchant_consistent": {"type": "boolean"},
            "risk_level": {"type": "string"},
            "risk_factors": {"type": "array"}
        }
