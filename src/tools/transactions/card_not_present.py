# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class CNPTransactions(BaseTool):
    """Confirm CNP status and assess consistency with typical CNP behavior."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Card Not Present Analysis Tool",
            description="Analyze current CNP transactions against historical CNP patterns.",
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
            self._logger.error(f"Failed to initialize CNP tool: {e}")
            return False
    
    async def execute(self, customer_id: str, payment_method: str, 
                     merchant_id: str, ip_address: str) -> ToolResult:
        """
        Execute CNP behavior consistency check
        
        TASK: "Confirm CNP status and assess consistency with typical CNP behavior"
        
        SIMPLE SCOPE:
        1. Verify CNP payment method
        2. Analyze CNP usage frequency and patterns
        3. Check merchant consistency for CNP transactions
        4. Basic IP address pattern analysis
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Check if payment method is CNP
            is_cnp = payment_method == "CNP"
            
            if not is_cnp:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "cnp",
                        "customer_id": customer_id,
                        "is_cnp": False,
                        "risk_level": "LOW",
                        "assessment": "Not a CNP transaction"
                    }
                )
            
            # Analyze CNP patterns
            cnp_patterns = self._analyze_cnp_patterns(
                self.user_transactions, merchant_id, ip_address
            )
            
            # Simple risk assessment
            risk_assessment = self._assess_cnp_risk(cnp_patterns)
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "cnp",
                    "customer_id": customer_id,
                    "is_cnp": True,
                    "cnp_transaction_count": cnp_patterns['cnp_count'],
                    "cnp_rate": cnp_patterns['cnp_rate'],
                    "merchant_consistent": cnp_patterns['merchant_consistent'],
                    "ip_pattern_consistent": cnp_patterns['ip_consistent'],
                    "risk_level": risk_assessment['level'],
                    "risk_factors": risk_assessment['risk_factors']
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"CNP analysis failed: {str(e)}"
            )
    
    def _analyze_cnp_patterns(self, transactions: List[Dict], 
                            current_merchant: str, current_ip: str) -> Dict[str, Any]:
        """
        Simple analysis of CNP usage patterns
        """
        if not transactions:
            return {
                'cnp_count': 0,
                'cnp_rate': 0.0,
                'merchant_consistent': False,
                'ip_consistent': False
            }
        
        total_transactions = len(transactions)
        
        # Filter CNP transactions
        cnp_transactions = [
            tx for tx in transactions 
            if tx.get('payment_method') == 'CNP'
        ]
        
        cnp_count = len(cnp_transactions)
        cnp_rate = cnp_count / total_transactions if total_transactions > 0 else 0.0
        
        # Check merchant consistency
        cnp_merchants = [tx.get('merchant_id') for tx in cnp_transactions]
        merchant_consistent = current_merchant in cnp_merchants
        
        # Basic IP pattern check (simple subnet matching)
        cnp_ips = [tx.get('ip_address') for tx in cnp_transactions if tx.get('ip_address')]
        ip_consistent = False
        if cnp_ips and current_ip:
            # Simple check - same subnet (first 3 octets)
            current_subnet = '.'.join(current_ip.split('.')[:3])
            for ip in cnp_ips:
                ip_subnet = '.'.join(ip.split('.')[:3])
                if current_subnet == ip_subnet:
                    ip_consistent = True
                    break
        
        return {
            'cnp_count': cnp_count,
            'cnp_rate': round(cnp_rate, 3),
            'merchant_consistent': merchant_consistent,
            'ip_consistent': ip_consistent
        }
    
    def _assess_cnp_risk(self, patterns: Dict) -> Dict[str, Any]:
        """
        Simple risk assessment for CNP transactions
        """
        risk_factors = []
        
        cnp_count = patterns.get('cnp_count', 0)
        merchant_consistent = patterns.get('merchant_consistent', False)
        ip_consistent = patterns.get('ip_consistent', False)
        
        # No CNP history
        if cnp_count == 0:
            risk_factors.append("No previous CNP transaction history")
            return {
                'level': 'HIGH',
                'risk_factors': risk_factors
            }
        
        # Check for inconsistencies
        if not merchant_consistent:
            risk_factors.append("Unusual merchant for CNP transaction")
        
        if not ip_consistent:
            risk_factors.append("Unusual IP address pattern for CNP transaction")
        
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
            "payment_method": {"type": "string", "description": "Payment method"},
            "ip_address": {"type": "string", "description": "IP address for CNP transaction"}
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "is_cnp": {"type": "boolean"},
            "cnp_transaction_count": {"type": "integer"},
            "cnp_rate": {"type": "number"},
            "merchant_consistent": {"type": "boolean"},
            "ip_pattern_consistent": {"type": "boolean"},
            "risk_level": {"type": "string"},
            "risk_factors": {"type": "array"}
        }
