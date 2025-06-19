# Importing Dependencies
import pandas as pd
from typing import Any, Dict, List

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class TokenNFCTransactions(BaseTool):
    """Confirm Token NFC status and assess consistency with typical tokenized NFC payment usage."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Token NFC Payment Analysis Tool",
            description="Analyze consistency of current Token NFC transaction with historical usage.",
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
            self._logger.error(f"Failed to initialize token NFC tool: {e}")
            return False
    
    async def execute(self, customer_id: str, payment_sub_type: str, device_id: str) -> ToolResult:
        """
        Execute Token NFC payment consistency check
        
        TASK: "Confirm Token NFC status and assess consistency with typical tokenized NFC payment usage"
        
        UNIQUE SCOPE:
        1. Token NFC payment verification
        2. Mobile payment adoption and frequency analysis
        3. Device usage pattern analysis
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Check if payment sub type is Token NFC
            is_token_nfc = payment_sub_type == "Token NFC" or payment_sub_type == "Tap to Pay"
            
            if not is_token_nfc:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "token_nfc",
                        "customer_id": customer_id,
                        "is_token_nfc": False,
                        "risk_level": "LOW",
                        "assessment": "Not a Token NFC transaction - check not applicable"
                    }
                )
            
            # Analyze Token NFC patterns
            token_nfc_patterns = self._analyze_token_nfc_patterns(
                self.user_transactions, device_id
            )
            
            # Assess risk based on Token NFC behavior
            risk_assessment = self._assess_token_nfc_risk(token_nfc_patterns)
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "token_nfc",
                    "customer_id": customer_id,
                    "is_token_nfc": True,
                    "token_nfc_count": token_nfc_patterns['token_nfc_count'],
                    "token_nfc_rate": token_nfc_patterns['token_nfc_rate'],
                    "device_count": token_nfc_patterns['device_count'],
                    "device_consistent": token_nfc_patterns['device_consistent'],
                    "risk_level": risk_assessment['level'],
                    "risk_factors": risk_assessment['risk_factors']
                }
            )
            
        except Exception as e:
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Token NFC analysis failed: {str(e)}"
            )
    
    def _analyze_token_nfc_patterns(self, transactions: List[Dict], 
                                  current_device: str) -> Dict[str, Any]:
        """
        Analyze customer's Token NFC payment patterns
        
        Token NFC Analysis:
        - Adoption rate of Token NFC payments
        - Device usage patterns
        - Mobile payment behavior consistency
        """
        if not transactions:
            return {
                'token_nfc_count': 0,
                'token_nfc_rate': 0.0,
                'device_count': 0,
                'device_consistent': False
            }
        
        total_transactions = len(transactions)
        
        # Filter Token NFC transactions
        token_nfc_transactions = [
            tx for tx in transactions 
            if tx.get('payment_sub_type') == 'Token NFC'
        ]
        
        token_nfc_count = len(token_nfc_transactions)
        token_nfc_rate = token_nfc_count / total_transactions if total_transactions > 0 else 0.0
        
        # Analyze device usage
        if token_nfc_count > 0:
            # Count unique devices used for Token NFC
            devices_used = [tx.get('device_id') for tx in token_nfc_transactions if tx.get('device_id')]
            unique_devices = set(devices_used)
            device_count = len(unique_devices)
            
            # Check if current device is consistent with history
            device_consistent = current_device in devices_used if current_device else False
        else:
            device_count = 0
            device_consistent = False
        
        return {
            'token_nfc_count': token_nfc_count,
            'token_nfc_rate': round(token_nfc_rate, 3),
            'device_count': device_count,
            'device_consistent': device_consistent
        }
    
    def _assess_token_nfc_risk(self, patterns: Dict) -> Dict[str, Any]:
        """
        Assess risk based on Token NFC usage patterns
        
        Risk Factors:
        - No Token NFC history (first-time mobile payment)
        - Low Token NFC adoption rate
        - New/unfamiliar device usage
        """
        risk_factors = []
        
        token_nfc_count = patterns.get('token_nfc_count', 0)
        token_nfc_rate = patterns.get('token_nfc_rate', 0.0)
        device_count = patterns.get('device_count', 0)
        device_consistent = patterns.get('device_consistent', False)
        
        # No Token NFC history
        if token_nfc_count == 0:
            risk_factors.append("No previous Token NFC transaction history")
            return {
                'level': 'HIGH',
                'risk_factors': risk_factors
            }
        
        # Low Token NFC adoption
        if token_nfc_rate < 0.05 and token_nfc_count >= 2:  # Less than 5% usage
            risk_factors.append(f"Low Token NFC adoption rate: {token_nfc_rate:.1%}")
        
        # New device usage
        if not device_consistent and device_count > 0:
            risk_factors.append("Transaction from new/unfamiliar device")
        
        # Multiple device usage (potential security concern)
        if device_count > 3:
            risk_factors.append(f"High number of devices used for Token NFC: {device_count}")
        
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
            "device_id": {"type": "string", "description": "Device identifier for Token NFC"}
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "is_token_nfc": {"type": "boolean"},
            "token_nfc_count": {"type": "integer"},
            "token_nfc_rate": {"type": "number"},
            "device_count": {"type": "integer"},
            "device_consistent": {"type": "boolean"},
            "risk_level": {"type": "string"},
            "risk_factors": {"type": "array"}
        }
