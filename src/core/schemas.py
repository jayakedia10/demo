# Importing Dependencies
from enum import Enum
from datetime import datetime
from dataclasses import dataclass
from pydantic import BaseModel, Field
from typing import Dict, List, Any, Optional


class CheckCategory(Enum):
    """Categories for tool classification"""
    VELOCITY = "velocity"
    # SIMILAR_AMOUNT = "similar_amount"

@dataclass
class CheckResult:
    check_name: str
    success: bool
    result: str
    description: str
    category: CheckCategory
    analysis: Dict
    error: Optional[str] = None

@dataclass
class Alert:
    alert_id: str
    customer_id: str
    transaction_id: str
    merchant_id: str
    transaction_amount: float
    transaction_timestamp: datetime
    merchant_category: str
    merchant_category_code: str
    location: str
    country: str
    currency: str
    payment_method: str
    payment_sub_type: str
    pin_verified: bool
    device_id: Optional[str]
    ip_address: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]

class AgentResult(BaseModel):
    agent_name: str = Field(description="Name of the agent that performed the analysis")
    alert_is_false_positive: bool = Field(description="Whether the alert was determined to be a false positive")
    findings: str = Field(description="Summary of key findings from the analysis")
    detailed_explanation: str = Field(description="Comprehensive explanation of the analysis results")
    confidence_score: float = Field(description="Confidence level in the analysis (0.0 to 1.0)")
    recommendations: List[str] = Field(description="List of recommended actions based on the analysis")