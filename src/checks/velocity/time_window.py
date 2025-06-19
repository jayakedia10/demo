# Importing Dependencies
import json
from typing import Optional
from datetime import datetime, timedelta

from ...core.basetools import BaseTool
from ...core.schemas import CheckCategory, CheckResult


class TimeWindowCheck(BaseTool):
    """
    Check if the time window of a given velocity is within the specified range.
    """

    def __init__(self):
        super().__init__(name="Time Window Check", 
                         description="Checks if the time window of a given velocity is within the specified range.",
                         category=CheckCategory.VELOCITY,
                         dependencies=["Historical Transactions"])
        
        # Load configuration
        with open('configs/sample_config.json', 'r') as f:
            config = json.load(f)
        
        velocity_config = config.get("thresholds", {}).get("velocity", {})
        
        # Time windows in minutes for velocity analysis (Requirement 3.1)
        self.time_window_mins = velocity_config.get("time_window_mins", [1, 2, 3, 5, 10, 15, 20, 60, 360, 1440])

        # Velocity thresholds for each time window
        self.velocity_thresholds = velocity_config.get("velocity_per_time_window", {
            "1": 2, "2": 3, "3": 4, "5": 5, "10": 7, "15": 10, 
            "20": 12, "60": 20, "360": 60, "1440": 150
        })

    async def initialize(self, customer_id: str, lookback_days: Optional[int] = None):
        await super().initialize(customer_id, lookback_days)
        self._is_initialized = True
    
    async def validate_inputs(self, customer_id: str, alert_timestamp: str) -> bool:
        """
        Validate the input parameters for the tool.
        """
        try:
            datetime.fromisoformat(alert_timestamp)
            if not isinstance(customer_id, str) or not customer_id:
                raise ValueError("Invalid customer_id")
            return True
        except ValueError:
            return False
        
    async def execute(self, customer_id: str, alert_timestamp: datetime) -> CheckResult:
        ## TODO: VALIDATE INPUTS

        # Initialize historical transactions
        if not self._is_initialized:
            await self.initialize(customer_id)
 
        velocity_violations = []
        window_counts = {}
        
        for window_minutes in self.time_window_mins:
            window_start = alert_timestamp - timedelta(minutes=window_minutes)
            
            # Count transactions in this time window
            window_count = sum(
                1 for transaction in self._historical_transactions 
                if window_start <= transaction['transaction_timestamp'] < alert_timestamp
            )
            
            window_counts[window_minutes] = window_count
            threshold = int(self.velocity_thresholds.get(str(window_minutes), 5))
            
            # Check for velocity violations
            if window_count > threshold:
                # Calculate deviation percentage from threshold
                deviation = (window_count - threshold) / threshold
                
                # Set severity based on deviation ranges (following time_day pattern)
                if deviation >= 0.5:  # 50% or more above threshold
                    severity = 'HIGH'
                elif deviation >= 0.25:  # 25% or more above threshold
                    severity = 'MEDIUM'
                else:  # Less than 25% above threshold
                    severity = 'LOW'
                
                velocity_violations.append({
                    'window_minutes': window_minutes,
                    'count': window_count,
                    'threshold': threshold,
                    'severity': severity,
                    'deviation': round(deviation, 3)
                })
        
        return CheckResult(
                check_name=self.name,
                success=True,
                result="Velocity time window analysis completed",
                description="Analyzed transaction velocity against configured time windows and thresholds.",
                category=self.category,
                analysis={
                    'velocity_violations': velocity_violations,
                    'window_counts': window_counts
                }
            )
    
    def _get_parameter_schema(self) -> dict:
        """
        Define the parameter schema for this tool.
        """
        return {
            "customer_id": {"type": "string", "description": "Customer ID for the transaction"},
            "alert_timestamp": {"type": "string", "format": "date-time", "description": "Timestamp of the alert in ISO format"}
        }
    
    def _get_return_schema(self) -> dict:
        """
        Define the return value schema for this tool.
        """
        return {
            "check_name": {"type": "string", "description": "Name of the check"},
            "success": {"type": "boolean", "description": "Indicates if the check was successful"},
            "result": {"type": "string", "description": "Result of the check execution"},
            "description": {"type": "string", "description": "Description of the check"},
            "category": {"type": "string", "enum": [cat.value for cat in CheckCategory], 
                         "description": "Category of the check"},
            "analysis": {
                "type": "object",
                "properties": {
                    "velocity_violations": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "window_minutes": {"type": "integer"},
                                "count": {"type": "integer"},
                                "threshold": {"type": "integer"},
                                "severity": {"type": "string"},
                                "deviation": {"type": "number"}
                            }
                        }
                    },
                    "window_counts": {
                        "type": "object",
                        # Additional properties can be defined here if needed
                    }
                }
            }
        }