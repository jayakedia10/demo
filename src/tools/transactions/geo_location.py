# Importing Dependencies
import pandas as pd
import math
from typing import Any, Dict, List
from datetime import datetime

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class GeoLocationTransactions(BaseTool):
    """Calculate travel feasibility between current and previous card-present transaction locations."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Geo Location Travel Analysis Tool",
            description="Calculate travel feasibility between current and previous card-present transaction locations.",
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
            self._logger.error(f"Failed to initialize geo location tool: {e}")
            return False
    
    async def execute(self, customer_id: str, lattitude: float, 
                     longitude: float, transaction_timestamp: str) -> ToolResult:
        """
        Execute geo location travel feasibility check
        
        TASK: "Calculate travel feasibility between current and previous card-present transaction locations"
        
        SIMPLE SCOPE:
        1. Find recent card-present transactions with geographic data
        2. Calculate distance and travel time between locations
        3. Assess travel feasibility based on time differences
        4. Return simple feasibility assessment
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Get previous card-present transactions with geo data
            previous_geo_transactions = self._get_previous_geo_transactions(
                self.user_transactions, transaction_timestamp
            )
            
            if not previous_geo_transactions:
                return ToolResult(
                    tool_name=self.name,
                    success=True,
                    result={
                        "check_type": "geo_location",
                        "customer_id": customer_id,
                        "has_previous_geo_data": False,
                        "risk_level": "LOW",
                        "assessment": "No previous card-present transactions with location data"
                    }
                )
            
            # Analyze travel feasibility
            feasibility_analysis = self._analyze_travel_feasibility(
                previous_geo_transactions, lattitude, longitude, transaction_timestamp
            )
            
            # Simple risk assessment
            risk_assessment = self._assess_geo_risk(feasibility_analysis)
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "geo_location",
                    "customer_id": customer_id,
                    "has_previous_geo_data": True,
                    "previous_transactions_checked": feasibility_analysis['transactions_checked'],
                    "impossible_travel_detected": feasibility_analysis['impossible_travel'],
                    "min_travel_feasibility": feasibility_analysis['min_feasibility'],
                    "risk_level": risk_assessment['level'],
                    "risk_factors": risk_assessment['risk_factors']
                }
            )
            
        except Exception as e:
            raise e
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Geo location analysis failed: {str(e)}"
            )
    
    def _get_previous_geo_transactions(self, transactions: List[Dict], 
                                     current_time: str) -> List[Dict]:
        """
        Get recent card-present transactions with geographic data
        """
        current_dt = datetime.fromisoformat(str(current_time).replace('Z', '+00:00'))
        
        # Filter for card-present transactions with latitude/longitude data
        geo_transactions = []
        for tx in transactions:
            # Check if transaction has geo data and is card-present
            if (tx.get('latitude') is not None and 
                tx.get('longitude') is not None and
                tx.get('payment_method') in ['Card Present', 'Contactless']):
                
                tx_time = datetime.fromisoformat(str(tx['transaction_date']))
                if tx_time < current_dt:  # Only previous transactions
                    geo_transactions.append(tx)
        
        # Sort by transaction time (most recent first) and return last 5
        geo_transactions.sort(key=lambda x: datetime.fromisoformat(str(x['transaction_date'])), reverse=True)
        return geo_transactions[:5]
    
    def _analyze_travel_feasibility(self, previous_transactions: List[Dict], 
                                  current_lat: float, current_lon: float, 
                                  current_time: str) -> Dict[str, Any]:
        """
        Analyze travel feasibility between locations
        """
        current_dt = datetime.fromisoformat(str(current_time).replace('Z', '+00:00'))
        feasibility_results = []
        impossible_travel = False
        
        for tx in previous_transactions:
            prev_lat = float(tx['latitude'])
            prev_lon = float(tx['longitude'])
            prev_time = datetime.fromisoformat(str(tx['transaction_date']))
            
            # Calculate distance using Haversine formula
            distance_km = self._haversine_distance(prev_lat, prev_lon, current_lat, current_lon)
            
            # Calculate time difference in hours
            time_diff_hours = (current_dt - prev_time).total_seconds() / 3600
            
            # Estimate minimum travel time (assuming 60 km/h average speed)
            min_travel_time_hours = distance_km / 60
            
            # Determine feasibility
            is_feasible = time_diff_hours >= min_travel_time_hours
            
            feasibility_results.append({
                'distance_km': round(distance_km, 2),
                'time_diff_hours': round(time_diff_hours, 2),
                'min_travel_time_hours': round(min_travel_time_hours, 2),
                'is_feasible': is_feasible,
                'transaction_id': tx.get('transaction_id')
            })
            
            # Check for impossible travel
            if not is_feasible and distance_km > 10:  # Only flag if significant distance
                impossible_travel = True
        
        # Find minimum feasibility ratio
        if feasibility_results:
            feasibility_ratios = [
                result['time_diff_hours'] / result['min_travel_time_hours'] 
                if result['min_travel_time_hours'] > 0 else float('inf')
                for result in feasibility_results
            ]
            min_feasibility = min(feasibility_ratios)
        else:
            min_feasibility = 1.0
        
        return {
            'transactions_checked': len(feasibility_results),
            'impossible_travel': impossible_travel,
            'min_feasibility': round(min_feasibility, 2),
            'feasibility_details': feasibility_results
        }
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate the great circle distance between two points on earth (in kilometers)
        """
        try:
            # Convert inputs to float and then to radians
            lat1 = math.radians(float(lat1))
            lon1 = math.radians(float(lon1))
            lat2 = math.radians(float(lat2))
            lon2 = math.radians(float(lon2))
            
            # Haversine formula
            dlat = lat2 - lat1
            dlon = lon2 - lon1
            a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
            c = 2 * math.asin(math.sqrt(a))
            r = 6371  # Radius of earth in kilometers
            return c * r
        except (ValueError, TypeError) as e:
            self._logger.error(f"Error calculating Haversine distance: {e}")
            return float('inf')  # Return infinity for invalid inputs
    
    def _assess_geo_risk(self, feasibility_analysis: Dict) -> Dict[str, Any]:
        """
        Simple risk assessment for geographic travel feasibility
        """
        risk_factors = []
        
        impossible_travel = feasibility_analysis.get('impossible_travel', False)
        min_feasibility = feasibility_analysis.get('min_feasibility', 1.0)
        transactions_checked = feasibility_analysis.get('transactions_checked', 0)
        
        # Impossible travel detected
        if impossible_travel:
            risk_factors.append("Impossible travel time detected between transaction locations")
            return {
                'level': 'HIGH',
                'risk_factors': risk_factors
            }
        
        # Very tight travel timing
        if min_feasibility < 0.8 and transactions_checked > 0:
            risk_factors.append(f"Very tight travel timing detected (feasibility: {min_feasibility:.1f})")
        
        # Tight travel timing
        elif min_feasibility < 1.2 and transactions_checked > 0:
            risk_factors.append(f"Tight travel timing detected (feasibility: {min_feasibility:.1f})")
        
        # Determine risk level
        if min_feasibility < 0.5:
            risk_level = 'HIGH'
        elif min_feasibility < 1.0:
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
            "lattitude": {"type": "number", "description": "Current transaction latitude"},
            "longitude": {"type": "number", "description": "Current transaction longitude"},
            "transaction_timestamp": {"type": "string", "description": "Current transaction timestamp"}
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "customer_id": {"type": "string"},
            "has_previous_geo_data": {"type": "boolean"},
            "previous_transactions_checked": {"type": "integer"},
            "impossible_travel_detected": {"type": "boolean"},
            "min_travel_feasibility": {"type": "number"},
            "risk_level": {"type": "string"},
            "risk_factors": {"type": "array"}
        }
