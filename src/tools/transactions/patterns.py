# Importing Dependencies
import pandas as pd
from collections import Counter
from typing import Any, Dict, List
from datetime import datetime, timedelta

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class PatternsTransactions(BaseTool):
    """Analyzes whether the current transaction deviates from the user's normal spending patterns."""

    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Spending Patterns Analysis Tool",
            description="Perform detailed analysis of user's historical spending patterns and compare with current transaction.",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Transactions Data Wrapper"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())

    async def initialize(self, **kwargs) -> bool:
        """Initialize the resources for the tool"""
        try:
            data = self.transaction_data
            self.user_transactions = data[
                (data['customer_id'] == kwargs.get('customer_id'))
            ].to_dict('records')
            return True
        except Exception as e:
            print(f"Failed to initialize data generator: {str(e)}")
            return False

    async def execute(self, customer_id: str, merchant_category: str, 
                     transaction_timestamp: str, lookback_days: int) -> ToolResult:
        """
        Execute spending patterns analysis
        
        TASK: "Perform detailed analysis of user's historical spending patterns 
        and compare with current transaction."
        
        UNIQUE SCOPE:
        1. Category spending preferences and consistency
        2. Time-of-day spending patterns
        3. Day-of-week patterns  
        4. Seasonal spending behavior
        5. Overall pattern consistency assessment
        """
        try:
            # Step: 0: Initialize Tool
            init_success = await self.initialize(customer_id=customer_id)
            
            if not init_success:
                return ToolResult(
                    tool_name=self.name,
                    success=False,
                    result={},
                    error="Failed to initialize tool"
                )
            # Step 1: Analyze spending patterns
            spending_patterns = self._analyze_spending_patterns(
                self.user_transactions, lookback_days
            )
            
            # Step 2: Calculate pattern consistency
            pattern_consistency = self._calculate_pattern_consistency(spending_patterns)
            
            # Step 3: Compare current transaction against patterns
            pattern_match = self._compare_current_transaction(
                spending_patterns, merchant_category, transaction_timestamp
            )
            
            # Step 4: Assess pattern risk
            risk_assessment = self._assess_pattern_risk(
                pattern_consistency, pattern_match
            )
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "spending_patterns_check",
                    "customer_id": customer_id,
                    "merchant_category": merchant_category,
                    "pattern_consistency": pattern_consistency,
                    "current_transaction_match": pattern_match,
                    "risk_assessment": risk_assessment['level'],
                    "risk_score": risk_assessment['score'],
                    "pattern_deviation_factors": risk_assessment['deviation_factors']
                }
            )
            
        except Exception as e:
            self._logger("Analysis Failed: ", str(e))
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Pattern analysis failed: {str(e)}"
            )
    
    def _analyze_spending_patterns(self, transactions: List[Dict], lookback_days: int) -> Dict[str, Any]:
        """
        Analyze historical spending patterns
        
        FOCUS: Behavioral patterns, NOT amounts or frequency
        
        Formulas:
        - Category Preference: P_cat = count(category) / total_transactions
        - Time Preference: P_time = count(time_slot) / total_transactions  
        - Day Preference: P_day = count(day_of_week) / total_transactions
        """
        if not transactions:
            return {
                'category_patterns': {},
                'time_patterns': {},
                'day_patterns': {},
                'seasonal_patterns': {},
                'pattern_strength': 0.0
            }
        
        # Filter transactions within lookback period
        cutoff_date = datetime.now() - timedelta(days=lookback_days)
        relevant_transactions = [
            tx for tx in transactions 
            if datetime.fromisoformat(str(tx['transaction_date'])) >= cutoff_date
        ]
        
        if not relevant_transactions:
            return {
            'category_patterns': {},
            'time_patterns': {},
            'day_patterns': {},
            'seasonal_patterns': {},
            'pattern_strength': 0.0,
            'total_transactions_analyzed': 0
        }
        
        total_transactions = len(relevant_transactions)
        
        # 1. Category Pattern Analysis
        category_counts = Counter(tx['category'] for tx in relevant_transactions)
        category_patterns = {
            cat: round(count / total_transactions, 3) 
            for cat, count in category_counts.items()
        }
        
        # 2. Time-of-Day Pattern Analysis
        time_patterns = self._analyze_time_patterns(relevant_transactions, total_transactions)
        
        # 3. Day-of-Week Pattern Analysis  
        day_patterns = self._analyze_day_patterns(relevant_transactions, total_transactions)
        
        # 4. Seasonal Pattern Analysis
        seasonal_patterns = self._analyze_seasonal_patterns(relevant_transactions, total_transactions)
        
        # 5. Calculate overall pattern strength
        pattern_strength = self._calculate_pattern_strength(
            category_patterns, time_patterns, day_patterns
        )
        
        return {
            'category_patterns': category_patterns,
            'time_patterns': time_patterns,
            'day_patterns': day_patterns,
            'seasonal_patterns': seasonal_patterns,
            'pattern_strength': pattern_strength,
            'total_transactions_analyzed': total_transactions
        }
    
    def _analyze_time_patterns(self, transactions: List[Dict], total: int) -> Dict[str, float]:
        """
        Analyze time-of-day spending patterns
        
        Time Slots:
        - Morning: 6-12
        - Afternoon: 12-18  
        - Evening: 18-24
        - Night: 0-6
        """
        time_counts = {'morning': 0, 'afternoon': 0, 'evening': 0, 'night': 0}
        
        for tx in transactions:
            hour = datetime.fromisoformat(str(tx['transaction_date'])).hour
            
            if 6 <= hour < 12:
                time_counts['morning'] += 1
            elif 12 <= hour < 18:
                time_counts['afternoon'] += 1
            elif 18 <= hour <= 23:
                time_counts['evening'] += 1
            else:  # 0-5
                time_counts['night'] += 1
        
        return {
            slot: round(count / total, 3) 
            for slot, count in time_counts.items()
        }
    
    def _analyze_day_patterns(self, transactions: List[Dict], total: int) -> Dict[str, float]:
        """
        Analyze day-of-week spending patterns
        """
        day_counts = Counter()
        
        for tx in transactions:
            day_name = datetime.fromisoformat(str(tx['transaction_date'])).strftime('%A').lower()
            day_counts[day_name] += 1
        
        return {
            day: round(count / total, 3) 
            for day, count in day_counts.items()
        }
    
    def _analyze_seasonal_patterns(self, transactions: List[Dict], total: int) -> Dict[str, float]:
        """
        Analyze seasonal spending patterns
        """
        seasonal_counts = {'spring': 0, 'summer': 0, 'monsoon': 0, 'winter': 0}
        
        for tx in transactions:
            month = datetime.fromisoformat(str(tx['transaction_date'])).month
            
            # Indian seasons
            if month in [3, 4, 5]:  # March-May
                seasonal_counts['summer'] += 1
            elif month in [6, 7, 8, 9]:  # June-September  
                seasonal_counts['monsoon'] += 1
            elif month in [10, 11]:  # October-November
                seasonal_counts['winter'] += 1
            else:  # December-February
                seasonal_counts['spring'] += 1
        
        return {
            season: round(count / total, 3) 
            for season, count in seasonal_counts.items()
        }
    
    def _calculate_pattern_strength(self, category_patterns: Dict, 
                                  time_patterns: Dict, day_patterns: Dict) -> float:
        """
        Calculate overall pattern strength using entropy-based measure
        
        Formula:
        Pattern Strength = 1 - (H_cat + H_time + H_day) / 3
        
        Where H = -Σ(p * log(p)) for each pattern (normalized entropy)
        Lower entropy = stronger patterns = higher strength
        """
        import math
        
        def calculate_entropy(probabilities: Dict) -> float:
            """Calculate normalized entropy (0-1 scale)"""
            if not probabilities or len(probabilities) <= 1:
                return 0.0
                
            # Calculate entropy
            entropy = -sum(p * math.log2(p) for p in probabilities.values() if p > 0)
            
            # Normalize by max possible entropy
            max_entropy = math.log2(len(probabilities))
            return entropy / max_entropy if max_entropy > 0 else 0.0
        
        # Calculate entropy for each pattern type
        cat_entropy = calculate_entropy(category_patterns)
        time_entropy = calculate_entropy(time_patterns)  
        day_entropy = calculate_entropy(day_patterns)
        
        # Pattern strength = 1 - average entropy
        pattern_strength = 1 - (cat_entropy + time_entropy + day_entropy) / 3
        
        return round(pattern_strength, 3)
    
    def _calculate_pattern_consistency(self, patterns: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculate consistency metrics for spending patterns
        
        Consistency Score = Pattern Strength * Diversity Factor
        Where Diversity Factor accounts for having established patterns
        """
        pattern_strength = patterns.get('pattern_strength', 0.0)
        category_patterns = patterns.get('category_patterns', {})
        
        # Diversity factor (having 2-5 main categories is ideal)
        num_categories = len([cat for cat, prob in category_patterns.items() if prob >= 0.1])
        
        if num_categories == 0:
            diversity_factor = 0.0
        elif 2 <= num_categories <= 5:
            diversity_factor = 1.0
        elif num_categories == 1:
            diversity_factor = 0.7  # Too concentrated
        else:
            diversity_factor = 0.8  # Too scattered
        
        consistency_score = pattern_strength * diversity_factor
        
        # Determine consistency level
        if consistency_score >= 0.7:
            consistency_level = 'HIGH'
        elif consistency_score >= 0.4:
            consistency_level = 'MEDIUM'
        else:
            consistency_level = 'LOW'
        
        return {
            'consistency_level': consistency_level,
            'consistency_score': round(consistency_score, 3),
            'pattern_strength': pattern_strength,
            'diversity_factor': diversity_factor,
            'established_categories': num_categories
        }
    
    def _compare_current_transaction(self, patterns: Dict[str, Any], 
                                   merchant_category: str, 
                                   current_date: str) -> Dict[str, Any]:
        """
        Compare current transaction against established patterns
        
        Match Score = (0.5 * category_match) + (0.3 * time_match) + (0.2 * day_match)
        """
        category_patterns = patterns.get('category_patterns', {})
        time_patterns = patterns.get('time_patterns', {})
        day_patterns = patterns.get('day_patterns', {})
        
        # Category match
        category_probability = category_patterns.get(merchant_category, 0.0)
        
        # Time match
        current_dt = datetime.fromisoformat(str(current_date).replace('Z', '+00:00'))
        current_hour = current_dt.hour
        
        if 6 <= current_hour < 12:
            time_slot = 'morning'
        elif 12 <= current_hour < 18:
            time_slot = 'afternoon'
        elif 18 <= current_hour <= 23:
            time_slot = 'evening'
        else:
            time_slot = 'night'
        
        time_probability = time_patterns.get(time_slot, 0.0)
        
        # Day match
        current_day = current_dt.strftime('%A').lower()
        day_probability = day_patterns.get(current_day, 0.0)
        
        # Overall match score
        match_score = (0.5 * category_probability) + (0.3 * time_probability) + (0.2 * day_probability)
        
        # Determine match level
        if match_score >= 0.3:
            match_level = 'HIGH'
        elif match_score >= 0.15:
            match_level = 'MEDIUM'  
        else:
            match_level = 'LOW'
        
        return {
            'overall_match_score': round(match_score, 3),
            'match_level': match_level,
            'category_match': {
                'category': merchant_category,
                'probability': category_probability,
                'is_common': category_probability >= 0.1
            },
            'time_match': {
                'time_slot': time_slot,
                'probability': time_probability,
                'is_typical': time_probability >= 0.2
            },
            'day_match': {
                'day': current_day,
                'probability': day_probability,
                'is_typical': day_probability >= 0.1
            }
        }
    
    def _assess_pattern_risk(self, consistency: Dict[str, Any], 
                           match: Dict[str, Any]) -> Dict[str, Any]:
        """
        Assess risk based on pattern consistency and current match
        
        Risk Formula:
        Risk Score = (1 - consistency_score) * 0.6 + (1 - match_score) * 0.4
        
        Risk Levels:
        - HIGH (0.6-1.0): Inconsistent patterns or poor match
        - MEDIUM (0.3-0.59): Moderate consistency/match
        - LOW (0.0-0.29): Strong patterns and good match
        """
        consistency_score = consistency.get('consistency_score', 0.0)
        match_score = match.get('overall_match_score', 0.0)
        
        # Calculate risk score
        risk_score = (1 - consistency_score) * 0.6 + (1 - match_score) * 0.4
        risk_score = round(risk_score, 3)
        
        # Determine risk level
        if risk_score >= 0.6:
            risk_level = 'HIGH'
        elif risk_score >= 0.3:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        # Identify deviation factors
        deviation_factors = []
        
        if consistency.get('consistency_level') == 'LOW':
            deviation_factors.append('Weak spending pattern establishment')
        
        if match.get('match_level') == 'LOW':
            deviation_factors.append('Transaction deviates from typical behavior')
        
        if not match.get('category_match', {}).get('is_common', False):
            deviation_factors.append(f"Unusual category: {match.get('category_match', {}).get('category', 'Unknown')}")
        
        if not match.get('time_match', {}).get('is_typical', False):
            deviation_factors.append(f"Unusual time: {match.get('time_match', {}).get('time_slot', 'Unknown')}")
        
        return {
            'level': risk_level,
            'score': risk_score,
            'deviation_factors': deviation_factors
        }

    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)
    
    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "merchant_category": {"type": "string", "description": "Current transaction category"},
            "transaction_timestamp": {"type": "string", "description": "Current transaction datetime"},
            "lookback_days": {"type": "integer", "description": "Days to analyze patterns"}
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "pattern_consistency": {"type": "object"},
            "current_transaction_match": {"type": "object"},
            "risk_assessment": {"type": "string"},
            "risk_score": {"type": "number"}
        }
