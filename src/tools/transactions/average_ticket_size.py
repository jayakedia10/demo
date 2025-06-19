# Importing Dependencies
import statistics
import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, timedelta

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class AverageTicketSizeTransactions(BaseTool):
    """Analyzes current transaction amount against user's average ticket size for specific merchant/category."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Average Ticket Size Analysis Tool",
            description="Calculate and compare current transaction amount against user's average for that merchant/category.",
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
            self._logger.error(f"Failed to initialize average ticket size tool: {e}")
            return False
    
    async def execute(self, customer_id: str, transaction_amount: float, 
                     merchant_id: str, category: str, lookback_days: int) -> ToolResult:
        """
        Execute average ticket size analysis
        
        TASK: "Calculate and compare current transaction amount against user's 
        average for that merchant/category"
        
        UNIQUE SCOPE:
        1. Merchant-specific average ticket size calculation
        2. Category-specific average ticket size calculation  
        3. Statistical deviation analysis (mean, std dev, z-score)
        4. Deviation significance assessment
        """
        try:
            # Initialize with customer data
            await self.initialize(customer_id=customer_id)
            
            # Filter transactions by lookback period
            cutoff_date = datetime.now() - timedelta(days=lookback_days)
            filtered_transactions = [
                tx for tx in self.user_transactions 
                if datetime.fromisoformat(str(tx['transaction_date']).replace('Z', '+00:00')) >= cutoff_date
            ]
            
            # Step 1: Calculate merchant-specific average ticket size
            merchant_analysis = self._analyze_merchant_ticket_size(
                filtered_transactions, transaction_amount, merchant_id
            )
            
            # Step 2: Calculate category-specific average ticket size  
            category_analysis = self._analyze_category_ticket_size(
                filtered_transactions, transaction_amount, category
            )
            
            # Step 3: Perform deviation significance assessment
            deviation_analysis = self._assess_deviation_significance(
                merchant_analysis, category_analysis, transaction_amount
            )
            
            # Step 4: Assess ticket size risk
            risk_assessment = self._assess_ticket_size_risk(
                merchant_analysis, category_analysis, deviation_analysis
            )
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "average_ticket_size_analysis",
                    "customer_id": customer_id,
                    "transaction_amount": transaction_amount,
                    "merchant_analysis": merchant_analysis,
                    "category_analysis": category_analysis, 
                    "deviation_analysis": deviation_analysis,
                    "risk_assessment": risk_assessment['level'],
                    "risk_score": risk_assessment['score'],
                    "ticket_size_factors": risk_assessment['ticket_size_factors']
                }
            )
            
        except Exception as e:
            self._logger("Analysis Failed: ", str(e))
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Average ticket size analysis failed: {str(e)}"
            )
    
    def _analyze_merchant_ticket_size(self, transactions: List[Dict], 
                                    transaction_amount: float, merchant_id: str) -> Dict[str, Any]:
        """
        Calculate merchant-specific average ticket size with statistical analysis
        
        Statistical Formulas:
        - Average Ticket Size: ATS_m = Σ(merchant_amounts) / count(merchant_transactions)
        - Standard Deviation: σ_m = √(Σ(amount - ATS_m)² / n)
        - Z-Score: z_m = (transaction_amount - ATS_m) / σ_m
        - Coefficient of Variation: CV_m = σ_m / ATS_m
        """
        if not transactions:
            return {
                'status': 'NO_MERCHANT_HISTORY',
                'merchant_id': merchant_id,
                'transaction_count': 0,
                'average_ticket_size': 0.0
            }
        
        # Filter transactions for specific merchant
        merchant_transactions = [
            tx for tx in transactions 
            if tx.get('merchant_id') == merchant_id
        ]
        
        if not merchant_transactions:
            return {
                'status': 'NO_MERCHANT_HISTORY', 
                'merchant_id': merchant_id,
                'transaction_count': 0,
                'average_ticket_size': 0.0
            }
        
        # Extract amounts for statistical analysis
        amounts = [float(tx['amount']) for tx in merchant_transactions]
        n = len(amounts)
        
        # Calculate merchant-specific statistical metrics
        average_ticket_size = statistics.mean(amounts)  # ATS_m
        std_dev = statistics.stdev(amounts) if n > 1 else 0.0  # σ_m
        median_ticket_size = statistics.median(amounts)
        min_amount = min(amounts)
        max_amount = max(amounts)
        
        # Calculate coefficient of variation
        coefficient_variation = std_dev / average_ticket_size if average_ticket_size > 0 else 0.0
        
        # Calculate current transaction metrics
        z_score = (transaction_amount - average_ticket_size) / std_dev if std_dev > 0 else 0.0
        deviation_percent = ((transaction_amount - average_ticket_size) / average_ticket_size * 100) if average_ticket_size > 0 else 0.0
        
        # Determine if current amount is typical for this merchant
        is_typical_amount = abs(z_score) <= 1.5
        
        return {
            'status': 'ANALYZED',
            'merchant_id': merchant_id,
            'transaction_count': n,
            'statistical_metrics': {
                'average_ticket_size': round(average_ticket_size, 2),
                'median_ticket_size': round(median_ticket_size, 2),
                'std_dev': round(std_dev, 2),
                'min_amount': min_amount,
                'max_amount': max_amount,
                'coefficient_variation': round(coefficient_variation, 3)
            },
            'current_amount_metrics': {
                'z_score': round(z_score, 3),
                'deviation_percent': round(deviation_percent, 1),
                'is_typical_amount': is_typical_amount,
                'is_above_average': transaction_amount > average_ticket_size
            }
        }
    
    def _analyze_category_ticket_size(self, transactions: List[Dict],
                                    transaction_amount: float, category: str) -> Dict[str, Any]:
        """
        Calculate category-specific average ticket size with statistical analysis
        
        Statistical Formulas:
        - Average Ticket Size: ATS_c = Σ(category_amounts) / count(category_transactions)  
        - Standard Deviation: σ_c = √(Σ(amount - ATS_c)² / n)
        - Z-Score: z_c = (transaction_amount - ATS_c) / σ_c
        - Percentile Rank: P_c = (count(amounts ≤ current) / total) * 100
        """
        if not transactions:
            return {
                'status': 'NO_CATEGORY_HISTORY',
                'category': category,
                'transaction_count': 0,
                'average_ticket_size': 0.0
            }
        
        # Filter transactions for specific category
        category_transactions = [
            tx for tx in transactions
            if tx.get('category') == category
        ]
        
        if not category_transactions:
            return {
                'status': 'NO_CATEGORY_HISTORY',
                'category': category, 
                'transaction_count': 0,
                'average_ticket_size': 0.0
            }
        
        # Extract amounts for statistical analysis
        amounts = [float(tx['amount']) for tx in category_transactions]
        n = len(amounts)
        
        # Calculate category-specific statistical metrics
        average_ticket_size = statistics.mean(amounts)  # ATS_c
        std_dev = statistics.stdev(amounts) if n > 1 else 0.0  # σ_c
        median_ticket_size = statistics.median(amounts)
        
        # Calculate percentile rank
        percentile_rank = sum(1 for amt in amounts if amt <= transaction_amount) / n * 100
        
        # Calculate current transaction metrics
        z_score = (transaction_amount - average_ticket_size) / std_dev if std_dev > 0 else 0.0
        deviation_percent = ((transaction_amount - average_ticket_size) / average_ticket_size * 100) if average_ticket_size > 0 else 0.0
        
        # Determine if current amount is typical for this category
        is_typical_amount = abs(z_score) <= 1.5
        
        return {
            'status': 'ANALYZED',
            'category': category,
            'transaction_count': n,
            'statistical_metrics': {
                'average_ticket_size': round(average_ticket_size, 2),
                'median_ticket_size': round(median_ticket_size, 2),
                'std_dev': round(std_dev, 2),
                'min_amount': min(amounts),
                'max_amount': max(amounts)
            },
            'current_amount_metrics': {
                'z_score': round(z_score, 3),
                'deviation_percent': round(deviation_percent, 1),
                'percentile_rank': round(percentile_rank, 1),
                'is_typical_amount': is_typical_amount,
                'is_above_average': transaction_amount > average_ticket_size
            }
        }
    
    def _assess_deviation_significance(self, merchant_analysis: Dict, 
                                     category_analysis: Dict, transaction_amount: float) -> Dict[str, Any]:
        """
        Assess significance of deviation from average ticket sizes
        
        Significance Assessment:
        1. Strong Deviation: |z| > 2.5 for either merchant or category
        2. Moderate Deviation: 1.96 < |z| ≤ 2.5 for either merchant or category  
        3. Weak Deviation: 1.0 < |z| ≤ 1.96 for either merchant or category
        4. Normal: |z| ≤ 1.0 for both merchant and category
        """
        deviation_indicators = []
        significance_score = 0.0
        
        # Analyze merchant deviation significance
        if merchant_analysis.get('status') == 'ANALYZED':
            merchant_z = merchant_analysis.get('current_amount_metrics', {}).get('z_score', 0)
            merchant_deviation = merchant_analysis.get('current_amount_metrics', {}).get('deviation_percent', 0)
            
            if abs(merchant_z) > 2.5:
                deviation_indicators.append(f'Strong deviation from merchant average ticket size (Z-score: {merchant_z:.2f})')
                significance_score += 0.8
            elif abs(merchant_z) > 1.96:
                deviation_indicators.append(f'Moderate deviation from merchant average ticket size (Z-score: {merchant_z:.2f})')
                significance_score += 0.5
            elif abs(merchant_z) > 1.0:
                deviation_indicators.append(f'Weak deviation from merchant average ticket size (Z-score: {merchant_z:.2f})')
                significance_score += 0.3
            
            # Check for extreme percentage deviations
            if abs(merchant_deviation) > 300:  # 3x typical amount
                deviation_indicators.append(f'Extreme deviation: {merchant_deviation:.0f}% from merchant typical')
                significance_score += 0.6
        
        # Analyze category deviation significance  
        if category_analysis.get('status') == 'ANALYZED':
            category_z = category_analysis.get('current_amount_metrics', {}).get('z_score', 0)
            category_percentile = category_analysis.get('current_amount_metrics', {}).get('percentile_rank', 50)
            
            if abs(category_z) > 2.5:
                deviation_indicators.append(f'Strong deviation from category average ticket size (Z-score: {category_z:.2f})')
                significance_score += 0.6
            elif abs(category_z) > 1.96:
                deviation_indicators.append(f'Moderate deviation from category average ticket size (Z-score: {category_z:.2f})')
                significance_score += 0.4
            elif abs(category_z) > 1.0:
                deviation_indicators.append(f'Weak deviation from category average ticket size (Z-score: {category_z:.2f})')
                significance_score += 0.2
            
            # Check percentile extremes
            if category_percentile > 95:
                deviation_indicators.append('Amount in top 5% for this category')
                significance_score += 0.4
            elif category_percentile < 5:
                deviation_indicators.append('Amount in bottom 5% for this category')
                significance_score += 0.3
        
        # Determine overall significance level
        if significance_score >= 0.8:
            significance_level = 'STRONG'
        elif significance_score >= 0.5:
            significance_level = 'MODERATE'
        elif significance_score >= 0.3:
            significance_level = 'WEAK'
        else:
            significance_level = 'NORMAL'
        
        return {
            'significance_level': significance_level,
            'significance_score': round(significance_score, 3),
            'deviation_indicators': deviation_indicators,
            'merchant_deviation_significant': merchant_analysis.get('status') == 'ANALYZED' and 
                                           abs(merchant_analysis.get('current_amount_metrics', {}).get('z_score', 0)) > 1.96,
            'category_deviation_significant': category_analysis.get('status') == 'ANALYZED' and
                                           abs(category_analysis.get('current_amount_metrics', {}).get('z_score', 0)) > 1.96
        }
    
    def _assess_ticket_size_risk(self, merchant_analysis: Dict, category_analysis: Dict, 
                               deviation_analysis: Dict) -> Dict[str, Any]:
        """
        Assess risk based on average ticket size analysis
        
        Risk Formula:
        Risk Score = (significance_score * 0.7) + (consistency_factor * 0.3)
        
        Where consistency_factor accounts for agreement between merchant and category analysis
        
        Risk Levels:
        - HIGH (0.7-1.0): Strong deviations from typical ticket sizes
        - MEDIUM (0.4-0.69): Moderate deviations requiring attention  
        - LOW (0.0-0.39): Normal ticket sizes within expected ranges
        """
        risk_score = 0.0
        ticket_size_factors = []
        
        # Base risk from deviation significance
        significance_score = deviation_analysis.get('significance_score', 0.0)
        significance_level = deviation_analysis.get('significance_level', 'NORMAL')
        
        risk_score += significance_score * 0.7
        ticket_size_factors.extend(deviation_analysis.get('deviation_indicators', []))
        
        # Consistency factor - check if merchant and category analysis agree
        merchant_above_avg = merchant_analysis.get('current_amount_metrics', {}).get('is_above_average', False)
        category_above_avg = category_analysis.get('current_amount_metrics', {}).get('is_above_average', False)
        
        if merchant_analysis.get('status') == 'ANALYZED' and category_analysis.get('status') == 'ANALYZED':
            if merchant_above_avg == category_above_avg:
                # Consistent direction - either both above or both below average
                consistency_factor = 0.3
                if merchant_above_avg:
                    ticket_size_factors.append('Amount above average for both merchant and category')
                else:
                    ticket_size_factors.append('Amount below average for both merchant and category')
            else:
                # Inconsistent - above average for one, below for another
                consistency_factor = 0.1
                ticket_size_factors.append('Inconsistent: above average for one, below for another')
        else:
            # Only one analysis available
            consistency_factor = 0.2
        
        risk_score += consistency_factor * 0.3
        
        # Additional risk factors
        if merchant_analysis.get('status') == 'NO_MERCHANT_HISTORY':
            risk_score += 0.2
            ticket_size_factors.append('No merchant history for ticket size comparison')
        
        if category_analysis.get('status') == 'NO_CATEGORY_HISTORY':
            risk_score += 0.1
            ticket_size_factors.append('No category history for ticket size comparison')
        
        # Normalize risk score
        risk_score = min(risk_score, 1.0)
        
        # Determine risk level
        if risk_score >= 0.7:
            risk_level = 'HIGH'
        elif risk_score >= 0.4:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        # Override for strong deviations
        if significance_level == 'STRONG':
            risk_level = 'HIGH'
            risk_score = max(risk_score, 0.8)
        
        return {
            'level': risk_level,
            'score': round(risk_score, 3),
            'ticket_size_factors': ticket_size_factors
        }
    
    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)
    
    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "merchant_id": {"type": "string", "description": "Merchant identifier for ticket size analysis"},
            "transaction_amount": {"type": "number", "description": "Current transaction amount"},
            "category": {"type": "string", "description": "Transaction category for ticket size analysis"},
            "lookback_days": {"type": "integer", "description": "Days to analyze historical ticket sizes"},
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "transaction_amount": {"type": "number"},
            "merchant_analysis": {"type": "object"},
            "category_analysis": {"type": "object"},
            "deviation_analysis": {"type": "object"},
            "risk_assessment": {"type": "string"},
            "risk_score": {"type": "number"},
            "ticket_size_factors": {"type": "array"}
        }
