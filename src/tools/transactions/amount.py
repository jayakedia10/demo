# Importing Dependencies
import statistics
import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, timedelta

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class AmountTransactions(BaseTool):
    """Analyzes if the transaction amount is an outlier for the customer."""

    def __init__(self, transaction_data: pd.DataFrame):
        super().__init__(
            name="Amount Analysis Tool",
            description="Analyze current transaction amount against historical norms (overall and specific).",
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
            self._logger.error(f"Failed to initialize amount tool: {e}")
            return False
    
    async def execute(self, customer_id: str, transaction_amount: float, 
                     category: str, merchant_id: str, lookback_days: int) -> ToolResult:
        """
        Execute amount analysis
        
        TASK: "Analyze current transaction amount against historical norms 
        (overall and specific)"
        
        UNIQUE SCOPE:
        1. Statistical analysis of transaction amounts (mean, median, std dev, percentiles)
        2. Overall amount pattern analysis with lookback period
        3. Merchant-specific amount analysis
        4. Category-specific amount analysis
        5. Outlier detection and contextualization
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
            
            # Step 1: Analyze overall amount patterns
            overall_analysis = self._analyze_overall_amounts(
                filtered_transactions, transaction_amount
            )
            
            # Step 2: Analyze merchant-specific amounts
            merchant_analysis = self._analyze_merchant_amounts(
                filtered_transactions, transaction_amount, merchant_id
            )
            
            # Step 3: Analyze category-specific amounts
            category_analysis = self._analyze_category_amounts(
                filtered_transactions, transaction_amount, category
            )
            
            # Step 4: Perform outlier detection
            outlier_analysis = self._detect_amount_outlier(
                overall_analysis, merchant_analysis, category_analysis, transaction_amount
            )
            
            # Step 5: Assess amount risk
            risk_assessment = self._assess_amount_risk(
                overall_analysis, merchant_analysis, category_analysis, outlier_analysis
            )
            
            return ToolResult(
                tool_name=self.name,
                success=True,
                result={
                    "check_type": "amount_analysis",
                    "customer_id": customer_id,
                    "transaction_amount": transaction_amount,
                    "overall_analysis": overall_analysis,
                    "merchant_analysis": merchant_analysis,
                    "category_analysis": category_analysis,
                    "outlier_analysis": outlier_analysis,
                    "risk_assessment": risk_assessment['level'],
                    "risk_score": risk_assessment['score'],
                    "amount_factors": risk_assessment['amount_factors'],
                }
            )
            
        except Exception as e:
            self._logger("Analysis Failed: ", str(e))
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={},
                error=f"Amount analysis failed: {str(e)}"
            )
    
    def _analyze_overall_amounts(self, transactions: List[Dict], 
                               transaction_amount: float) -> Dict[str, Any]:
        """
        Analyze overall transaction amounts with statistical methods
        
        Statistical Formulas:
        - Mean: μ = Σ(amounts) / n
        - Standard Deviation: σ = √(Σ(x - μ)² / n)
        - Z-Score: z = (transaction_amount - μ) / σ
        - Percentile Rank: P = (count(amounts ≤ current) / total) * 100
        """
        if not transactions:
            return {
                'status': 'NO_HISTORY',
                'transaction_count': 0,
                'statistical_metrics': {}
            }
        
        amounts = [float(tx['amount']) for tx in transactions]
        n = len(amounts)
        
        # Calculate statistical metrics
        mean = statistics.mean(amounts)  # μ
        median = statistics.median(amounts)
        std_dev = statistics.stdev(amounts) if n > 1 else 0.0  # σ
        min_amount = min(amounts)
        max_amount = max(amounts)
        
        # Calculate percentiles
        sorted_amounts = sorted(amounts)
        percentiles = {
            'p25': sorted_amounts[int(n * 0.25)],
            'p50': median,
            'p75': sorted_amounts[int(n * 0.75)],
            'p90': sorted_amounts[int(n * 0.90)],
            'p95': sorted_amounts[int(n * 0.95)]
        }
        
        # Calculate current amount metrics
        z_score = (transaction_amount - mean) / std_dev if std_dev > 0 else 0.0
        percentile_rank = sum(1 for amt in amounts if amt <= transaction_amount) / n * 100
        
        # Deviation from mean (percentage)
        deviation_percent = ((transaction_amount - mean) / mean * 100) if mean > 0 else 0.0
        
        return {
            'status': 'ANALYZED',
            'transaction_count': n,
            'statistical_metrics': {
                'mean': round(mean, 2),
                'median': round(median, 2),
                'std_dev': round(std_dev, 2),
                'min': min_amount,
                'max': max_amount,
                'coefficient_of_variation': round(std_dev / mean, 3) if mean > 0 else 0.0
            },
            'percentiles': {k: round(v, 2) for k, v in percentiles.items()},
            'transaction_amount_metrics': {
                'z_score': round(z_score, 3),
                'percentile_rank': round(percentile_rank, 1),
                'deviation_percent': round(deviation_percent, 1),
                'is_above_mean': transaction_amount > mean,
                'is_above_median': transaction_amount > median
            }
        }
    
    def _analyze_merchant_amounts(self, transactions: List[Dict], 
                                transaction_amount: float, merchant_id: str) -> Dict[str, Any]:
        """
        Analyze amounts specific to the merchant
        
        Merchant-Specific Analysis:
        - Filter transactions by merchant_id
        - Calculate merchant-specific statistical metrics
        - Compare current amount against merchant norms
        """
        if not transactions:
            return {
                'status': 'NO_MERCHANT_HISTORY',
                'merchant_id': merchant_id,
                'transaction_count': 0
            }
        
        # Filter by merchant
        merchant_transactions = [
            tx for tx in transactions 
            if tx.get('merchant_id') == merchant_id
        ]
        
        if not merchant_transactions:
            return {
                'status': 'NO_MERCHANT_HISTORY',
                'merchant_id': merchant_id,
                'transaction_count': 0
            }
        
        amounts = [float(tx['amount']) for tx in merchant_transactions]
        n = len(amounts)
        
        # Calculate merchant-specific statistics
        mean = statistics.mean(amounts)
        median = statistics.median(amounts)
        std_dev = statistics.stdev(amounts) if n > 1 else 0.0
        
        # Merchant-specific metrics for current amount
        z_score = (transaction_amount - mean) / std_dev if std_dev > 0 else 0.0
        percentile_rank = sum(1 for amt in amounts if amt <= transaction_amount) / n * 100
        deviation_percent = ((transaction_amount - mean) / mean * 100) if mean > 0 else 0.0
        
        return {
            'status': 'ANALYZED',
            'merchant_id': merchant_id,
            'transaction_count': n,
            'statistical_metrics': {
                'mean': round(mean, 2),
                'median': round(median, 2),
                'std_dev': round(std_dev, 2),
                'min': min(amounts),
                'max': max(amounts)
            },
            'transaction_amount_metrics': {
                'z_score': round(z_score, 3),
                'percentile_rank': round(percentile_rank, 1),
                'deviation_percent': round(deviation_percent, 1),
                'is_typical_for_category': abs(z_score) <= 1.5
            }
        }

    def _analyze_category_amounts(self, transactions: List[Dict], 
                                current_amount: float, category: str) -> Dict[str, Any]:
        """
        Analyze amounts specific to the transaction category
        
        Category-Specific Analysis:
        - Filter transactions by category
        - Calculate category-specific statistical metrics
        - Compare current amount against category norms
        """
        if not transactions:
            return {
                'status': 'NO_CATEGORY_HISTORY',
                'category': category,
                'transaction_count': 0
            }
        
        # Filter by category
        category_transactions = [
            tx for tx in transactions 
            if tx.get('category') == category
        ]
        
        if not category_transactions:
            return {
                'status': 'NO_CATEGORY_HISTORY',
                'category': category,
                'transaction_count': 0
            }
        
        amounts = [float(tx['amount']) for tx in category_transactions]
        n = len(amounts)
        
        # Calculate category-specific statistics
        mean = statistics.mean(amounts)
        median = statistics.median(amounts)
        std_dev = statistics.stdev(amounts) if n > 1 else 0.0
        
        # Category-specific metrics for current amount
        z_score = (current_amount - mean) / std_dev if std_dev > 0 else 0.0
        percentile_rank = sum(1 for amt in amounts if amt <= current_amount) / n * 100
        deviation_percent = ((current_amount - mean) / mean * 100) if mean > 0 else 0.0
        
        return {
            'status': 'ANALYZED',
            'category': category,
            'transaction_count': n,
            'statistical_metrics': {
                'mean': round(mean, 2),
                'median': round(median, 2),
                'std_dev': round(std_dev, 2),
                'min': min(amounts),
                'max': max(amounts)
            },
            'current_amount_metrics': {
                'z_score': round(z_score, 3),
                'percentile_rank': round(percentile_rank, 1),
                'deviation_percent': round(deviation_percent, 1),
                'is_typical_for_category': abs(z_score) <= 1.5
            }
        }

    def _detect_amount_outlier(self, overall_analysis: Dict, merchant_analysis: Dict, 
                             category_analysis: Dict, transaction_amount: float) -> Dict[str, Any]:
        """
        Detect if current amount is an outlier using multiple methods
        
        Outlier Detection Methods:
        1. Z-Score Method: |z| > 2.5 (strong outlier), |z| > 1.96 (moderate outlier)
        2. IQR Method: amount < Q1 - 1.5*IQR or amount > Q3 + 1.5*IQR
        3. Percentile Method: amount > P95 (high outlier) or amount < P5 (low outlier)
        """
        outlier_indicators = []
        outlier_score = 0.0
        
        # Check overall outlier status
        if overall_analysis.get('status') == 'ANALYZED':
            overall_metrics = overall_analysis.get('transaction_amount_metrics', {})
            z_score = overall_metrics.get('z_score', 0)
            percentile = overall_metrics.get('percentile_rank', 50)
            
            # Z-Score outlier detection
            if abs(z_score) > 2.5:
                outlier_indicators.append('Strong statistical outlier (Z-score > 2.5)')
                outlier_score += 0.8
            elif abs(z_score) > 1.96:
                outlier_indicators.append('Moderate statistical outlier (Z-score > 1.96)')
                outlier_score += 0.5
            
            # Percentile outlier detection
            if percentile > 95:
                outlier_indicators.append('Amount in top 5% of historical transactions')
                outlier_score += 0.4
            elif percentile < 5:
                outlier_indicators.append('Amount in bottom 5% of historical transactions')
                outlier_score += 0.3
            
            # IQR outlier detection using percentiles
            percentiles = overall_analysis.get('percentiles', {})
            if percentiles:
                q1 = percentiles.get('p25', 0)
                q3 = percentiles.get('p75', 0)
                iqr = q3 - q1
                
                if transaction_amount > q3 + 1.5 * iqr:
                    outlier_indicators.append('Amount exceeds upper IQR boundary')
                    outlier_score += 0.6
                elif transaction_amount < q1 - 1.5 * iqr:
                    outlier_indicators.append('Amount below lower IQR boundary')
                    outlier_score += 0.4
        
        # Check merchant-specific outlier status
        if merchant_analysis and merchant_analysis.get('status') == 'ANALYZED':
            merchant_metrics = merchant_analysis.get('transaction_amount_metrics', {})
            merchant_z = merchant_metrics.get('z_score', 0)
            merchant_deviation = merchant_metrics.get('deviation_percent', 0)
            
            if abs(merchant_z) > 2.0:
                outlier_indicators.append(f'Outlier for merchant {merchant_analysis["merchant_id"]}')
                outlier_score += 0.6
            
            # Check for significant deviation from merchant's typical amount
            if abs(merchant_deviation) > 200:  # More than 2x typical amount
                outlier_indicators.append(f'Amount {merchant_deviation:.0f}% different from merchant typical')
                outlier_score += 0.4
        
        # Check category-specific outlier status
        if category_analysis and category_analysis.get('status') == 'ANALYZED':
            category_metrics = category_analysis.get('transaction_amount_metrics', {})
            category_z = category_metrics.get('z_score', 0)
            
            if abs(category_z) > 2.0:
                outlier_indicators.append(f'Outlier for {category_analysis["category"]} category')
                outlier_score += 0.5
        
        # Determine outlier level
        if outlier_score >= 0.8:
            outlier_level = 'STRONG'
        elif outlier_score >= 0.4:
            outlier_level = 'MODERATE'
        elif outlier_score >= 0.2:
            outlier_level = 'WEAK'
        else:
            outlier_level = 'NONE'
        
        return {
            'outlier_level': outlier_level,
            'outlier_score': round(outlier_score, 3),
            'outlier_indicators': outlier_indicators,
            'outlier_methods': {
                'z_score_outlier': abs(overall_analysis.get('transaction_amount_metrics', {}).get('z_score', 0)) > 1.96,
                'percentile_outlier': overall_analysis.get('transaction_amount_metrics', {}).get('percentile_rank', 50) > 95,
                'merchant_outlier': merchant_analysis and abs(merchant_analysis.get('transaction_amount_metrics', {}).get('z_score', 0)) > 2.0,
                'category_outlier': category_analysis and abs(category_analysis.get('transaction_amount_metrics', {}).get('z_score', 0)) > 2.0
            }
        }
    
    def _assess_amount_risk(self, overall_analysis: Dict, merchant_analysis: Dict, 
                          category_analysis: Dict, outlier_analysis: Dict) -> Dict[str, Any]:
        """
        Assess risk based on amount analysis
        
        Risk Formula:
        Risk Score = (outlier_score * 0.6) + (deviation_factor * 0.4)
        
        Risk Levels:
        - HIGH (0.7-1.0): Strong outliers or extreme deviations
        - MEDIUM (0.4-0.69): Moderate outliers or deviations
        - LOW (0.0-0.39): Normal amounts within expected ranges
        """
        risk_score = 0.0
        amount_factors = []
        
        # Outlier risk component
        outlier_score = outlier_analysis.get('outlier_score', 0.0)
        outlier_level = outlier_analysis.get('outlier_level', 'NONE')
        
        if outlier_level == 'STRONG':
            risk_score += 0.6
            amount_factors.extend(outlier_analysis.get('outlier_indicators', []))
        elif outlier_level == 'MODERATE':
            risk_score += 0.4
            amount_factors.extend(outlier_analysis.get('outlier_indicators', []))
        elif outlier_level == 'WEAK':
            risk_score += 0.2
            amount_factors.extend(outlier_analysis.get('outlier_indicators', []))
        
        # Deviation risk component
        if overall_analysis.get('status') == 'ANALYZED':
            deviation_percent = abs(overall_analysis.get('transaction_amount_metrics', {}).get('deviation_percent', 0))
            
            if deviation_percent > 500:  # 5x normal amount
                risk_score += 0.4
                amount_factors.append(f'Amount {deviation_percent:.0f}% above normal')
            elif deviation_percent > 200:  # 3x normal amount
                risk_score += 0.3
                amount_factors.append(f'Amount {deviation_percent:.0f}% above normal')
            elif deviation_percent > 100:  # 2x normal amount
                risk_score += 0.2
                amount_factors.append(f'Amount {deviation_percent:.0f}% above normal')
        
        # Merchant-specific risk
        if merchant_analysis and merchant_analysis.get('status') == 'ANALYZED':
            merchant_metrics = merchant_analysis.get('transaction_amount_metrics', {})
            merchant_deviation = abs(merchant_metrics.get('deviation_percent', 0))
            
            if merchant_deviation > 300:  # 3x merchant typical amount
                risk_score += 0.4
                amount_factors.append(f'Amount {merchant_deviation:.0f}% above merchant typical')
            elif merchant_deviation > 200:  # 2x merchant typical amount
                risk_score += 0.3
                amount_factors.append(f'Amount {merchant_deviation:.0f}% above merchant typical')
            
            # Check for unusual transaction frequency
            if merchant_metrics.get('transaction_frequency', 0) > 3:  # More than 3 transactions per day
                risk_score += 0.2
                amount_factors.append(f'High frequency of transactions with merchant {merchant_analysis["merchant_id"]}')
        
        # Category-specific risk
        if category_analysis and category_analysis.get('status') == 'ANALYZED':
            category_metrics = category_analysis.get('transaction_amount_metrics', {})
            if not category_metrics.get('is_typical_for_category', True):
                risk_score += 0.2
                amount_factors.append(f'Unusual amount for {category_analysis["category"]} category')
        
        # Normalize risk score
        risk_score = min(risk_score, 1.0)
        
        # Determine risk level
        if risk_score >= 0.7:
            risk_level = 'HIGH'
        elif risk_score >= 0.4:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'

        # Add specific recommendations based on outlier type
        if outlier_level == 'STRONG':
            risk_level = 'HIGH'
            risk_score = 1.0
        
        return {
            'level': risk_level,
            'score': round(risk_score, 3),
            'amount_factors': amount_factors,
        }

    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs."""
        return all(field in kwargs for field in self.required_fields)

    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "merchant_id": {"type": "string", "description": "Merchant identifier for specific analysis"},
            "transaction_amount": {"type": "number", "description": "Current transaction amount"},
            "category": {"type": "string", "description": "Transaction category for specific analysis"},
            "lookback_days": {"type": "integer", "description": "Days to analyze historical amounts"},
        }
    
    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "check_type": {"type": "string"},
            "transaction_amount": {"type": "number"},
            "overall_analysis": {"type": "object"},
            "category_analysis": {"type": "object"},
            "outlier_analysis": {"type": "object"},
            "risk_assessment": {"type": "string"},
            "risk_score": {"type": "number"},
            "amount_factors": {"type": "array"},
        }