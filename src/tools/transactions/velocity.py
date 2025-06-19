# Importing Dependencies
import json
import pandas as pd
from typing import Any, Dict, List
from datetime import datetime, timedelta
from collections import Counter

from ...core.basetools import BaseTool
from ...core.schemas import ToolCategory, ToolResult

class VelocityTransactions(BaseTool):
    """
    Velocity analysis tool implementing requirements 3.1, 3.2, and 3.3.
    
    Purpose: Detect rapid-fire transaction patterns and multi-dimensional anomalies
    that indicate potential fraud through velocity-based indicators.
    
    Key Logic:
    1. Monitor transaction frequency across multiple time windows (1 min to 24 hrs)
    2. Detect unusual hour activity patterns in recent transactions  
    3. Identify multi-dimensional anomalies across payment channels, devices, locations
    4. Apply 3 fraud scenarios based on velocity patterns and anomaly detection
    """
    
    def __init__(self, transaction_data: pd.DataFrame, avg_time_gap_mins = None):
        super().__init__(
            name="Velocity Analysis Tool",
            description="Analyzes transaction velocity patterns and multi-dimensional anomalies for fraud detection",
            category=ToolCategory.TRANSACTION_ANALYSIS,
            dependencies=["Historical Transactions"]
        )
        self.transaction_data = transaction_data
        self.required_fields = list(self._get_parameter_schema().keys())
        
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
        
        # Time gap threshold for rapid sequence detection (Requirement 3.2)
        self.avg_time_gap_mins = avg_time_gap_mins if avg_time_gap_mins is not None else velocity_config.get("avg_time_gap_mins", 2.0)

        # Unusual hours definition (Requirement 3.2)
        self.unusual_hours = list(range(0, 6)) + list(range(23, 24))  # 11 PM - 6 AM

    async def initialize(self, **kwargs) -> bool:
        """Filter transactions for specific customer"""
        try:
            data = self.transaction_data
            self.user_transactions = data[
                (data['customer_id'] == kwargs.get('customer_id'))
            ].to_dict('records')
            self._is_initialized = True
            return True
        except Exception as e:
            self._logger.error(f"Failed to initialize velocity tool: {e}")
            return False

    async def execute(self, customer_id: str, transaction_timestamp: str) -> ToolResult:
        """Main execution method for velocity analysis"""
        try:
            await self.initialize(customer_id=customer_id)
            alert_time = datetime.fromisoformat(str(transaction_timestamp))
            
            # Step 1: Get historical transactions within 1 day lookback
            historical_transactions = self._get_historical_transactions(alert_time)
            
            # Step 2: Analyze velocity patterns across time windows (Requirement 3.1)
            velocity_analysis = self._analyze_velocity_patterns(historical_transactions, alert_time)
            
            # Step 3: Analyze time gaps and unusual hours (Requirement 3.2)
            time_gap_analysis = self._analyze_time_gaps_and_hours(historical_transactions, alert_time)
            
            # Step 4: Analyze multi-dimensional anomalies (Requirement 3.3)
            anomaly_analysis = self._analyze_multidimensional_anomalies(historical_transactions, alert_time)
            
            # Step 5: Apply velocity fraud scenarios
            scenario_results = self._apply_scenarios(velocity_analysis, time_gap_analysis, anomaly_analysis)
            
            # Step 6: Generate final result
            result = self._generate_result(
                velocity_analysis, time_gap_analysis, anomaly_analysis, 
                scenario_results, len(historical_transactions)
            )
            
            return ToolResult(tool_name=self.name, success=True, result=result)
            
        except Exception as e:
            raise e
            self._logger.error(f"Velocity analysis failed: {str(e)}")
            return ToolResult(
                tool_name=self.name,
                success=False,
                result={
                    "scenario_analysis": [],
                    "overall_assessment": {"result": "Error", "rationale": [f"Analysis failed: {str(e)}"]}
                },
                error=str(e)
            )

    def _get_historical_transactions(self, alert_time: datetime) -> List[Dict]:
        """Get customer's transactions within 1 day lookback period"""
        lookback_start = alert_time - timedelta(days=1)
        relevant_transactions = [
            tx for tx in self.user_transactions 
            if lookback_start <= datetime.fromisoformat(str(tx['transaction_date'])) <= alert_time
        ]
        return sorted(relevant_transactions, key=lambda x: datetime.fromisoformat(str(x['transaction_date'])))

    def _analyze_velocity_patterns(self, transactions: List[Dict], alert_time: datetime) -> Dict:
        """
        Requirement 3.1: Analyze transaction velocity across multiple time windows
        
        Logic:
        1. Count transactions in each time window (1 min to 24 hrs)
        2. Compare against configurable thresholds
        3. Calculate deviation severity for violations
        4. Detect rapid-fire transaction patterns
        """
        velocity_violations = []
        window_counts = {}
        
        for window_minutes in self.time_window_mins:
            window_start = alert_time - timedelta(minutes=window_minutes)
            
            # Count transactions in this time window
            window_count = sum(
                1 for tx in transactions 
                if datetime.fromisoformat(str(tx['transaction_date'])) >= window_start
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
        
        return {
            'has_violations': len(velocity_violations) > 0,
            'violation_count': len(velocity_violations),
            'violations': velocity_violations,
            'max_severity': max([v['severity'] for v in velocity_violations], default='NONE'),
            'window_counts': window_counts
        }

    def _analyze_time_gaps_and_hours(self, transactions: List[Dict], alert_time: datetime) -> Dict:
        """
        Requirement 3.2: Analyze time gaps and unusual hours activity
        
        Logic:
        1. Calculate average time gap between consecutive transactions
        2. Check for rapid sequence violations (gap < threshold)
        3. Detect unusual hours activity in last 10 minutes
        """
        if len(transactions) < 2:
            return {
                'avg_gap_minutes': None,
                'gap_violation': False,
                'unusual_hours_activity': False,
                'last_10_min_transactions': 0
            }
        
        # Calculate average time gaps between consecutive transactions
        sorted_times = [datetime.fromisoformat(str(tx['transaction_date'])) for tx in transactions]
        gaps = [
            (sorted_times[i] - sorted_times[i-1]).total_seconds() / 60 
            for i in range(1, len(sorted_times))
        ]
        avg_gap = sum(gaps) / len(gaps) if gaps else None
        
        # Check for unusual hours activity in last 10 minutes
        last_10_min_start = alert_time - timedelta(minutes=10)
        last_10_min_transactions = [
            tx for tx in transactions 
            if datetime.fromisoformat(str(tx['transaction_date'])) >= last_10_min_start
        ]
        
        # Check if any transactions in last 10 minutes happened during unusual hours
        unusual_hours_activity = any(
            datetime.fromisoformat(str(tx['transaction_date'])).hour in self.unusual_hours
            for tx in last_10_min_transactions
        )
        
        return {
            'avg_gap_minutes': round(avg_gap, 2) if avg_gap else None,
            'gap_violation': avg_gap < self.avg_time_gap_mins if avg_gap else False,
            'unusual_hours_activity': unusual_hours_activity,
            'last_10_min_transactions': len(last_10_min_transactions)
        }

    def _analyze_multidimensional_anomalies(self, transactions: List[Dict], alert_time: datetime) -> Dict:
        """
        Requirement 3.3: Enhanced Multi-dimensional velocity anomaly detection
        
        Instead of just counting diversity, detects specific fraud patterns:
        1. Same merchant + multiple devices/locations (account takeover/cloned cards)
        2. High-value transactions + location/device changes (fraudulent escalation)
        3. Rapid payment method switching (testing different payment methods)
        4. Amount escalation patterns (progressive fraud testing)
        5. Cross-channel abuse (rapid online/physical switching)
        6. MCC switching for same merchant (unusual merchant behavior)
        7. Rapid geographic movement (impossible travel patterns)
        
        Each pattern provides specific fraud context for LLM analysis.
        """
        # Get transactions in last 10 minutes only
        last_10_min_start = alert_time - timedelta(minutes=10)
        # recent_transactions = [
        #     tx for tx in transactions 
        #     if datetime.fromisoformat(str(tx['transaction_date'])) >= last_10_min_start
        # ]
        recent_transactions = transactions
        
        if not recent_transactions:
            return {
                'has_velocity_patterns': False,
                'pattern_count': 0,
                'detected_patterns': {},
                'recent_transaction_count': 0
            }
        
        detected_patterns = {}
        
        # Pattern 1: Same Merchant + Multiple Devices (Account Takeover/Card Testing)
        merchant_device_pattern = self._detect_same_merchant_multiple_devices(recent_transactions)
        if merchant_device_pattern:
            detected_patterns['same_merchant_multiple_devices'] = merchant_device_pattern
        
        # Pattern 2: Same Merchant + Multiple Locations (Cloned Cards/Impossible Travel)
        merchant_location_pattern = self._detect_same_merchant_multiple_locations(recent_transactions)
        if merchant_location_pattern:
            detected_patterns['same_merchant_multiple_locations'] = merchant_location_pattern
        
        # Pattern 3: Same Merchant + Multiple IPs (Network Switching/Proxy Usage)
        merchant_ip_pattern = self._detect_same_merchant_multiple_ips(recent_transactions)
        if merchant_ip_pattern:
            detected_patterns['same_merchant_multiple_ips'] = merchant_ip_pattern
        
        # Pattern 4: High-Value Transactions + Location/Device Changes (Fraudulent Escalation)
        high_value_pattern = self._detect_high_value_location_device_changes(recent_transactions)
        if high_value_pattern:
            detected_patterns['high_value_location_device_changes'] = high_value_pattern
        
        # Pattern 5: Rapid Payment Method Switching (Testing Payment Methods)
        payment_switching_pattern = self._detect_rapid_payment_method_switching(recent_transactions)
        if payment_switching_pattern:
            detected_patterns['rapid_payment_method_switching'] = payment_switching_pattern
        
        # Pattern 6: Amount Escalation Pattern (Progressive Fraud Testing)
        amount_escalation_pattern = self._detect_amount_escalation(recent_transactions)
        if amount_escalation_pattern:
            detected_patterns['amount_escalation_pattern'] = amount_escalation_pattern
        
        # Pattern 7: Cross-Channel Abuse (Rapid Online/Physical Switching)
        cross_channel_pattern = self._detect_cross_channel_abuse(recent_transactions)
        if cross_channel_pattern:
            detected_patterns['cross_channel_abuse'] = cross_channel_pattern
        
        # Pattern 8: MCC Switching for Same Merchant (Unusual Merchant Behavior)
        mcc_switching_pattern = self._detect_mcc_switching_same_merchant(recent_transactions)
        if mcc_switching_pattern:
            detected_patterns['mcc_switching_same_merchant'] = mcc_switching_pattern
        
        # Pattern 9: Rapid Geographic Movement (Impossible Travel)
        geographic_movement_pattern = self._detect_rapid_geographic_movement(recent_transactions)
        if geographic_movement_pattern:
            detected_patterns['rapid_geographic_movement'] = geographic_movement_pattern
        
        return {
            'has_velocity_patterns': len(detected_patterns) > 0,
            'pattern_count': len(detected_patterns),
            'detected_patterns': detected_patterns,
            'recent_transaction_count': len(recent_transactions)
        }

    def _detect_same_merchant_multiple_devices(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 1: Same merchant accessed from multiple devices
        Fraud Context: Account takeover, card testing, or unauthorized access
        """
        from collections import defaultdict
        merchant_devices = defaultdict(set)
        merchant_transactions = defaultdict(list)
        
        for tx in transactions:
            merchant_id = tx.get('merchant_id')
            device_id = tx.get('device_id')
            if merchant_id and device_id:
                merchant_devices[merchant_id].add(device_id)
                merchant_transactions[merchant_id].append(tx)
        
        anomalies = {}
        for merchant_id, devices in merchant_devices.items():
            if len(devices) > 1:
                txs = merchant_transactions[merchant_id]
                total_amount = sum(float(tx.get('amount', 0)) for tx in txs)
                anomalies[merchant_id] = {
                    'device_count': len(devices),
                    'devices': list(devices),
                    'transaction_count': len(txs),
                    'total_amount': round(total_amount, 2)
                }
        
        return anomalies

    def _detect_same_merchant_multiple_locations(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 2: Same merchant transactions from multiple locations
        Fraud Context: Cloned cards, impossible travel, location spoofing
        """
        from collections import defaultdict
        merchant_locations = defaultdict(set)
        merchant_transactions = defaultdict(list)
        
        for tx in transactions:
            merchant_id = tx.get('merchant_id')
            location = tx.get('location')
            if merchant_id and location:
                merchant_locations[merchant_id].add(location)
                merchant_transactions[merchant_id].append(tx)
        
        anomalies = {}
        for merchant_id, locations in merchant_locations.items():
            if len(locations) > 1:
                txs = merchant_transactions[merchant_id]
                total_amount = sum(float(tx.get('amount', 0)) for tx in txs)
                anomalies[merchant_id] = {
                    'location_count': len(locations),
                    'locations': list(locations),
                    'transaction_count': len(txs),
                    'total_amount': round(total_amount, 2)
                }
        
        return anomalies

    def _detect_same_merchant_multiple_ips(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 3: Same merchant accessed from multiple IP addresses
        Fraud Context: Network switching, proxy usage, distributed attacks
        """
        from collections import defaultdict
        merchant_ips = defaultdict(set)
        merchant_transactions = defaultdict(list)
        
        for tx in transactions:
            merchant_id = tx.get('merchant_id')
            ip_address = tx.get('ip_address')
            if merchant_id and ip_address:
                merchant_ips[merchant_id].add(ip_address)
                merchant_transactions[merchant_id].append(tx)
        
        anomalies = {}
        for merchant_id, ips in merchant_ips.items():
            if len(ips) > 1:
                txs = merchant_transactions[merchant_id]
                total_amount = sum(float(tx.get('amount', 0)) for tx in txs)
                anomalies[merchant_id] = {
                    'ip_count': len(ips),
                    'ips': list(ips),
                    'transaction_count': len(txs),
                    'total_amount': round(total_amount, 2)
                }
        
        return anomalies

    def _detect_high_value_location_device_changes(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 4: High-value transactions with location/device changes
        Fraud Context: Fraudulent escalation after gaining access
        """
        if not transactions:
            return {}
        
        # Calculate dynamic high-value threshold (2x average or minimum 1000)
        amounts = [float(tx.get('amount', 0)) for tx in transactions]
        avg_amount = sum(amounts) / len(amounts)
        high_value_threshold = max(avg_amount * 2, 1000)
        
        high_value_txs = [tx for tx in transactions if float(tx.get('amount', 0)) >= high_value_threshold]
        
        if len(high_value_txs) < 2:
            return {}
        
        # Check for location/device diversity in high-value transactions
        locations = set(tx.get('location') for tx in high_value_txs if tx.get('location'))
        devices = set(tx.get('device_id') for tx in high_value_txs if tx.get('device_id'))
        ips = set(tx.get('ip_address') for tx in high_value_txs if tx.get('ip_address'))
        
        if len(locations) > 1 or len(devices) > 1 or len(ips) > 1:
            total_high_value_amount = sum(float(tx.get('amount', 0)) for tx in high_value_txs)
            return {
                'high_value_transaction_count': len(high_value_txs),
                'total_high_value_amount': round(total_high_value_amount, 2),
                'threshold_used': round(high_value_threshold, 2),
                'location_diversity': len(locations) > 1,
                'device_diversity': len(devices) > 1,
                'ip_diversity': len(ips) > 1,
                'unique_locations': len(locations),
                'unique_devices': len(devices),
                'unique_ips': len(ips)
            }
        
        return {}

    def _detect_rapid_payment_method_switching(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 5: Rapid switching between payment methods
        Fraud Context: Testing different payment methods, bypassing controls
        """
        payment_methods = [tx.get('payment_method') for tx in transactions if tx.get('payment_method')]
        payment_sub_types = [tx.get('payment_sub_type') for tx in transactions if tx.get('payment_sub_type')]
        
        unique_methods = set(payment_methods)
        unique_sub_types = set(payment_sub_types)
        
        if len(unique_methods) > 1 or len(unique_sub_types) > 2:
            return {
                'payment_method_count': len(unique_methods),
                'payment_sub_type_count': len(unique_sub_types),
                'methods_used': list(unique_methods),
                'sub_types_used': list(unique_sub_types),
                'transaction_count': len(transactions)
            }
        
        return {}

    def _detect_amount_escalation(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 6: Progressive increase in transaction amounts
        Fraud Context: Testing limits, escalating fraud amounts
        """
        if len(transactions) < 3:
            return {}
        
        # Sort transactions by time
        sorted_txs = sorted(transactions, key=lambda x: datetime.fromisoformat(str(x['transaction_date'])))
        amounts = [float(tx.get('amount', 0)) for tx in sorted_txs]
        
        # Check for consistent escalation (each transaction 1.5x or more than previous)
        escalation_ratios = []
        escalation_count = 0
        
        for i in range(1, len(amounts)):
            if amounts[i-1] > 0:
                ratio = amounts[i] / amounts[i-1]
                escalation_ratios.append(ratio)
                if ratio >= 1.5:
                    escalation_count += 1
        
        # Consider it escalation if at least 2 consecutive increases of 1.5x or more
        if escalation_count >= 2:
            return {
                'transaction_count': len(amounts),
                'start_amount': amounts[0],
                'end_amount': amounts[-1],
                'escalation_factor': round(amounts[-1] / amounts[0], 2) if amounts[0] > 0 else 0,
                'escalation_steps': escalation_count,
                'avg_escalation_ratio': round(sum(escalation_ratios) / len(escalation_ratios), 2)
            }
        
        return {}

    def _detect_cross_channel_abuse(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 7: Rapid switching between online and physical channels
        Fraud Context: Testing different channels, bypassing channel-specific controls
        """
        channels = []
        for tx in transactions:
            payment_method = tx.get('payment_method')
            if payment_method == 'CNP':
                channels.append('online')
            elif payment_method in ['Card Present', 'Contactless']:
                channels.append('physical')
            else:
                channels.append('other')
        
        unique_channels = set(channels)
        
        if len(unique_channels) > 1:
            channel_counts = {channel: channels.count(channel) for channel in unique_channels}
            return {
                'channels_used': list(unique_channels),
                'channel_switches': len(unique_channels),
                'transaction_count': len(transactions),
                'channel_distribution': channel_counts
            }
        
        return {}

    def _detect_mcc_switching_same_merchant(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 8: MCC switching for same merchant
        Fraud Context: Unusual merchant behavior, potential merchant compromise
        """
        from collections import defaultdict
        merchant_mccs = defaultdict(set)
        merchant_transactions = defaultdict(list)
        
        for tx in transactions:
            merchant_id = tx.get('merchant_id')
            mcc = tx.get('mcc')
            if merchant_id and mcc:
                merchant_mccs[merchant_id].add(mcc)
                merchant_transactions[merchant_id].append(tx)
        
        anomalies = {}
        for merchant_id, mccs in merchant_mccs.items():
            if len(mccs) > 1:
                txs = merchant_transactions[merchant_id]
                anomalies[merchant_id] = {
                    'mcc_count': len(mccs),
                    'mccs_used': list(mccs),
                    'transaction_count': len(txs)
                }
        return anomalies

    def _detect_rapid_geographic_movement(self, transactions: List[Dict]) -> Dict:
        """
        Pattern 9: Impossible geographic movement between transactions
        Fraud Context: Impossible travel times, location spoofing
        """
        if len(transactions) < 2:
            return {}
        
        # Sort transactions by time
        sorted_txs = sorted(transactions, key=lambda x: datetime.fromisoformat(str(x['transaction_date'])))
        
        # Filter transactions with geographic coordinates
        geo_txs = [
            tx for tx in sorted_txs 
            if tx.get('latitude') is not None and tx.get('longitude') is not None
        ]
        
        if len(geo_txs) < 2:
            return {}
        
        import math
        
        def haversine_distance(lat1, lon1, lat2, lon2):
            """Calculate distance between two points in kilometers"""
            R = 6371  # Earth radius in km
            phi1, phi2 = math.radians(lat1), math.radians(lat2)
            dphi = math.radians(lat2 - lat1)
            dlambda = math.radians(lon2 - lon1)
            a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
            return 2*R*math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        # Check for impossible travel between consecutive transactions
        for i in range(1, len(geo_txs)):
            prev_tx = geo_txs[i-1]
            curr_tx = geo_txs[i]
            
            distance_km = haversine_distance(
                float(prev_tx['latitude']), float(prev_tx['longitude']),
                float(curr_tx['latitude']), float(curr_tx['longitude'])
            )
            
            time_diff_minutes = (
                datetime.fromisoformat(str(curr_tx['transaction_date'])) - 
                datetime.fromisoformat(str(prev_tx['transaction_date']))
            ).total_seconds() / 60
            
            # Flag if distance > 10km and time < 5 minutes (impossible travel)
            if distance_km > 10 and time_diff_minutes < 5:
                return {
                    'impossible_travel_detected': True,
                    'distance_km': round(distance_km, 2),
                    'time_diff_minutes': round(time_diff_minutes, 2),
                    'prev_location': prev_tx.get('location'),
                    'curr_location': curr_tx.get('location'),
                    'transaction_pair': [prev_tx.get('transaction_id'), curr_tx.get('transaction_id')]
                }
        
        return {}

    def _apply_scenarios(self, velocity_analysis: Dict, time_gap_analysis: Dict, anomaly_analysis: Dict) -> List[Dict]:
        """
        Apply velocity fraud detection scenarios with enhanced multi-dimensional analysis
        """
        scenarios = []
        
        # Scenario 3.1: High velocity in time windows (unchanged)
        triggered_3_1 = velocity_analysis['has_violations']
        
        if triggered_3_1:
            high_severity_violations = [v for v in velocity_analysis['violations'] if v['severity'] == 'HIGH']
            if high_severity_violations:
                violation_details = [
                    f"{v['count']} transactions in {v['window_minutes']} minutes (threshold: {v['threshold']})"
                    for v in high_severity_violations[:2]
                ]
                rationale_3_1 = f"High velocity violations detected: {'; '.join(violation_details)}"
            else:
                violation_details = [
                    f"{v['count']} transactions in {v['window_minutes']} minutes (threshold: {v['threshold']})"
                    for v in velocity_analysis['violations'][:2]
                ]
                rationale_3_1 = f"Velocity violations detected: {'; '.join(violation_details)}"
        else:
            rationale_3_1 = f"No velocity violations detected across {len(self.time_window_mins)} time windows"
        
        scenarios.append({
            'scenario_id': '3.1',
            'triggered': triggered_3_1,
            'rationale': rationale_3_1
        })
        
        # Scenario 3.2: Unusual hours activity (unchanged)
        triggered_3_2 = (
            time_gap_analysis['unusual_hours_activity'] and 
            (time_gap_analysis['gap_violation'] or time_gap_analysis['last_10_min_transactions'] >= 2)
        )
        
        if triggered_3_2:
            rationale_3_2 = f"Unusual hours activity: {time_gap_analysis['last_10_min_transactions']} transactions in last 10 minutes during off-hours (11 PM - 6 AM)"
            if time_gap_analysis['gap_violation']:
                rationale_3_2 += f" with rapid sequence (avg gap: {time_gap_analysis['avg_gap_minutes']} minutes)"
        else:
            rationale_3_2 = f"No unusual hours activity detected or insufficient transaction frequency"
        
        scenarios.append({
            'scenario_id': '3.2',
            'triggered': triggered_3_2,
            'rationale': rationale_3_2
        })
        
        # Scenario 3.3: Enhanced multi-dimensional velocity patterns
        triggered_3_3 = anomaly_analysis['has_velocity_patterns'] and anomaly_analysis['pattern_count'] >= 1
        
        if triggered_3_3:
            # Build detailed rationale with specific patterns
            pattern_descriptions = []
            detected_patterns = anomaly_analysis['detected_patterns']
            
            # High-priority patterns (immediate fraud indicators)
            high_priority_patterns = [
                'same_merchant_multiple_devices',
                'same_merchant_multiple_locations', 
                'same_merchant_multiple_ips',
                'rapid_geographic_movement'
            ]
            
            # Medium-priority patterns (suspicious behavior)
            medium_priority_patterns = [
                'high_value_location_device_changes',
                'amount_escalation_pattern',
                'rapid_payment_method_switching',
                'cross_channel_abuse'
            ]
            
            # Process high-priority patterns first
            for pattern_name in high_priority_patterns:
                if pattern_name in detected_patterns:
                    pattern_data = detected_patterns[pattern_name]
                    if pattern_name == 'same_merchant_multiple_devices':
                        merchant_count = len(pattern_data)
                        pattern_descriptions.append(f"Same merchant accessed from multiple devices: {merchant_count} merchants affected")
                    elif pattern_name == 'same_merchant_multiple_locations':
                        merchant_count = len(pattern_data)
                        pattern_descriptions.append(f"Same merchant transactions from multiple locations: {merchant_count} merchants affected")
                    elif pattern_name == 'same_merchant_multiple_ips':
                        merchant_count = len(pattern_data)
                        pattern_descriptions.append(f"Same merchant accessed from multiple IPs: {merchant_count} merchants affected")
                    elif pattern_name == 'rapid_geographic_movement':
                        pattern_descriptions.append(f"Impossible travel detected: {pattern_data['distance_km']} km in {pattern_data['time_diff_minutes']} minutes")
            
            # Add medium-priority patterns if space allows
            for pattern_name in medium_priority_patterns:
                if pattern_name in detected_patterns and len(pattern_descriptions) < 3:
                    pattern_data = detected_patterns[pattern_name]
                    if pattern_name == 'high_value_location_device_changes':
                        pattern_descriptions.append(f"High-value transactions with location/device changes: {pattern_data['high_value_transaction_count']} transactions")
                    elif pattern_name == 'amount_escalation_pattern':
                        pattern_descriptions.append(f"Amount escalation pattern: {pattern_data['escalation_factor']}x increase over {pattern_data['transaction_count']} transactions")
                    elif pattern_name == 'rapid_payment_method_switching':
                        pattern_descriptions.append(f"Rapid payment method switching: {pattern_data['payment_method_count']} different methods")
                    elif pattern_name == 'cross_channel_abuse':
                        pattern_descriptions.append(f"Cross-channel abuse: {pattern_data['channel_switches']} different channels")
            
            rationale_3_3 = f"Velocity fraud patterns detected in {anomaly_analysis['recent_transaction_count']} recent transactions: {'; '.join(pattern_descriptions[:3])}"
        else:
            rationale_3_3 = f"No significant velocity fraud patterns detected in {anomaly_analysis['recent_transaction_count']} recent transactions"
        
        scenarios.append({
            'scenario_id': '3.3',
            'triggered': triggered_3_3,
            'rationale': rationale_3_3
        })
        
        return scenarios

    def _generate_result(self, velocity_analysis: Dict, time_gap_analysis: Dict, 
                        anomaly_analysis: Dict, scenario_results: List[Dict], 
                        total_transactions: int) -> Dict:
        """Generate final result with scenario analysis following time_day pattern"""
        
        # Scenario configurations
        scenario_configs = {
            '3.1': {
                'description': 'High velocity violations in multiple time windows',
                'fraud_result': 'Probable Fraud (High)',
                'normal_result': 'Not Fraud'
            },
            '3.2': {
                'description': 'Unusual hours activity with rapid transaction sequences',
                'fraud_result': 'Probable Fraud',
                'normal_result': 'Not Fraud'
            },
            '3.3': {
                'description': 'Multi-dimensional velocity anomalies across payment channels',
                'fraud_result': 'Probable Fraud',
                'normal_result': 'Not Fraud'
            }
        }
        
        # Build scenario analysis
        scenario_analysis = []
        for result in scenario_results:
            scenario_id = result['scenario_id']
            config = scenario_configs[scenario_id]
            
            scenario_analysis.append({
                "scenario_id": scenario_id,
                "scenario_description": config['description'],
                "scenario_result": config['fraud_result'] if result['triggered'] else config['normal_result'],
                "rationale": result['rationale']
            })
        
        # Determine overall assessment with proper priority
        triggered_scenarios = [s for s in scenario_results if s['triggered']]
        
        if any(s['scenario_id'] == '3.1' for s in triggered_scenarios):
            overall_result = 'Probable Fraud (High)'
        elif any(s['scenario_id'] in ['3.2', '3.3'] for s in triggered_scenarios):
            overall_result = 'Probable Fraud'
        else:
            overall_result = 'Not Fraud'
        
        overall_rationale = [s['rationale'] for s in triggered_scenarios]
        
        return {
            "scenario_analysis": scenario_analysis,
            "overall_assessment": {
                "result": overall_result,
                "rationale": overall_rationale
            },
            "analysis_metrics": {
                "total_transactions_analyzed": total_transactions,
                "velocity_violations_count": velocity_analysis['violation_count'],
                "max_velocity_severity": velocity_analysis['max_severity'],
                "avg_gap_minutes": time_gap_analysis['avg_gap_minutes'],
                "gap_violation": time_gap_analysis['gap_violation'],
                "unusual_hours_detected": time_gap_analysis['unusual_hours_activity'],
                "last_10_min_transactions": time_gap_analysis['last_10_min_transactions'],
                "multidimensional_anomalies_count": anomaly_analysis['pattern_count'],
                "recent_transaction_count": anomaly_analysis['recent_transaction_count'],
                "avg_time_gap_threshold": self.avg_time_gap_mins
            }
        }

    def validate_inputs(self, **kwargs) -> bool:
        """Validate required inputs"""
        return all(field in kwargs for field in self.required_fields)

    def _get_parameter_schema(self) -> Dict[str, Any]:
        return {
            "customer_id": {"type": "string", "description": "Customer identifier"},
            "transaction_timestamp": {"type": "string", "description": "Current transaction timestamp (ISO format)"}
        }

    def _get_return_schema(self) -> Dict[str, Any]:
        return {
            "scenario_analysis": {
                "type": "array",
                "description": "List of individual velocity scenario analyses with their IDs, descriptions, results, and rationales.",
                "items": {
                    "type": "object",
                    "properties": {
                        "scenario_id": {
                            "type": "string",
                            "description": "Velocity scenario identifier: '3.1', '3.2', or '3.3'."
                        },
                        "scenario_description": {
                            "type": "string",
                            "description": "Detailed description of the velocity scenario being evaluated."
                        },
                        "scenario_result": {
                            "type": "string",
                            "description": "Outcome: 'Probable Fraud (High)', 'Probable Fraud', or 'Not Fraud'."
                        },
                        "rationale": {
                            "type": "string",
                            "description": "Explanation with specific velocity metrics and findings."
                        }
                    },
                    "required": ["scenario_id", "scenario_description", "scenario_result", "rationale"]
                }
            },
            "overall_assessment": {
                "type": "object",
                "description": "Overall velocity assessment based on all scenario analyses.",
                "properties": {
                    "result": {
                        "type": "string",
                        "description": "Final result: 'Probable Fraud (High)', 'Probable Fraud', or 'Not Fraud'."
                    },
                    "rationale": {
                        "type": "array",
                        "description": "List of key rationales from triggered velocity scenarios.",
                        "items": {"type": "string"}
                    }
                },
                "required": ["result", "rationale"]
            },
            "analysis_metrics": {
                "type": "object",
                "description": "Numerical velocity metrics for AI model decision-making.",
                "properties": {
                    "total_transactions_analyzed": {
                        "type": "integer",
                        "description": "Total historical transactions analyzed within 1 day lookback."
                    },
                    "velocity_violations_count": {
                        "type": "integer",
                        "description": "Number of time windows where velocity thresholds were exceeded."
                    },
                    "max_velocity_severity": {
                        "type": "string",
                        "description": "Highest severity level of velocity violations: 'HIGH', 'MEDIUM', 'LOW', or 'NONE'."
                    },
                    "avg_gap_minutes": {
                        "type": "number",
                        "description": "Average time gap in minutes between consecutive transactions."
                    },
                    "gap_violation": {
                        "type": "boolean",
                        "description": "Whether average time gap is below the rapid sequence threshold."
                    },
                    "unusual_hours_detected": {
                        "type": "boolean",
                        "description": "Whether transactions occurred during unusual hours (11 PM - 6 AM) in last 10 minutes."
                    },
                    "last_10_min_transactions": {
                        "type": "integer",
                        "description": "Number of transactions in the last 10 minutes from alert time."
                    },
                    "multidimensional_anomalies_count": {
                        "type": "integer",
                        "description": "Count of different dimensions showing anomalous patterns in recent transactions."
                    },
                    "recent_transaction_count": {
                        "type": "integer",
                        "description": "Total number of transactions analyzed in the last 10 minutes for anomaly detection."
                    },
                    "avg_time_gap_threshold": {
                        "type": "number",
                        "description": "Configured threshold for average time gap used to detect rapid sequences."
                    }
                },
                "required": ["total_transactions_analyzed", "velocity_violations_count", "max_velocity_severity", "avg_gap_minutes", "gap_violation", "unusual_hours_detected", "last_10_min_transactions", "multidimensional_anomalies_count", "recent_transaction_count", "avg_time_gap_threshold"]
            }
        }
