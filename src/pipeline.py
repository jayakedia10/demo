# Importing Dependencies
import os
import json
import pandas as pd
from typing import Dict 
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

from .core.schemas import Alert
from .agents import (CheckAnalysisAgent,
                     HistoricalTransactionAnalysisAgent,
                     HistoricalAlertAnalysisAgent,
                     FinalAnalysisAgent)
from .utils.data_generator import SampleTransactionsDataGenerator

class AlertAnalysisPipeline:
    def __init__(self, alert: Alert):
        self.alert = alert
        # self.check_analysis_agent = CheckAnalysisAgent()
        self.config = self._get_custom_config()
        self.transaction_data = self._get_transaction_data()
        self.alert_data = self._get_alert_data()
        self.transaction_agent = HistoricalTransactionAnalysisAgent(
            alert=self.alert, 
            transaction_data=self.transaction_data,
            config=self.config)
        self.alert_agent = HistoricalAlertAnalysisAgent(
            alert=self.alert, 
            alert_data=self.alert_data)
        # self.final_agent = FinalAnalysisAgent()

    def _get_custom_config(self) -> Dict:
        
        config_path = "configs/sample_config.json"
        with open(config_path, 'r') as f:
            return json.load(f)

    def _get_transaction_data(self):
        data_generator = SampleTransactionsDataGenerator()
        transaction_data = data_generator.generate_data(num_users=1,
                                                        transactions_per_user=1000)
        # Add anomaly transaction with suspicious characteristics
        anomaly = pd.DataFrame([{
            "customer_id": self.alert.customer_id,
            "transaction_id": f"tx_{self.alert.customer_id}_anomaly",
            "amount": 50000,  # High value transaction
            "category": "Electronics",  # High-risk category
            "location": "Unknown",  # Missing location data
            "transaction_date": datetime.now(),
            "merchant_id": self.alert.merchant_id,
            "country": "India", 
            "currency": "INR",
            "payment_method": "CNP",  # Card Not Present
            "payment_sub_type": "Online",
            "pin_verified": False,
            "device_id": None,  # Missing device data
            "ip_address": "192.168.1.1",
            "latitude": None,  # Missing geolocation
            "longitude": None
        }])
        
        # Append anomaly to transaction data with proper index handling
        transaction_data = pd.concat([transaction_data, anomaly], ignore_index=True)
        return transaction_data
    
    def _get_alert_data(self):
        pass

    async def investigate_alert(self):
        """
        Main pipeline method that orchestrates the complete fraud investigation
        """
        # Directly running Historical Transaction Analysis Agent
        return await self.transaction_agent.execute_task()