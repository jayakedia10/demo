# Importing Dependencies
import random
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional

class SampleTransactionsDataGenerator:
    """
    A data wrapper class for handling transaction data generation and management.
    """
    
    def __init__(self):
        self.merchant_categories = {
            "Grocery": "5411",
            "Fuel": "5541",
            "Electronics": "5732",
            "Clothing": "5651",
            "Restaurant": "5812",
            "Travel": "4511",
            "Healthcare": "8011",
            "Entertainment": "7832",
            "Education": "8299",
            "Utilities": "4900"
        }
        self.locations = ["Bandra", "Andheri", "Borivali", "Colaba", "Dadar", "Kurla", "Powai", 
                         "Vasai", "Thane", "Navi Mumbai"]
        self.payment_methods = {
            "Card Present": {
                "sub_types": ["Mag Stripe", "EMV Chip", "Token NFC"],
                "pin_verified": [True, False]
            },
            "Contactless": {
                "sub_types": ["Tap to Pay", "Mobile Wallet"],
                "pin_verified": [False]
            },
            "CNP": {
                "sub_types": ["Online"],
                "pin_verified": [False]
            }
        }
        self._data: Optional[pd.DataFrame] = None
        self._velocity_data: Optional[pd.DataFrame] = None

    def generate_data(self, num_users: int = 10, transactions_per_user: int = 20) -> pd.DataFrame:
        """
        Generate sample transaction data with realistic temporal patterns.

        Args:
            num_users (int): Number of users to generate data for
            transactions_per_user (int): Number of transactions per user

        Returns:
            pd.DataFrame: Generated transaction data
        """
        transaction_data = []
        base_date = datetime.now()

        for customer_id in range(1, num_users + 1):
            # Start with current date and generate historical transactions
            current_date = base_date
            
            # Generate transactions with realistic time gaps
            for transaction_id in range(1, transactions_per_user + 1):
                # Add random time gap between transactions
                time_gap = self._generate_time_gap()
                current_date = current_date - time_gap  # Subtract to go back in time
                
                transaction = self._generate_transaction(customer_id, transaction_id, current_date)
                transaction_data.append(transaction)

        self._data = pd.DataFrame(transaction_data)
        self._data = self._data.sort_values('transaction_timestamp', ascending=False)
        return self._data

    def generate_velocity_data(self, num_users: int = 5, transactions_per_user: int = 10) -> pd.DataFrame:
        """
        Generate high-velocity transaction data for testing velocity-based rules.

        Args:
            num_users (int): Number of users to generate data for
            transactions_per_user (int): Number of transactions per user

        Returns:
            pd.DataFrame: Generated velocity transaction data
        """
        velocity_data = []
        base_date = datetime.now()

        for customer_id in range(1, num_users + 1):
            current_date = base_date
            
            # Generate rapid transactions within short time windows
            for transaction_id in range(1, transactions_per_user + 1):
                # Generate transactions with very short time gaps (1-5 minutes)
                time_gap = timedelta(minutes=random.randint(1, 5))
                current_date = current_date - time_gap
                
                transaction = self._generate_transaction(customer_id, transaction_id, current_date)
                velocity_data.append(transaction)

        self._velocity_data = pd.DataFrame(velocity_data)
        self._velocity_data = self._velocity_data.sort_values('transaction_timestamp', ascending=False)
        return self._velocity_data

    def _generate_time_gap(self) -> timedelta:
        """
        Generate realistic time gaps between transactions.
        More frequent transactions during business hours and weekdays.
        """
        # 40% chance of multiple transactions within same hour
        if random.random() < 0.8:
            minutes = random.randint(1, 59)
            return timedelta(minutes=minutes)
            
        # 30% chance of transaction within same day
        if random.random() < 0.2:
            hours = random.randint(1, 23)
            minutes = random.randint(0, 59)
            return timedelta(hours=hours, minutes=minutes)
            
        # 20% chance of transaction within same week
        if random.random() < 0.1:
            days = random.randint(1, 6)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            return timedelta(days=days, hours=hours, minutes=minutes)
            
        # 10% chance of transaction within last month
        days = random.randint(7, 30)
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        return timedelta(days=days, hours=hours, minutes=minutes)

    def _generate_transaction(self, 
                              customer_id: int, 
                              transaction_id: int,
                              transaction_timestamp: datetime,
                              transaction_amount: float = 0.0) -> Dict:
        """
        Generate a single transaction record with realistic patterns.

        Args:
            customer_id (int): User identifier
            transaction_id (int): Transaction identifier
            transaction_timestamp (datetime): Transaction timestamp

        Returns:
            Dict: Transaction record
        """
        payment_method = random.choice(list(self.payment_methods.keys()))
        sub_type = random.choice(self.payment_methods[payment_method]["sub_types"])
        pin_verified = random.choice(self.payment_methods[payment_method]["pin_verified"])
        
        # Generate realistic amounts based on category
        category = random.choice(list(self.merchant_categories.keys()))
        amount = transaction_amount if transaction_amount > 0 else self._generate_amount(category)

        # Generate consistent merchant IDs for regular transactions
        merchant_id = self._generate_merchant_id(customer_id, category)

        return {
            "customer_id": str(customer_id),
            "transaction_id": f"tx_{customer_id}_{transaction_id}",
            "amount": amount,
            "category": category,
            "mcc": self.merchant_categories[category],
            "location": random.choice(self.locations),
            "transaction_timestamp": transaction_timestamp,
            "merchant_id": merchant_id,
            "country": "India",
            "currency": "INR",
            "payment_method": payment_method,
            "payment_sub_type": sub_type,
            "pin_verified": pin_verified,
            "device_id": f"device_{random.randint(1, 2)}" if sub_type in ["Token NFC", "Mobile Wallet"] else None,
            "ip_address": f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}" if payment_method == "CNP" else None,
            "latitude": round(random.uniform(18.9, 19.2), 6) if payment_method in ["Card Present", "Contactless"] else None,
            "longitude": round(random.uniform(72.8, 73.0), 6) if payment_method in ["Card Present", "Contactless"] else None,
            "alert_history": True,
        }

    def _generate_amount(self, category: str) -> float:
        """
        Generate realistic transaction amounts based on category.
        """
        base_amounts = {
            "Grocery": (500, 3000),
            "Fuel": (1000, 2000),
            "Electronics": (2000, 50000),
            "Clothing": (1000, 5000),
            "Restaurant": (500, 3000),
            "Travel": (5000, 50000),
            "Healthcare": (1000, 10000),
            "Entertainment": (500, 2000),
            "Education": (5000, 50000),
            "Utilities": (1000, 5000)
        }
        
        min_amount, max_amount = base_amounts.get(category, (100, 5000))
        return round(random.uniform(min_amount, max_amount), 2)

    def _generate_merchant_id(self, customer_id: int, category: str) -> str:
        """
        Generate consistent merchant IDs for regular transactions.
        """
        # 70% chance of using a regular merchant for the user
        if random.random() < 0.7:
            return f"merchant_{customer_id}_{category}_{random.randint(1, 3)}"
        return f"merchant_{random.randint(1, 15)}"

    def get_data(self) -> Optional[pd.DataFrame]:
        """
        Get the current transaction data.

        Returns:
            Optional[pd.DataFrame]: Current transaction data if available
        """
        return self._data

    def get_velocity_data(self) -> Optional[pd.DataFrame]:
        """
        Get the current velocity transaction data.

        Returns:
            Optional[pd.DataFrame]: Current velocity transaction data if available
        """
        return self._velocity_data

    def get_merchant_transactions(self, merchant_id: str) -> pd.DataFrame:
        """
        Get transactions for a specific merchant.

        Args:
            merchant_id (str): Merchant identifier

        Returns:
            pd.DataFrame: Filtered transactions for the merchant
        """
        if self._data is None:
            raise ValueError("No data available. Generate data first.")
        return self._data[self._data['merchant_id'] == merchant_id]

    def get_user_transactions(self, customer_id: str, lookback_days: Optional[int] = None) -> List[Dict]:
        """
        Get transactions for a specific user as JSON.

        Args:
            customer_id (str): User identifier 
            lookback_days (Optional[int]): Number of days to look back from current date

        Returns:
            Dict: Filtered transactions for the user in JSON format
        """
        # Generate data if not already done
        self.generate_data(num_users=5, transactions_per_user=1000)

        if self._data is None:
            raise ValueError("No data available. Generate data first.")
        
        filtered_data = self._data[self._data['customer_id'] == customer_id]
        
        if lookback_days is not None:
            cutoff_date = datetime.now() - timedelta(days=lookback_days)
            filtered_data = filtered_data[filtered_data['transaction_timestamp'] >= cutoff_date]

        return filtered_data.to_dict(orient='records')