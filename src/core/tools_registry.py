# Importing Dependencies
import pandas as pd
from typing import Dict, List, Optional
from .basetools import BaseTool, ToolCategory

from ..tools.transactions import (PreviousHistoryTransactions,
                                PatternsTransactions,
                                VelocityTransactions,
                                AmountTransactions,
                                AverageTicketSizeTransactions,
                                RiskyMCCTransactions,
                                FirstTimeAlertTransactions,
                                RiskyCountryCurrencyTransactions,
                                CardPresentTransactions,
                                ContactlessTransactions,
                                TokenNFCTransactions,
                                PinVerifiedTransactions,
                                MagStripeTransactions,
                                CNPTransactions,
                                GeoLocationTransactions)

class ToolRegistry:
    """Registry for managing fraud detection tools."""
    
    def __init__(self, transaction_data: pd.DataFrame):
        self.transaction_data = transaction_data
        self._tools: Dict[str, BaseTool] = {}
        self._categories: Dict[ToolCategory, List[str]] = {}
        
        # Register default tools
        self._register_default_tools()
    
    def _register_default_tools(self):
        """Register all default tools."""
        default_tools = [
            PreviousHistoryTransactions(self.transaction_data),
            PatternsTransactions(self.transaction_data),
            VelocityTransactions(self.transaction_data),
            AmountTransactions(self.transaction_data),
            AverageTicketSizeTransactions(self.transaction_data),
            RiskyMCCTransactions(self.transaction_data),
            FirstTimeAlertTransactions(self.transaction_data),
            RiskyCountryCurrencyTransactions(self.transaction_data),
            CardPresentTransactions(self.transaction_data),
            ContactlessTransactions(self.transaction_data),
            TokenNFCTransactions(self.transaction_data),
            PinVerifiedTransactions(self.transaction_data),
            MagStripeTransactions(self.transaction_data),
            CNPTransactions(self.transaction_data),
            GeoLocationTransactions(self.transaction_data)
        ]
        for tool in default_tools:
            self.register_tool(tool)
    
    def register_tool(self, tool_instance: BaseTool):
        """Register a tool instance."""
        tool_name = tool_instance.name
        tool_category = tool_instance.category
        
        self._tools[tool_name] = tool_instance
        
        if tool_category not in self._categories:
            self._categories[tool_category] = []
        self._categories[tool_category].append(tool_name)
    
    def get_tool(self, tool_name: str) -> Optional[BaseTool]:
        """Get a tool instance."""
        return self._tools.get(tool_name)
    
    def get_tools_by_category(self, category: ToolCategory) -> List[str]:
        """Get all tool names in a category."""
        return self._categories.get(category, [])
    
    def list_all_tools(self) -> Dict[str, Dict]:
        """List all registered tools with their schemas."""
        tools_info = {}
        for tool_name, tool_instance in self._tools.items():
            tools_info[tool_name] = tool_instance.get_schema()
        return tools_info
