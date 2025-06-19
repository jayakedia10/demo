# Importing Dependencies
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from .schemas import CheckCategory, CheckResult
from ..utils.data_generator import SampleTransactionsDataGenerator


class BaseTool(ABC):
    """Abstract base class for all fraud detection tools."""
    
    def __init__(self, name: str, 
                 description: str,
                 category: CheckCategory,
                 dependencies: Optional[List[str]] = None):
        self.name = name
        self.description = description
        self.category = category
        self.dependencies = dependencies or []
        self._is_initialized = False
        self._logger = logging.getLogger(f"{self.category}.{self.name}")
    
    @abstractmethod
    async def initialize(self, customer_id: str, lookback_days: Optional[int]) -> bool:
        """Initialize the tool with necessary resources."""
        data_wrapper = SampleTransactionsDataGenerator()
        self._historical_transactions = data_wrapper.get_user_transactions(customer_id, lookback_days)
        return True

    @abstractmethod
    def validate_inputs(self, **kwargs) -> bool:
        """Validate input parameters before execution."""
        pass

    @abstractmethod
    async def execute(self, **kwargs) -> CheckResult:
        """Execute the tool using given parameters."""
        pass

    async def managed_execution(self, **kwargs) -> CheckResult:
        """Execute the tool using given parameters with automatic cleanup."""
        try:
            return await self.execute(**kwargs)
        finally:
            await self.cleanup()
    
    async def cleanup(self) -> None:
        """Clean up resources after tool execution."""
        pass
    
    def get_schema(self) -> Dict[str, Any]:
        """Return the tool's input/output schema for validation."""
        return {
            "name": self.name,
            "description": self.description,
            "category": self.category,
            "dependencies": self.dependencies,
            "args": self._get_parameter_schema(),
            "returns": self._get_return_schema()
        }
    
    @abstractmethod
    def _get_parameter_schema(self) -> Dict[str, Any]:
        """Define the parameter schema for this tool."""
        pass
    
    @abstractmethod
    def _get_return_schema(self) -> Dict[str, Any]:
        """Define the return value schema for this tool."""
        pass