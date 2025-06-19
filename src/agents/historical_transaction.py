# Importing Dependencies
import json
import pandas as pd
from typing import List, Dict
from autogen_core.tools import FunctionTool, Tool
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient

from ..core.schemas import Alert, AgentResult, ToolCategory
from ..core.prompts import get_historical_transaction_analysis_agent_prompt
from ..core.tools_registry import ToolRegistry

# Historical Transaction Analysis Agent Class
class HistoricalTransactionAnalysisAgent:
    def __init__(self, alert: Alert, transaction_data: pd.DataFrame, config: Dict):
        self.alert = alert
        self.config = config
        self.transaction_data = transaction_data
        self.tool_registry = ToolRegistry(transaction_data=transaction_data)
        self.tools =  self._initialize_tools()
        self.agent = AssistantAgent(
            name="historical_transaction_analysis_agent",
            model_client=OpenAIChatCompletionClient(model="gpt-4o-mini"),
            tools=self.tools,
            system_message=self._get_system_message(),
            output_content_type=AgentResult
        )

    def _get_system_message(self):
        return get_historical_transaction_analysis_agent_prompt(self.config)
    
    def _initialize_tools(self) -> List[Tool]:
        """Initialize tools from registry and convert to FunctionTool format."""
        tools = []
        
        # Get all transaction analysis tools
        transaction_tools = self.tool_registry.get_tools_by_category(ToolCategory.TRANSACTION_ANALYSIS)
        
        for tool_name in transaction_tools:
            # Get tool instance from registry
            tool_instance = self.tool_registry.get_tool(tool_name)
            if tool_instance:
                tools.append(
                    FunctionTool(
                        tool_instance.execute,
                        name=tool_name.lower().replace(" ", "_"),
                        description=tool_instance.description,
                        strict=True
                    )
                )
        
        return tools
    
    def _create_task_prompt(self):
        return f"""Perform all the checks and call all the tools for analysing the raised alert.\n\nAlert: {self.alert} and analyze if the raised alert is false positive or not."""

    async def execute_task(self) -> AgentResult:
        # Create specific prompts based on check type
        prompt = self._create_task_prompt()
        
        response = await self.agent.run(
            task=prompt
        )
        
        return response