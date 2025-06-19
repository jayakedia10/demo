# Importing Dependencies
import os
import json
import pandas as pd
from typing import List, Dict
from dotenv import load_dotenv
from autogen_core.tools import FunctionTool, Tool
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient

from ..core.schemas import Alert, AgentResult, ToolCategory
from ..core.prompts import get_time_day_analysis_agent_prompt
from ..tools.transactions import TimeDayTransactions

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Time & Day Analysis Agent Class
class TimeDayAnalysisAgent:
    def __init__(self, alert: Alert, transaction_data: pd.DataFrame):
        self.alert = alert
        self.config = self._get_custom_config()
        self.transaction_data = transaction_data
        self.tools = [TimeDayTransactions(transaction_data)]
        self.agent = AssistantAgent(
            name="time_day_analysis_agent",
            model_client=OpenAIChatCompletionClient(model="gpt-4o-mini",
                                                    api_key=OPENAI_API_KEY),
            tools=self._initialize_tools(),
            system_message=self._get_system_message(),
            output_content_type=AgentResult
        )

    def _get_custom_config(self) -> Dict:
        """Load custom configuration from JSON file."""
        config_path = "configs/sample_config.json"
        with open(config_path, 'r') as f:
            return json.load(f)

    def _get_system_message(self):
        return get_time_day_analysis_agent_prompt(self.config)
    
    def _initialize_tools(self) -> List[Tool]:
        """Initialize tools from registry and convert to FunctionTool format."""
        tools = []
        for tool in self.tools:
            tools.append(
                FunctionTool(
                    tool.execute,
                    name=tool.name.lower().replace(" ", "_"),
                    description=tool.description,
                    strict=True
                )
            )
        
        return tools
    
    def _create_task_prompt(self):
         return f"""Perform the time & day analysis on the raised alert.\n\nAlert: {self.alert} and analyze if the raised alert is false positive or not."""
    
    async def execute_task(self) -> AgentResult:
        """Execute the analysis task and return the result."""
        task_prompt = self._create_task_prompt()
        return await self.agent.run(task=task_prompt)