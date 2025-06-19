# Importing Dependencies
import pandas as pd
from typing import List
from autogen_core.tools import FunctionTool, Tool
from autogen_agentchat.agents import AssistantAgent
from autogen_ext.models.openai import OpenAIChatCompletionClient

from ..core.schemas import Alert, AgentResult
from ..core.prompts import get_historical_alert_analysis_agent_prompt

# Historical Alert Analysis Agent Class
class HistoricalAlertAnalysisAgent:
    def __init__(self, alert: Alert, alert_data: pd.DataFrame):
        self.alert = alert
        self.tools = self._initialize_tools()
        self.agent = AssistantAgent(
            name="historical_alert_analysis_agent",
            model_client=OpenAIChatCompletionClient(model="gpt-4o-mini"),
            tools=self.tools,
            system_message=self._get_system_message(),
            output_content_type=AgentResult
        )

    def _get_system_message(self):
        return get_historical_alert_analysis_agent_prompt()
    
    def _initialize_tools(self) -> List[Tool]:
        return []

    async def execute_task(self) -> AgentResult:
        # Create specific prompts based on check type
        prompt = self._create_task_prompt()
        
        response = await self.agent.a_initiate_chat(
            recipient=self.agent,
            message=prompt,
            max_turns=1
        )
        
        return response