# Importing Dependencies
import json
from typing import List
from autogen_agentchat.agents import AssistantAgent

from ..core.schemas import AgentResult
from ..core.prompts import get_final_analysis_agent_prompt


class FinalAnalysisAgent:
    def __init__(self, llm_config):
        self.agent = AssistantAgent(
            name="final_analysis_agent",
            system_message=self._get_system_message(),
            llm_config=llm_config,
            human_input_mode="NEVER"
        )

    def _get_system_message(self):
        return get_final_analysis_agent_prompt()

    async def execute_task(self) -> AgentResult:
        # Create specific prompts based on check type
        prompt = self._create_task_prompt()
        
        response = await self.agent.a_initiate_chat(
            recipient=self.agent,
            message=prompt,
            max_turns=1
        )
        
        return response