# Importing Dependencies
import json
from typing import List
from autogen_agentchat.agents import AssistantAgent

from ..core.schemas import Alert, AgentResult
from ..core.prompts import get_check_analysis_agent_prompt

# Check Analysis Agent Class
class CheckAnalysisAgent:
    def __init__(self, llm_config):
        self.agent = AssistantAgent(
            name="check_analysis_agent",
            system_message=self._get_system_message(),
            llm_config=llm_config,
            human_input_mode="NEVER"
        )

    def _get_system_message(self):
        return get_check_analysis_agent_prompt()
    
    async def analyze_alert(self, alert: Alert) -> List[AgentResult]:
        prompt = f"""
        FRAUD ALERT ANALYSIS REQUEST

        Alert Details:
        - Alert ID: {alert.alert_id}
        - Customer ID: {alert.customer_id}
        - Transaction ID: {alert.transaction_id}
        - Check Type: {alert.check_type}
        - Risk Score: {alert.risk_score}
        
        Transaction Data:
        {json.dumps(alert.transaction_data, indent=2)}

        Please analyze this alert and break it down into specific investigation tasks for the specialized agents.
        Focus on the {alert.check_type} check that triggered this alert.
        """
        
        response = await self.agent.a_initiate_chat(
            recipient=self.agent,
            message=prompt,
            max_turns=1
        )
        
        # Parse response and create AnalysisTask objects
        return self._parse_analysis_response(response, alert)
    
