import os
import logging
from langchain_mcp_adapters.client import MultiServerMCPClient

logger = logging.getLogger(__name__)

AIRDOCTOR_MCP_URL = os.getenv("AIRDOCTOR_MCP_URL", "http://localhost:8081")


def get_streamable_http_mcp_client() -> MultiServerMCPClient:
    """Returns an MCP Client compatible with LangChain/LangGraph."""
    return MultiServerMCPClient(
        {
            "airdoctor_mcp": {
                "transport": "streamable_http",
                "url": AIRDOCTOR_MCP_URL,
            }
        }
    )
