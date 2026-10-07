"""
AirDoctor LLM Provider Loader.
Supports multi-cloud model backends:
1. Amazon Bedrock (via langchain-aws)
2. Google Cloud Vertex AI (via langchain-google-vertexai)
"""

import os
import logging
from typing import Any
from langchain_core.language_models.chat_models import BaseChatModel

logger = logging.getLogger("airdoctor.model")

DEFAULT_BEDROCK_MODEL_ID = "global.anthropic.claude-sonnet-4-5-20250929-v1:0"
DEFAULT_VERTEX_MODEL = "gemini-3-pro"


def _load_bedrock_model() -> BaseChatModel:
    """Initializes ChatBedrock using AWS IAM credentials."""
    try:
        from langchain_aws import ChatBedrock
    except ImportError:
        raise ImportError(
            "The 'langchain-aws' package is required when LLM_PROVIDER='bedrock'. "
            "Install it with: pip install langchain-aws"
        )

    model_id = os.getenv("BEDROCK_MODEL_ID", DEFAULT_BEDROCK_MODEL_ID)
    region = os.getenv("AWS_REGION", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    logger.info(f"[AirDoctor] Loading Amazon Bedrock model '{model_id}' in region '{region}'")
    return ChatBedrock(model_id=model_id, region_name=region)


def _load_vertex_model() -> BaseChatModel:
    """Initializes ChatVertexAI using Google Cloud credentials."""
    try:
        from langchain_google_vertexai import ChatVertexAI
    except ImportError:
        raise ImportError(
            "The 'langchain-google-vertexai' package is required when LLM_PROVIDER='vertexai'. "
            "Install it with: pip install langchain-google-vertexai"
        )

    model_name = os.getenv("VERTEX_MODEL_NAME", os.getenv("VERTEX_MODEL_ID", DEFAULT_VERTEX_MODEL))
    project = os.getenv("GOOGLE_CLOUD_PROJECT", os.getenv("GCP_PROJECT"))
    location = os.getenv("GOOGLE_CLOUD_REGION", os.getenv("GCP_REGION", "us-central1"))
    temperature = float(os.getenv("VERTEX_TEMPERATURE", "0.0"))

    logger.info(f"[AirDoctor] Loading Vertex AI model '{model_name}' in project '{project}', region '{location}'")
    return ChatVertexAI(
        model_name=model_name,
        project=project,
        location=location,
        temperature=temperature
    )


def load_model() -> BaseChatModel:
    """
    Factory function returning the configured BaseChatModel instance.
    Select provider using the LLM_PROVIDER environment variable ('bedrock' | 'vertexai').
    """
    provider = os.getenv("LLM_PROVIDER", "bedrock").lower().strip()

    if provider in ("bedrock", "aws"):
        return _load_bedrock_model()
    elif provider in ("vertexai", "vertex", "google", "gemini"):
        return _load_vertex_model()
    else:
        raise ValueError(
            f"Unsupported LLM_PROVIDER='{provider}'. "
            f"Supported providers are: 'bedrock', 'vertexai'"
        )
