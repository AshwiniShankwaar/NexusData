"""
NexusData: The Universal Agentic Data Interlocutor.
"""
__version__ = "0.1.0"

# Suppress Pydantic's model_name namespace warning globally —
# LLMConfig deliberately uses model_name with protected_namespaces=()
import warnings as _w
_w.filterwarnings(
    "ignore",
    message=r'Field "model_name" has conflict with protected namespace',
    category=UserWarning,
)
