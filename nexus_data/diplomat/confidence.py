"""
nexus_data/diplomat/confidence.py  — Task 5.1
Confidence Scorer: maps LLM log-probability signals to a 0–1 score
and routes low-confidence answers to the Clarification Bridge.
"""
from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

CONFIDENCE_THRESHOLD = 0.65   # below this → trigger clarification


def logprobs_to_confidence(logprobs: List[float]) -> float:
    """
    Convert a list of per-token log-probabilities to a scalar confidence score.

    Strategy: mean of the top-K most probable tokens' log-probs,
    normalised to [0, 1] via exp(mean).

    Parameters
    ----------
    logprobs : list of log-probabilities (negatives), one per token

    Returns
    -------
    float in [0, 1]
    """
    if not logprobs:
        return 0.0
    mean_logprob = sum(logprobs) / len(logprobs)
    # exp(mean_logprob) ∈ (0, 1] since logprobs ≤ 0
    return round(min(1.0, max(0.0, math.exp(mean_logprob))), 4)


class ConfidenceScorer:
    """
    Wraps LLM response metadata to produce an actionable confidence signal.

    Works with:
    - Ollama  : response["eval_count"] / logprobs list
    - OpenAI  : response.choices[0].logprobs.token_logprobs
    - Raw     : pass logprobs list directly
    """

    def __init__(self, threshold: float = CONFIDENCE_THRESHOLD):
        self.threshold = threshold

    def score_from_logprobs(self, logprobs: List[float]) -> float:
        return logprobs_to_confidence(logprobs)

    def score_from_ollama_response(self, response: Dict[str, Any]) -> float:
        """Extract confidence from an Ollama /api/generate response dict."""
        lp = response.get("logprobs", [])
        if lp:
            return self.score_from_logprobs(lp)
        # Fallback: use eval_duration / prompt_eval_count heuristic
        return 0.5   # neutral when no logprobs available

    def is_confident(self, score: float) -> bool:
        return score >= self.threshold

    def should_clarify(self, score: float) -> bool:
        return not self.is_confident(score)
