"""Zero-shot 7-class triage over SigLIP 2.

Algorithm per CLIP paper §3.1.4, adapted for multilingual SigLIP 2:

  1. Precompute text embeddings for every template of every label, across
     DE/FR/IT/EN. Average them to one vector per label, L2-normalize.
  2. For each image, encode → L2-normalize.
  3. Cosine-sim against each class vector → logits.
  4. Softmax(logits × 100) → class probabilities. (Scaling by 100 sharpens; this
     is the usual CLIP zero-shot recipe. SigLIP sigmoid training is a detail of
     training, not scoring — for retrieval we still use cosine.)
  5. Argmax → label; take the softmax score as `confidence`.
  6. If (confidence < THRESHOLD) or (margin_top1_minus_top2 < AMBIGUOUS_MARGIN),
     emit a [WARN] and route the image according to the bias rule:
       - low confidence         → "other-uninformative" (drop)
       - ambiguous & kept-class → keep (favor retention per user directive)

Every non-trivial branch writes through image_search.common.warn so we never
silently drop or re-route per project CLAUDE.md §5.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import torch
from PIL import Image

from image_search.common.model import LoadedModel
from image_search.common.prompts import (
    ALL_CLASSES,
    DROPPED_CLASSES,
    KEPT_CLASSES,
    flatten,
)
from image_search.common.warn import warn


CONFIDENCE_THRESHOLD: float = 0.35
AMBIGUOUS_MARGIN: float = 0.05
SOFTMAX_TEMPERATURE: float = 100.0  # standard CLIP-style sharpening


@dataclass(frozen=True)
class TriageResult:
    label: str
    confidence: float
    margin: float
    all_scores: dict[str, float]


def _l2_normalize(x: torch.Tensor) -> torch.Tensor:
    return x / x.norm(dim=-1, keepdim=True).clamp(min=1e-9)


def _unwrap_features(out) -> torch.Tensor:
    """transformers 5.x returns BaseModelOutputWithPooling from get_*_features;
    earlier versions returned a tensor. Handle both."""
    if isinstance(out, torch.Tensor):
        return out
    if hasattr(out, "pooler_output") and out.pooler_output is not None:
        return out.pooler_output
    raise TypeError(f"unexpected features output: {type(out).__name__}")


@torch.inference_mode()
def build_class_text_bank(lm: LoadedModel) -> dict[str, torch.Tensor]:
    """Encode every template, average per label, return a dict of L2-normalized
    per-class text vectors on lm.device with lm.dtype.
    """
    bank: dict[str, torch.Tensor] = {}
    for label in ALL_CLASSES:
        templates = flatten(label)
        inputs = lm.processor(text=templates, padding="max_length",
                              truncation=True, return_tensors="pt")
        inputs = {k: v.to(lm.device) for k, v in inputs.items()}
        feats = _unwrap_features(lm.model.get_text_features(**inputs))
        feats = _l2_normalize(feats.to(lm.dtype))
        bank[label] = _l2_normalize(feats.mean(dim=0, keepdim=True)).squeeze(0)
    return bank


def _decide_from_scores(
    probs: torch.Tensor,  # (N, C) softmax probabilities
    parent_ids: list[str] | None,
) -> list[TriageResult]:
    results: list[TriageResult] = []
    for i in range(probs.shape[0]):
        row = probs[i]
        sorted_idx = torch.argsort(row, descending=True)
        top_idx = int(sorted_idx[0].item())
        runner_idx = int(sorted_idx[1].item())
        top_label = ALL_CLASSES[top_idx]
        runner_label = ALL_CLASSES[runner_idx]
        top_score = float(row[top_idx].item())
        runner_score = float(row[runner_idx].item())
        margin = top_score - runner_score
        all_scores = {c: float(row[j].item()) for j, c in enumerate(ALL_CLASSES)}

        parent = parent_ids[i] if parent_ids else f"image_{i}"

        final_label = top_label
        if top_score < CONFIDENCE_THRESHOLD:
            final_label = "other-uninformative"
            warn("triage_lowconf", parent=parent, top=top_label,
                 score=round(top_score, 4), fallback=final_label)
        elif margin < AMBIGUOUS_MARGIN:
            top_is_kept = top_label in KEPT_CLASSES
            runner_is_kept = runner_label in KEPT_CLASSES
            if top_is_kept != runner_is_kept:
                final_label = top_label if top_is_kept else runner_label
                warn("triage_ambiguous_kept", parent=parent,
                     top=top_label, top_score=round(top_score, 4),
                     runner=runner_label, runner_score=round(runner_score, 4),
                     margin=round(margin, 4), chose=final_label)
            else:
                warn("triage_ambiguous", parent=parent,
                     top=top_label, runner=runner_label, margin=round(margin, 4))

        results.append(TriageResult(
            label=final_label,
            confidence=top_score,
            margin=margin,
            all_scores=all_scores,
        ))
    return results


@torch.inference_mode()
def classify(
    images: list[Image.Image],
    lm: LoadedModel,
    class_bank: dict[str, torch.Tensor],
    *,
    parent_ids: list[str] | None = None,
) -> list[TriageResult]:
    if not images:
        return []
    inputs = lm.processor(images=images, return_tensors="pt")
    inputs = {k: v.to(lm.device) for k, v in inputs.items()}
    img_feats = _unwrap_features(lm.model.get_image_features(**inputs))
    img_feats = _l2_normalize(img_feats.to(lm.dtype))

    stacked = torch.stack([class_bank[c] for c in ALL_CLASSES], dim=0)
    logits = img_feats @ stacked.T
    probs = torch.softmax(logits.float() * SOFTMAX_TEMPERATURE, dim=-1)

    return _decide_from_scores(probs, parent_ids)


def is_kept(label: str) -> bool:
    return label in KEPT_CLASSES


def is_dropped(label: str) -> bool:
    return label in DROPPED_CLASSES
