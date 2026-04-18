"""Image and text encoders built on top of a LoadedModel.

Returns L2-normalized, CPU-side float32 numpy arrays so downstream code
(store, query) does not need to know anything about torch devices or dtypes.
"""
from __future__ import annotations

import numpy as np
import torch
from PIL import Image

from image_search.common.model import LoadedModel
from image_search.common.warn import warn


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


def _check_nan(arr: np.ndarray, context: str) -> np.ndarray:
    """Drop rows with NaN/inf and emit [WARN] per row. Returns the cleaned array
    and a boolean mask of kept rows."""
    bad = ~np.isfinite(arr).all(axis=-1)
    if bad.any():
        for i in np.where(bad)[0]:
            warn("nan_embedding", context=context, row=int(i))
    return arr, ~bad


@torch.inference_mode()
def encode_images(images: list[Image.Image], lm: LoadedModel,
                  *, context: str = "embed") -> tuple[np.ndarray, np.ndarray]:
    """Encode `images`. Returns (features fp32 (N, D), keep_mask bool (N,)).
    Rows with NaN/inf are masked out (keep_mask[i] = False) after a [WARN].
    """
    if not images:
        return np.zeros((0, lm.projection_dim), dtype=np.float32), np.zeros((0,), dtype=bool)

    inputs = lm.processor(images=images, return_tensors="pt")
    inputs = {k: v.to(lm.device) for k, v in inputs.items()}
    feats = _unwrap_features(lm.model.get_image_features(**inputs))
    feats = _l2_normalize(feats.to(torch.float32))  # upcast once before leaving device
    arr = feats.detach().cpu().numpy()
    return _check_nan(arr, context)


@torch.inference_mode()
def encode_text(texts: list[str], lm: LoadedModel,
                *, context: str = "embed_text") -> tuple[np.ndarray, np.ndarray]:
    if not texts:
        return np.zeros((0, lm.projection_dim), dtype=np.float32), np.zeros((0,), dtype=bool)
    inputs = lm.processor(text=texts, padding="max_length",
                          truncation=True, return_tensors="pt")
    inputs = {k: v.to(lm.device) for k, v in inputs.items()}
    feats = _unwrap_features(lm.model.get_text_features(**inputs))
    feats = _l2_normalize(feats.to(torch.float32))
    arr = feats.detach().cpu().numpy()
    return _check_nan(arr, context)
