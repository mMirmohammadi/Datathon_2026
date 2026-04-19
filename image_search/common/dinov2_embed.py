"""DINOv2 image encoder: preprocess -> forward -> x_norm_patchtokens -> GeM -> L2.

Produces one fp32 L2-normalized numpy row per input PIL image.

Autocast policy:
    On CUDA:  model.forward_features runs under autocast(bf16).
              Patch tokens are cast to fp32 BEFORE GeM.pow(p) to avoid
              overflow -- bf16 has only 8 mantissa bits, and x^3 for |x|>~5
              loses meaningful precision. fp32 pow(3) is safe for all
              realistic DINOv2 activations.
    On MPS:   autocast(fp16).
    On CPU:   no autocast, all fp32.

Output contract:
    encode_images(imgs, lm) -> (feats: (N, D) fp32 L2-unit, keep_mask: (N,) bool)
    Rows with NaN/inf are masked out after a [WARN] nan_embedding_dinov2.
"""
from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import numpy as np
import torch
from PIL import Image

from image_search.common.dinov2_model import LoadedDinov2
from image_search.common.dinov2_transform import preprocess_batch
from image_search.common.gem import DEFAULT_EPS, DEFAULT_P, gem_pool
from image_search.common.warn import warn


PATCH_TOKENS_KEY: str = "x_norm_patchtokens"


@contextmanager
def _maybe_autocast(device: str, dtype: torch.dtype) -> Iterator[None]:
    """Enable autocast on cuda/mps with the device's preferred low-precision
    dtype. CPU stays in fp32 (autocast on CPU is bf16-only and slower here)."""
    if device == "cuda":
        with torch.autocast(device_type="cuda", dtype=dtype):
            yield
    elif device == "mps":
        with torch.autocast(device_type="mps", dtype=dtype):
            yield
    else:
        yield


def _check_nan(arr: np.ndarray, context: str) -> tuple[np.ndarray, np.ndarray]:
    bad = ~np.isfinite(arr).all(axis=-1)
    if bad.any():
        for i in np.where(bad)[0]:
            warn("nan_embedding_dinov2", context=context, row=int(i),
                 fallback="drop row")
    return arr, ~bad


@torch.inference_mode()
def encode_images(
    images: list[Image.Image],
    lm: LoadedDinov2,
    *,
    p: float = DEFAULT_P,
    eps: float = DEFAULT_EPS,
    context: str = "dinov2_embed",
) -> tuple[np.ndarray, np.ndarray]:
    """Run DINOv2 ViT-L/14 on `images`, GeM-pool patch tokens, L2-normalize.

    Returns:
        feats     : (N_kept, D) fp32 numpy, L2-unit rows (D = lm.embed_dim = 1024)
        keep_mask : (N_input,) bool -- True for rows that made it through
                    NaN/inf filtering. Caller uses keep_mask to filter
                    aligned metadata lists (image_id, source, ...).
    """
    if not images:
        empty = np.zeros((0, lm.embed_dim), dtype=np.float32)
        return empty, np.zeros((0,), dtype=bool)

    x = preprocess_batch(images).to(lm.device, non_blocking=True)
    with _maybe_autocast(lm.device, lm.dtype):
        out = lm.model.forward_features(x)

    if PATCH_TOKENS_KEY not in out:
        raise KeyError(
            f"DINOv2 forward_features returned no '{PATCH_TOKENS_KEY}' "
            f"key; got {sorted(out.keys())}. Model version mismatch?"
        )
    patches = out[PATCH_TOKENS_KEY]  # [B, N, D] in autocast dtype

    # Cast to fp32 BEFORE GeM power to avoid bf16/fp16 overflow in pow(p).
    patches = patches.to(torch.float32)

    # Geometry invariant -- hard-fail if the model returned unexpected shape
    # (would misalign stored descriptors with the metadata rows).
    B, N, D = patches.shape
    if D != lm.embed_dim:
        raise ValueError(
            f"DINOv2 patch tokens D={D} != expected embed_dim={lm.embed_dim}"
        )

    pooled = gem_pool(patches, p=p, eps=eps, l2_normalize=True)  # [B, D] fp32 unit
    arr = pooled.detach().cpu().numpy()
    return _check_nan(arr, context)
