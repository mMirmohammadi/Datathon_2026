"""SigLIP 2 loader with device/dtype selection.

Device and dtype policy (read research in the plan file):

    CUDA           → bf16 (preferred on Ampere+ per HF docs)
    MPS (Apple)    → fp16 (safer default; bf16 on MPS landed in torch 2.6 but
                     is less tested. We can revisit once a local benchmark
                     confirms bf16 is numerically stable on this box.)
    CPU            → fp32 + [WARN] device_fallback_cpu

The Giant checkpoint is ~3.74 GB at fp16, ~7.48 GB at fp32. First load triggers
a Hugging Face cache download; subsequent loads are fast.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import torch
from transformers import AutoConfig, AutoModel, AutoProcessor

from image_search.common.warn import warn


# Model IDs — verified against the public Google collection on HF.
GIANT_MODEL_ID = "google/siglip2-giant-opt-patch16-384"
SO400M_MODEL_ID = "google/siglip2-so400m-patch16-384"
TINY_MODEL_ID = "google/siglip2-base-patch16-384"  # cheap checkpoint used in unit tests


@dataclass
class LoadedModel:
    model_id: str
    device: str        # "cuda" | "mps" | "cpu"
    dtype: torch.dtype
    model: Any
    processor: Any
    projection_dim: int
    image_size: int


def select_device() -> tuple[str, torch.dtype]:
    if torch.cuda.is_available():
        return "cuda", torch.bfloat16
    if torch.backends.mps.is_available():
        return "mps", torch.float16
    warn("device_fallback_cpu", expected="cuda or mps", got="cpu",
         fallback="float32 on cpu (will be very slow)")
    return "cpu", torch.float32


def _discover_projection_dim(cfg: Any) -> int:
    # transformers 5.x unifies siglip/siglip2 under model_type="siglip";
    # the projection dim lives at text_config.projection_size on current checkpoints.
    for obj, attr in [
        (cfg.text_config, "projection_size"),
        (cfg.text_config, "projection_dim"),
        (cfg, "projection_dim"),
        (cfg.vision_config, "projection_size"),
        (cfg.vision_config, "hidden_size"),  # last-resort fallback
    ]:
        val = getattr(obj, attr, None)
        if val is not None:
            return int(val)
    raise RuntimeError(f"could not discover projection_dim from config: {cfg}")


def load(model_id: str = GIANT_MODEL_ID, *, device: str | None = None,
         dtype: torch.dtype | None = None) -> LoadedModel:
    cfg = AutoConfig.from_pretrained(model_id)
    if device is None or dtype is None:
        sel_device, sel_dtype = select_device()
        device = device or sel_device
        dtype = dtype or sel_dtype

    model = AutoModel.from_pretrained(model_id, dtype=dtype).eval().to(device)
    processor = AutoProcessor.from_pretrained(model_id)

    return LoadedModel(
        model_id=model_id,
        device=device,
        dtype=dtype,
        model=model,
        processor=processor,
        projection_dim=_discover_projection_dim(cfg),
        image_size=int(cfg.vision_config.image_size),
    )
