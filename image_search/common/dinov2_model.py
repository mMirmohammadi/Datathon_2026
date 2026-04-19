"""Lazy loader for DINOv2 ViT-L/14 (register-token variant).

We use ``dinov2_vitl14_reg`` (Darcet et al. 2023, arXiv:2309.16588) over the
plain ``dinov2_vitl14`` for two reasons verified against Meta's MODEL_CARD.md
in the official repo (github.com/facebookresearch/dinov2):

  1. Retrieval quality is measurably better: Oxford-H mAP 55.7 vs 54.0 for
     ViT-L/14. (The reg variant was trained to fix the "artifact tokens"
     pathology where a few high-norm patch tokens absorb global semantics
     and wash out dense features --- exactly what would hurt GeM pooling.)
  2. Patch tokens returned by ``forward_features(x)["x_norm_patchtokens"]``
     are cleaner (no artifact outliers) for pooling.

Device / dtype policy (see notes at the top of the session log):

    CUDA + torch>=2.4 cu12x -> bf16 autocast for forward; always return fp32 on host
    MPS (Apple)             -> fp16 autocast
    CPU                     -> fp32, [WARN] device_fallback_cpu

Blackwell (sm_120) note: DINOv2 internally prefers xformers memory-efficient
attention. pre-built xformers wheels historically lag Blackwell. If the hub
load prints a "xFormers not available" warning, that is expected; the fallback
path uses PyTorch SDPA which supports sm_120 in cu128 builds.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import torch

from image_search.common.warn import warn


# Hub entry name -- resolved by github.com/facebookresearch/dinov2/hubconf.py.
DINOV2_HUB_REPO: str = "facebookresearch/dinov2"
DINOV2_HUB_ENTRY: str = "dinov2_vitl14_reg"

# ViT-L/14 fixed geometry.
PATCH_SIZE: int = 14
EMBED_DIM: int = 1024
NUM_REG_TOKENS: int = 4
# At canonical eval crop 224x224: 224/14 = 16, so 16*16 = 256 patch tokens.
DEFAULT_INPUT_SIZE: int = 224
DEFAULT_NUM_PATCHES: int = (DEFAULT_INPUT_SIZE // PATCH_SIZE) ** 2  # 256


@dataclass
class LoadedDinov2:
    device: str            # "cuda" | "mps" | "cpu"
    dtype: torch.dtype     # autocast dtype for forward
    model: Any             # torch.nn.Module
    embed_dim: int
    patch_size: int
    input_size: int        # 224 for our Tier-1 recipe
    entry: str             # hub entry used (for audit)


def select_device() -> tuple[str, torch.dtype]:
    """Pick device + autocast dtype, consistent with image_search/common/model.py."""
    if torch.cuda.is_available():
        return "cuda", torch.bfloat16
    if torch.backends.mps.is_available():
        return "mps", torch.float16
    warn("device_fallback_cpu", expected="cuda or mps", got="cpu",
         fallback="fp32 on cpu (very slow for 71k images)")
    return "cpu", torch.float32


def load(
    *,
    device: str | None = None,
    dtype: torch.dtype | None = None,
    entry: str = DINOV2_HUB_ENTRY,
    input_size: int = DEFAULT_INPUT_SIZE,
) -> LoadedDinov2:
    """Load DINOv2 via torch.hub. First call downloads the checkpoint to the
    torch hub cache (~300 MB for ViT-L). Subsequent calls reuse the cache.

    Hard-validates: input_size must be a multiple of PATCH_SIZE (14), or DINOv2
    will silently crop to the nearest-lower multiple. We refuse to run on a
    non-aligned size so every indexed row uses the exact same geometry.
    """
    if input_size % PATCH_SIZE != 0:
        raise ValueError(
            f"input_size {input_size} must be a multiple of patch size "
            f"{PATCH_SIZE} (DINOv2 crops to floor(size/14)*14 otherwise)"
        )

    sel_device, sel_dtype = select_device()
    device = device or sel_device
    dtype = dtype or sel_dtype

    # torch.hub loads and caches weights; pretrained=True is the default for
    # the backbone entries. We do not pass an explicit flag here, tracking the
    # published API in dinov2/hubconf.py.
    model = torch.hub.load(DINOV2_HUB_REPO, entry, verbose=False)
    model.eval()
    model.to(device)

    return LoadedDinov2(
        device=device,
        dtype=dtype,
        model=model,
        embed_dim=EMBED_DIM,
        patch_size=PATCH_SIZE,
        input_size=input_size,
        entry=entry,
    )
