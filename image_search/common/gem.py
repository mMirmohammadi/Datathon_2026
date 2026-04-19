"""GeM (Generalized Mean) pooling over patch tokens.

Reference formula (Radenovic et al. 2018, arXiv:1711.02512):

    f_d = ( (1/N) * sum_i x_{i,d}^p )^(1/p)

for a feature map with N tokens, each D-dimensional.

Key implementation choice: GeM was originally designed for CNN+ReLU features
(non-negative by construction). DINOv2's ``x_norm_patchtokens`` are the output
of a final LayerNorm and can be negative. The canonical convention across every
production VPR impl I verified (cnnimageretrieval-pytorch, gmberton's benchmark,
MixVPR, OpenVPRLab, naver/deep-image-retrieval) is ``clamp(min=eps)`` before the
power --- negatives are mapped to eps and contribute ~0 to the pooled mean.

See _context/strategy_visual_reverse_search.md (the grounded version) and the
pre-implementation research in the session log for the full cross-check.
"""
from __future__ import annotations

import torch
import torch.nn.functional as F


DEFAULT_P: float = 3.0
DEFAULT_EPS: float = 1e-6


def gem_pool(
    patches: torch.Tensor,
    *,
    p: float = DEFAULT_P,
    eps: float = DEFAULT_EPS,
    l2_normalize: bool = True,
) -> torch.Tensor:
    """GeM-pool patch tokens along dim 1 (the token dim).

    Input shape:  [B, N, D]  (batch, patches, features)
    Output shape: [B, D]     (one descriptor per batch item)

    Steps:
        1. clamp(min=eps)  -> negatives and zeros -> eps
        2. pow(p)          -> elementwise
        3. mean(dim=1)     -> pool across N tokens
        4. pow(1/p)        -> inverse power
        5. optional L2-normalize along dim 1

    Raises ValueError on unexpected input shape so caller sees a hard failure
    rather than silent shape-mismatch at storage time.
    """
    if patches.dim() != 3:
        raise ValueError(
            f"gem_pool expects [B, N, D] (3 dims); got {tuple(patches.shape)}"
        )
    if p <= 0:
        raise ValueError(f"p must be > 0 (GeM is undefined otherwise); got {p}")
    if eps <= 0:
        raise ValueError(f"eps must be > 0; got {eps}")

    x = patches.clamp(min=eps).pow(p)            # [B, N, D]
    x = x.mean(dim=1)                            # [B, D]
    x = x.pow(1.0 / p)                           # [B, D]
    if l2_normalize:
        x = F.normalize(x, p=2.0, dim=1)         # [B, D]
    return x
