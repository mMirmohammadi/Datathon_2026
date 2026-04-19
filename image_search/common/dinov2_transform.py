"""Canonical DINOv2 eval transform.

Direct transcription of facebookresearch/dinov2/dinov2/data/transforms.py
make_classification_eval_transform(), with default parameters:

    Resize(resize_size=256, interpolation=BICUBIC)
    CenterCrop(crop_size=224)
    ToTensor()                      -> [0, 1] float tensor
    Normalize(ImageNet mean/std)

Pixel order: RGB.

The Resize with a single int arg takes the SHORTER side to 256 (torchvision
convention), preserving aspect ratio. Then CenterCrop to 224x224. For our
71k-image corpus this yields 16x16 = 256 patch tokens per image with ViT-L/14
(stride 14, 224/14 = 16).
"""
from __future__ import annotations

import torch
from PIL import Image
from torchvision import transforms as T
from torchvision.transforms import InterpolationMode


IMAGENET_MEAN: tuple[float, float, float] = (0.485, 0.456, 0.406)
IMAGENET_STD: tuple[float, float, float] = (0.229, 0.224, 0.225)

RESIZE_SIZE: int = 256
CROP_SIZE: int = 224


def make_eval_transform() -> T.Compose:
    """Return the canonical DINOv2 classification/retrieval eval transform."""
    return T.Compose([
        T.Resize(RESIZE_SIZE, interpolation=InterpolationMode.BICUBIC),
        T.CenterCrop(CROP_SIZE),
        T.ToTensor(),
        T.Normalize(mean=IMAGENET_MEAN, std=IMAGENET_STD),
    ])


def preprocess_one(img: Image.Image, tf: T.Compose | None = None) -> torch.Tensor:
    """Apply the eval transform to one PIL image. Returns a [3, 224, 224] fp32 tensor."""
    if img.mode != "RGB":
        img = img.convert("RGB")
    t = tf if tf is not None else make_eval_transform()
    out = t(img)
    # Invariants -- hard-fail rather than drift silently
    assert out.shape == (3, CROP_SIZE, CROP_SIZE), out.shape
    assert out.dtype == torch.float32, out.dtype
    return out


def preprocess_batch(imgs: list[Image.Image]) -> torch.Tensor:
    """Preprocess a list of PIL images. Returns a [B, 3, 224, 224] fp32 tensor."""
    if not imgs:
        return torch.empty(0, 3, CROP_SIZE, CROP_SIZE, dtype=torch.float32)
    tf = make_eval_transform()
    return torch.stack([preprocess_one(i, tf) for i in imgs], dim=0)
