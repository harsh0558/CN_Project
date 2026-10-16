"""Generate a deterministic, seeded binary test file for transfer experiments."""

from __future__ import annotations

import argparse
import hashlib
import random
import sys
from pathlib import Path

# Add project root to sys.path so the tool can be run directly from any directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rtp.config import DEFAULT_FILE_SIZE


def generate_file(
    output_path: str | Path,
    size: int = DEFAULT_FILE_SIZE,
    seed: int = 42,
    chunk_size: int = 65536,
) -> str:
    """Generate a deterministic file of exact size using seeded pseudo-random bytes.

    Returns the hex-encoded SHA-256 digest of the generated file.
    """
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    rng = random.Random(seed)
    hasher = hashlib.sha256()
    bytes_remaining = size

    with open(path, "wb") as f:
        while bytes_remaining > 0:
            current_chunk = min(chunk_size, bytes_remaining)
            chunk = rng.randbytes(current_chunk)
            f.write(chunk)
            hasher.update(chunk)
            bytes_remaining -= current_chunk

    sha256_hex = hasher.hexdigest()
    return sha256_hex


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic test file.")
    parser.add_argument(
        "--size",
        type=int,
        default=DEFAULT_FILE_SIZE,
        help=f"File size in bytes (default: {DEFAULT_FILE_SIZE})",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="test_20mb.bin",
        help="Output file path (default: test_20mb.bin)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42)",
    )
    args = parser.parse_args()

    sha256 = generate_file(args.output, size=args.size, seed=args.seed)
    print(f"Generated {args.size} bytes to '{args.output}'")
    print(f"Seed: {args.seed}")
    print(f"SHA-256: {sha256}")


if __name__ == "__main__":
    main()
