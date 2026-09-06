#!/usr/bin/env python3
"""Offline file splitter/joiner with text and MessagePack frame formats."""

from __future__ import annotations

import argparse
import asyncio
import base64
import binascii
import hashlib
import html
import http.server
import json
import math
import os
import re
import socket
import socketserver
import struct
import sys
import tempfile
import ssl
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse, request

try:
    import msgpack
except ImportError:  # pragma: no cover - depends on the host environment.
    msgpack = None

TEXT_FORMAT = "OFFSPLIT/1"
MSGPACK_FORMAT = "OFFSPLIT/2"
TEXT_MAGIC = "OFFSPLIT/1"
MSGPACK_META_MAGIC = b"OFFSPLIT-META/2\n"
MSGPACK_FRAME_MAGIC = b"OFFSPLIT-FRAME/2\n"
HEADER_PREFIX = "HEADER "
FRAME_PREFIX = "FRAME "
DEFAULT_CHUNK_SIZE = 1024 * 1024
SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


class FrameError(Exception):
    """Raised when framed data is malformed or fails validation."""


def parse_size(value: str) -> int:
    units = {
        "b": 1,
        "k": 1024,
        "kb": 1024,
        "m": 1024**2,
        "mb": 1024**2,
        "g": 1024**3,
        "gb": 1024**3,
    }
    match = re.fullmatch(r"(\d+)([A-Za-z]+)?", value.strip())
    if not match:
        raise argparse.ArgumentTypeError("size must look like 1024, 512k, 10m, or 1g")
    number = int(match.group(1))
    unit = (match.group(2) or "b").lower()
    if unit not in units:
        raise argparse.ArgumentTypeError(f"unknown size unit: {unit}")
    size = number * units[unit]
    if size <= 0:
        raise argparse.ArgumentTypeError("size must be greater than zero")
    return size


def safe_stem(name: str) -> str:
    stem = SAFE_NAME_RE.sub("_", name).strip("._")
    return stem or "data"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def metadata_hash(metadata: dict[str, Any]) -> str:
    keys = ("format", "transfer_id", "filename", "size", "sha256", "chunk_size", "total_frames")
    stable_metadata = {key: metadata.get(key) for key in keys}
    encoded = json.dumps(stable_metadata, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()[:16]


def replace_file(path: Path, data: bytes) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("wb") as file:
        file.write(data)
    os.replace(temp_path, path)


def write_text_part(path: Path, metadata: dict[str, Any], sequence: int, chunk: bytes) -> None:
    crc = binascii.crc32(chunk) & 0xFFFFFFFF
    encoded = base64.b64encode(chunk).decode("ascii")
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", encoding="ascii", newline="\n") as file:
        file.write(f"{TEXT_MAGIC}\n")
        file.write(f"{HEADER_PREFIX}{json.dumps(metadata, sort_keys=True, separators=(',', ':'))}\n")
        file.write(f"{FRAME_PREFIX}{sequence} {len(chunk)} {crc:08x} {encoded}\n")
    os.replace(temp_path, path)


def require_msgpack() -> None:
    if msgpack is None:
        raise FrameError("MessagePack format needs the Python 'msgpack' module")


def packb(value: dict[str, Any]) -> bytes:
    require_msgpack()
    return msgpack.packb(value, use_bin_type=True)


def unpackb(path: Path, payload: bytes) -> dict[str, Any]:
    require_msgpack()
    try:
        value = msgpack.unpackb(payload, raw=False)
    except Exception as exc:
        raise FrameError(f"{path}: invalid MessagePack payload: {exc}") from exc
    if not isinstance(value, dict):
        raise FrameError(f"{path}: MessagePack payload must be a map")
    return value


def write_msgpack_manifest(path: Path, metadata: dict[str, Any]) -> None:
    payload = {"type": "metadata", **metadata}
    replace_file(path, MSGPACK_META_MAGIC + packb(payload))


def write_msgpack_part(path: Path, metadata: dict[str, Any], sequence: int, chunk: bytes) -> None:
    payload = {
        "type": "frame",
        "format": MSGPACK_FORMAT,
        "transfer_id": metadata["transfer_id"],
        "metadata_hash": metadata_hash(metadata),
        "seq": sequence,
        "len": len(chunk),
        "crc32": binascii.crc32(chunk) & 0xFFFFFFFF,
        "data": chunk,
    }
    replace_file(path, MSGPACK_FRAME_MAGIC + packb(payload))


def write_part(path: Path, metadata: dict[str, Any], sequence: int, chunk: bytes, frame_format: str) -> None:
    if frame_format == "text":
        write_text_part(path, metadata, sequence, chunk)
    elif frame_format == "msgpack":
        write_msgpack_part(path, metadata, sequence, chunk)
    else:
        raise FrameError(f"unknown format: {frame_format}")


def part_matches(path: Path, expected_metadata: dict[str, Any], sequence: int, chunk: bytes) -> bool:
    if not path.exists():
        return False
    try:
        metadata, existing_sequence, existing_chunk = read_part(path, expected_metadata)
    except FrameError:
        return False
    return (
        metadata_key(metadata) == metadata_key(expected_metadata)
        and existing_sequence == sequence
        and existing_chunk == chunk
    )


def command_split(args: argparse.Namespace) -> int:
    source = Path(args.input).expanduser().resolve()
    if not source.is_file():
        raise FrameError(f"input is not a file: {source}")

    output_dir = Path(args.output).expanduser().resolve() if args.output else source.with_suffix(source.suffix + ".parts")
    output_dir.mkdir(parents=True, exist_ok=True)

    file_size = source.stat().st_size
    total_frames = math.ceil(file_size / args.chunk_size) if file_size else 1
    source_sha256 = sha256_file(source)
    transfer_id = hashlib.sha256(
        f"{source.name}\0{file_size}\0{source_sha256}".encode("utf-8")
    ).hexdigest()[:16]
    frame_format = args.format
    metadata = {
        "format": MSGPACK_FORMAT if frame_format == "msgpack" else TEXT_FORMAT,
        "transfer_id": transfer_id,
        "filename": source.name,
        "size": file_size,
        "sha256": source_sha256,
        "chunk_size": args.chunk_size,
        "total_frames": total_frames,
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }

    prefix = safe_stem(source.name)
    if frame_format == "msgpack":
        manifest_path = output_dir / f"{prefix}.{transfer_id}.ofm"
        if not args.resume or not manifest_path.exists():
            write_msgpack_manifest(manifest_path, metadata)

    written_frames = 0
    resumed_frames = 0
    with source.open("rb") as file:
        for sequence in range(total_frames):
            chunk = file.read(args.chunk_size)
            if frame_format == "msgpack":
                part_path = output_dir / f"{prefix}.{transfer_id}.{sequence:06d}.ofs"
            else:
                part_path = output_dir / f"{prefix}.{sequence:06d}.ofs"
            if args.resume and part_matches(part_path, metadata, sequence, chunk):
                resumed_frames += 1
                continue
            write_part(part_path, metadata, sequence, chunk, frame_format)
            written_frames += 1

    print(f"split: {source}")
    print(f"format: {frame_format}")
    print(f"parts: {total_frames} file(s) in {output_dir}")
    print(f"written: {written_frames}")
    print(f"resumed: {resumed_frames}")
    print(f"id: {transfer_id}")
    return 0


def read_text_part(path: Path) -> tuple[dict[str, Any], int, bytes]:
    with path.open("r", encoding="ascii", newline="") as file:
        magic = file.readline().rstrip("\n")
        header_line = file.readline().rstrip("\n")
        frame_line = file.readline().rstrip("\n")
        trailing = file.readline()

    if magic != TEXT_MAGIC:
        raise FrameError(f"{path}: bad magic")
    if not header_line.startswith(HEADER_PREFIX):
        raise FrameError(f"{path}: missing header")
    if not frame_line.startswith(FRAME_PREFIX):
        raise FrameError(f"{path}: missing frame")
    if trailing:
        raise FrameError(f"{path}: expected exactly one frame")

    try:
        metadata = json.loads(header_line[len(HEADER_PREFIX) :])
    except json.JSONDecodeError as exc:
        raise FrameError(f"{path}: invalid header JSON: {exc}") from exc

    fields = frame_line[len(FRAME_PREFIX) :].split(" ", 3)
    if len(fields) != 4:
        raise FrameError(f"{path}: invalid frame fields")

    try:
        sequence = int(fields[0])
        expected_len = int(fields[1])
        expected_crc = int(fields[2], 16)
        chunk = base64.b64decode(fields[3], validate=True)
    except (ValueError, binascii.Error) as exc:
        raise FrameError(f"{path}: invalid frame encoding: {exc}") from exc

    if expected_len != len(chunk):
        raise FrameError(f"{path}: length mismatch, expected {expected_len}, got {len(chunk)}")
    actual_crc = binascii.crc32(chunk) & 0xFFFFFFFF
    if expected_crc != actual_crc:
        raise FrameError(f"{path}: CRC mismatch, expected {expected_crc:08x}, got {actual_crc:08x}")

    return metadata, sequence, chunk


def read_msgpack_manifest(path: Path) -> dict[str, Any]:
    data = path.read_bytes()
    if not data.startswith(MSGPACK_META_MAGIC):
        raise FrameError(f"{path}: bad MessagePack manifest magic")
    metadata = unpackb(path, data[len(MSGPACK_META_MAGIC) :])
    if metadata.get("type") != "metadata":
        raise FrameError(f"{path}: not a metadata manifest")
    metadata.pop("type", None)
    if metadata.get("format") != MSGPACK_FORMAT:
        raise FrameError(f"{path}: unsupported MessagePack format")
    return metadata


def read_msgpack_part(path: Path, metadata: dict[str, Any]) -> tuple[dict[str, Any], int, bytes]:
    data = path.read_bytes()
    if not data.startswith(MSGPACK_FRAME_MAGIC):
        raise FrameError(f"{path}: bad MessagePack frame magic")
    frame = unpackb(path, data[len(MSGPACK_FRAME_MAGIC) :])
    if frame.get("type") != "frame":
        raise FrameError(f"{path}: not a data frame")
    if frame.get("format") != MSGPACK_FORMAT:
        raise FrameError(f"{path}: unsupported MessagePack frame format")
    if frame.get("transfer_id") != metadata.get("transfer_id"):
        raise FrameError(f"{path}: transfer_id does not match manifest")
    if frame.get("metadata_hash") != metadata_hash(metadata):
        raise FrameError(f"{path}: metadata_hash does not match manifest")

    try:
        sequence = int(frame["seq"])
        expected_len = int(frame["len"])
        expected_crc = int(frame["crc32"])
    except (KeyError, TypeError, ValueError) as exc:
        raise FrameError(f"{path}: invalid frame fields: {exc}") from exc

    chunk = frame.get("data")
    if not isinstance(chunk, bytes):
        raise FrameError(f"{path}: frame data must be bytes")
    if expected_len != len(chunk):
        raise FrameError(f"{path}: length mismatch, expected {expected_len}, got {len(chunk)}")
    actual_crc = binascii.crc32(chunk) & 0xFFFFFFFF
    if expected_crc != actual_crc:
        raise FrameError(f"{path}: CRC mismatch, expected {expected_crc:08x}, got {actual_crc:08x}")
    return metadata, sequence, chunk


def read_part(path: Path, metadata: dict[str, Any] | None = None) -> tuple[dict[str, Any], int, bytes]:
    with path.open("rb") as file:
        prefix = file.read(max(len(MSGPACK_FRAME_MAGIC), len(TEXT_MAGIC)))
    if prefix.startswith(TEXT_MAGIC.encode("ascii")):
        return read_text_part(path)
    if prefix.startswith(MSGPACK_FRAME_MAGIC):
        if metadata is None:
            raise FrameError(f"{path}: MessagePack frame needs a manifest")
        return read_msgpack_part(path, metadata)
    raise FrameError(f"{path}: unknown frame format")


def metadata_key(metadata: dict[str, Any]) -> tuple[Any, ...]:
    keys = ("format", "transfer_id", "filename", "size", "sha256", "chunk_size", "total_frames")
    return tuple(metadata.get(key) for key in keys)


def collect_part_paths(input_path: Path) -> list[Path]:
    if input_path.is_dir():
        paths = sorted(path for path in input_path.iterdir() if path.is_file() and path.suffix == ".ofs")
    elif input_path.is_file():
        paths = [input_path]
    else:
        raise FrameError(f"input is not a file or directory: {input_path}")
    if not paths:
        raise FrameError(f"no .ofs part files found in: {input_path}")
    return paths


def collect_msgpack_manifest(input_path: Path) -> tuple[Path, Path]:
    if input_path.is_file() and input_path.suffix == ".ofm":
        return input_path, input_path.parent
    if input_path.is_file() and input_path.suffix == ".ofs":
        manifests = sorted(input_path.parent.glob("*.ofm"))
        if len(manifests) != 1:
            raise FrameError(f"{input_path}: MessagePack frame needs exactly one .ofm manifest in the same directory")
        return manifests[0], input_path.parent
    if input_path.is_dir():
        manifests = sorted(input_path.glob("*.ofm"))
        if not manifests:
            raise FrameError(f"no .ofm manifest found in: {input_path}")
        if len(manifests) != 1:
            raise FrameError(f"multiple .ofm manifests found in {input_path}; join one manifest file directly")
        return manifests[0], input_path
    raise FrameError(f"input is not a file or directory: {input_path}")


def load_text_frames(input_path: Path) -> tuple[dict[str, Any], dict[int, bytes]]:
    paths = collect_part_paths(input_path)

    first_metadata: dict[str, Any] | None = None
    first_key: tuple[Any, ...] | None = None
    frames: dict[int, bytes] = {}

    for path in paths:
        metadata, sequence, chunk = read_part(path)
        current_key = metadata_key(metadata)
        if first_metadata is None:
            first_metadata = metadata
            first_key = current_key
        elif current_key != first_key:
            raise FrameError(f"{path}: metadata does not match the first part")
        if sequence in frames:
            raise FrameError(f"{path}: duplicate frame sequence {sequence}")
        frames[sequence] = chunk

    assert first_metadata is not None
    return first_metadata, frames


def load_msgpack_frames(input_path: Path) -> tuple[dict[str, Any], dict[int, bytes]]:
    manifest_path, parts_dir = collect_msgpack_manifest(input_path)
    metadata = read_msgpack_manifest(manifest_path)
    transfer_id = metadata["transfer_id"]
    paths = sorted(path for path in parts_dir.iterdir() if path.is_file() and path.suffix == ".ofs")
    frames: dict[int, bytes] = {}

    for path in paths:
        try:
            frame_metadata, sequence, chunk = read_msgpack_part(path, metadata)
        except FrameError as exc:
            if "transfer_id does not match manifest" in str(exc):
                continue
            raise
        if metadata_key(frame_metadata) != metadata_key(metadata):
            raise FrameError(f"{path}: metadata does not match manifest")
        if sequence in frames:
            raise FrameError(f"{path}: duplicate frame sequence {sequence}")
        frames[sequence] = chunk

    if not frames:
        raise FrameError(f"no matching .ofs frames found for transfer_id {transfer_id}")
    return metadata, frames


def load_frames(input_path: Path) -> tuple[dict[str, Any], dict[int, bytes]]:
    if input_path.is_dir() and any(input_path.glob("*.ofm")):
        return load_msgpack_frames(input_path)
    if input_path.is_file() and input_path.suffix == ".ofm":
        return load_msgpack_frames(input_path)
    if input_path.is_file():
        try:
            return load_text_frames(input_path)
        except FrameError:
            return load_msgpack_frames(input_path)
    return load_text_frames(input_path)


def validate_complete_frames(metadata: dict[str, Any], frames: dict[int, bytes]) -> None:
    total_frames = int(metadata["total_frames"])
    missing = [sequence for sequence in range(total_frames) if sequence not in frames]
    if missing:
        preview = ", ".join(str(sequence) for sequence in missing[:10])
        suffix = "..." if len(missing) > 10 else ""
        raise FrameError(f"missing frame(s): {preview}{suffix}")


def validate_partial(partial_path: Path, frames: dict[int, bytes], chunk_size: int, expected_size: int) -> int:
    if not partial_path.exists():
        return 0

    partial_size = partial_path.stat().st_size
    if partial_size > expected_size:
        raise FrameError(f"partial output is larger than expected: {partial_path}")
    if partial_size == expected_size:
        return partial_size
    if chunk_size and partial_size % chunk_size != 0:
        raise FrameError(f"partial output stops mid-frame, remove it or retry without --resume: {partial_path}")

    checked = 0
    sequence = 0
    with partial_path.open("rb") as file:
        while checked < partial_size:
            expected_chunk = frames.get(sequence)
            if expected_chunk is None:
                raise FrameError(f"partial output needs missing frame {sequence}")
            data = file.read(len(expected_chunk))
            if data != expected_chunk:
                raise FrameError(f"partial output does not match frame {sequence}: {partial_path}")
            checked += len(data)
            sequence += 1
    return partial_size


def digest_existing(path: Path) -> tuple[hashlib._Hash, int]:
    digest = hashlib.sha256()
    written = 0
    with path.open("rb") as file:
        for block in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(block)
            written += len(block)
    return digest, written


def join_frames(input_path: Path, output: Path | None = None, force: bool = False, resume: bool = True) -> dict[str, Any]:
    input_path = input_path.expanduser().resolve()
    first_metadata, frames = load_frames(input_path)
    validate_complete_frames(first_metadata, frames)

    output = output.expanduser().resolve() if output else Path(first_metadata["filename"]).resolve()
    if output.exists() and not force:
        raise FrameError(f"output already exists, use --force to overwrite: {output}")

    partial_path = output.with_name(output.name + ".partial")
    expected_size = int(first_metadata["size"])
    chunk_size = int(first_metadata["chunk_size"])
    resumed_bytes = validate_partial(partial_path, frames, chunk_size, expected_size) if resume else 0

    if resumed_bytes:
        digest, written = digest_existing(partial_path)
        mode = "ab"
    else:
        digest = hashlib.sha256()
        written = 0
        mode = "wb"

    total_frames = int(first_metadata["total_frames"])
    start_sequence = total_frames if written == expected_size else written // chunk_size
    with partial_path.open(mode) as file:
        for sequence in range(start_sequence, total_frames):
            chunk = frames[sequence]
            file.write(chunk)
            digest.update(chunk)
            written += len(chunk)

    expected_sha256 = str(first_metadata["sha256"])
    actual_sha256 = digest.hexdigest()
    if written != expected_size:
        partial_path.unlink(missing_ok=True)
        raise FrameError(f"final size mismatch, expected {expected_size}, got {written}")
    if actual_sha256 != expected_sha256:
        partial_path.unlink(missing_ok=True)
        raise FrameError(f"final SHA256 mismatch, expected {expected_sha256}, got {actual_sha256}")

    os.replace(partial_path, output)
    return {
        "output": str(output),
        "frames": total_frames,
        "resumed_bytes": resumed_bytes,
        "sha256": actual_sha256,
    }


def command_join(args: argparse.Namespace) -> int:
    input_path = Path(args.input).expanduser().resolve()
    output = Path(args.output).expanduser().resolve() if args.output else None
    result = join_frames(input_path, output, args.force, args.resume)
    print(f"joined: {result['output']}")
    print(f"frames: {result['frames']}")
    print(f"resumed_bytes: {result['resumed_bytes']}")
    print(f"sha256: {result['sha256']}")
    return 0


def json_bytes(value: Any) -> bytes:
    return json.dumps(value, separators=(",", ":")).encode("utf-8")


def parse_json_bytes(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FrameError(f"invalid JSON body: {exc}") from exc
    if not isinstance(value, dict):
        raise FrameError("JSON body must be an object")
    return value


def scan_transfer(parts_dir: Path, manifest_path: Path) -> dict[str, Any]:
    metadata = read_msgpack_manifest(manifest_path)
    total_frames = int(metadata["total_frames"])
    received: list[int] = []
    invalid = 0
    for path in sorted(parts_dir.glob("*.ofs")):
        try:
            _, sequence, _ = read_msgpack_part(path, metadata)
        except FrameError as exc:
            if "transfer_id does not match manifest" in str(exc):
                continue
            invalid += 1
            continue
        received.append(sequence)
    received_set = set(received)
    missing = [sequence for sequence in range(total_frames) if sequence not in received_set]
    return {
        "transfer_id": metadata["transfer_id"],
        "filename": metadata["filename"],
        "size": metadata["size"],
        "sha256": metadata["sha256"],
        "chunk_size": metadata["chunk_size"],
        "total_frames": total_frames,
        "received_frames": len(received_set),
        "missing_frames": len(missing),
        "missing": missing,
        "invalid_frames": invalid,
        "complete": not missing and invalid == 0,
        "manifest": str(manifest_path),
    }


def list_transfers(inbox: Path) -> list[dict[str, Any]]:
    transfers = []
    for manifest_path in sorted(inbox.glob("*.ofm")):
        try:
            transfers.append(scan_transfer(inbox, manifest_path))
        except FrameError:
            continue
    return transfers


def find_manifest(inbox: Path, transfer_id: str) -> Path:
    matches = sorted(inbox.glob(f"*.{transfer_id}.ofm"))
    if not matches:
        raise FrameError(f"manifest not found for transfer_id {transfer_id}")
    if len(matches) > 1:
        raise FrameError(f"multiple manifests found for transfer_id {transfer_id}")
    return matches[0]


def frame_transfer_id(data: bytes, path: Path) -> str:
    if not data.startswith(MSGPACK_FRAME_MAGIC):
        raise FrameError("uploaded frame is not OFFSPLIT/2 MessagePack")
    frame = unpackb(path, data[len(MSGPACK_FRAME_MAGIC) :])
    transfer_id = frame.get("transfer_id")
    if not isinstance(transfer_id, str) or not transfer_id:
        raise FrameError("uploaded frame has no transfer_id")
    return transfer_id


def frame_sequence(data: bytes, path: Path) -> int:
    if not data.startswith(MSGPACK_FRAME_MAGIC):
        raise FrameError("uploaded frame is not OFFSPLIT/2 MessagePack")
    frame = unpackb(path, data[len(MSGPACK_FRAME_MAGIC) :])
    try:
        return int(frame["seq"])
    except (KeyError, TypeError, ValueError) as exc:
        raise FrameError(f"uploaded frame has invalid seq: {exc}") from exc


def manifest_name(metadata: dict[str, Any]) -> str:
    return f"{safe_stem(str(metadata['filename']))}.{metadata['transfer_id']}.ofm"


def frame_name(metadata: dict[str, Any], sequence: int) -> str:
    return f"{safe_stem(str(metadata['filename']))}.{metadata['transfer_id']}.{sequence:06d}.ofs"


WEB_APP = r"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Offsplit Transfer</title>
  <style>
    :root {
      --bg: #f4f6f8;
      --panel: #ffffff;
      --ink: #18202a;
      --muted: #667085;
      --line: #d9e0e7;
      --accent: #1f7a5a;
      --accent-2: #2459a8;
      --warn: #a15c00;
      --bad: #b42318;
      --shadow: 0 16px 40px rgba(24, 32, 42, .08);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      background: var(--bg);
      color: var(--ink);
      font: 14px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    header {
      min-height: 76px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
      padding: 18px 28px;
      border-bottom: 1px solid var(--line);
      background: #fbfcfd;
    }
    h1 { margin: 0; font-size: 21px; font-weight: 700; letter-spacing: 0; }
    main { max-width: 1180px; margin: 0 auto; padding: 24px; }
    .toolbar { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .grid { display: grid; grid-template-columns: 1.15fr .85fr; gap: 18px; align-items: start; }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      box-shadow: var(--shadow);
      overflow: hidden;
    }
    .panel h2 {
      margin: 0;
      padding: 16px 18px;
      font-size: 15px;
      border-bottom: 1px solid var(--line);
      background: #fbfcfd;
    }
    .content { padding: 18px; }
    .transfer {
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 14px;
      margin-bottom: 12px;
      background: #fff;
    }
    .transfer:last-child { margin-bottom: 0; }
    .row { display: flex; justify-content: space-between; gap: 14px; align-items: center; }
    .name { font-weight: 700; min-width: 0; overflow-wrap: anywhere; }
    .meta { color: var(--muted); font-size: 12px; margin-top: 3px; overflow-wrap: anywhere; }
    .bar { height: 9px; background: #e8edf2; border-radius: 999px; overflow: hidden; margin: 12px 0 8px; }
    .fill { height: 100%; background: var(--accent); width: 0%; }
    .pill {
      border: 1px solid var(--line);
      border-radius: 999px;
      padding: 4px 9px;
      font-size: 12px;
      color: var(--muted);
      white-space: nowrap;
      background: #fff;
    }
    label { display: block; color: var(--muted); font-size: 12px; margin: 0 0 6px; }
    input, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 7px;
      padding: 10px 11px;
      color: var(--ink);
      background: #fff;
      font: inherit;
      min-height: 40px;
    }
    .fields { display: grid; gap: 12px; }
    .two { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    button {
      border: 1px solid #1f6c50;
      border-radius: 7px;
      padding: 9px 12px;
      color: #fff;
      background: var(--accent);
      font-weight: 650;
      cursor: pointer;
      min-height: 38px;
    }
    button.secondary { background: #fff; color: var(--accent-2); border-color: #b8c7dc; }
    button.warn { background: var(--warn); border-color: var(--warn); }
    button:disabled { opacity: .55; cursor: not-allowed; }
    .actions { display: flex; gap: 8px; flex-wrap: wrap; margin-top: 14px; }
    .empty { color: var(--muted); padding: 18px; border: 1px dashed var(--line); border-radius: 8px; }
    .log {
      min-height: 88px;
      max-height: 190px;
      overflow: auto;
      white-space: pre-wrap;
      background: #101820;
      color: #dbe7ef;
      border-radius: 8px;
      padding: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      font-size: 12px;
    }
    @media (max-width: 850px) {
      header { align-items: flex-start; flex-direction: column; padding: 16px; }
      main { padding: 16px; }
      .grid, .two { grid-template-columns: 1fr; }
      .row { align-items: flex-start; flex-direction: column; }
    }
  </style>
</head>
<body>
  <header>
    <div>
      <h1>Offsplit Transfer</h1>
      <div class="meta">Framed file transfer with resume, CRC, and final SHA256 verification</div>
    </div>
    <div class="toolbar">
      <button class="secondary" id="refreshBtn">Refresh</button>
    </div>
  </header>
  <main class="grid">
    <section class="panel">
      <h2>Transfers</h2>
      <div class="content" id="transfers"><div class="empty">No transfer received yet.</div></div>
    </section>
    <section class="panel">
      <h2>Transfer Mode</h2>
      <div class="content fields">
        <div>
          <label for="transferInput">Source file path</label>
          <input id="transferInput" placeholder="/home/user/video.iso">
        </div>
        <div>
          <label for="partsOutput">Parts directory</label>
          <input id="partsOutput" placeholder="/home/user/video.parts">
        </div>
        <div class="two">
          <div>
            <label for="transferMode">Mode</label>
            <select id="transferMode">
              <option value="offline">Offline</option>
              <option value="http">HTTP</option>
              <option value="websocket">WebSocket</option>
              <option value="udp">UDP</option>
              <option value="quic">QUIC</option>
              <option value="mqtt">MQTT</option>
            </select>
          </div>
          <div>
            <label for="chunkSize">Chunk size</label>
            <input id="chunkSize" value="1m">
          </div>
        </div>
        <div>
          <label for="frameFormat">Frame format</label>
          <select id="frameFormat">
            <option value="msgpack">MessagePack</option>
            <option value="text">Text</option>
          </select>
        </div>
        <div id="networkFields">
          <label for="targetUrl">Target URL</label>
          <input id="targetUrl" placeholder="http://host:8080, ws://host:8090, udp://host:9000, quic://host:9443, or mqtt://broker:1883">
        </div>
        <div class="two">
          <div>
            <label for="authToken">Network token</label>
            <input id="authToken" placeholder="optional">
          </div>
          <div id="mqttTopicField">
            <label for="mqttTopic">MQTT topic</label>
            <input id="mqttTopic" value="offsplit">
          </div>
        </div>
        <div class="two" id="mqttAuthFields">
          <div>
            <label for="mqttUsername">MQTT username</label>
            <input id="mqttUsername" placeholder="optional">
          </div>
          <div>
            <label for="mqttPassword">MQTT password</label>
            <input id="mqttPassword" type="password" placeholder="optional">
          </div>
        </div>
        <div class="actions">
          <button id="transferBtn">Run Transfer</button>
        </div>
        <div>
          <label for="log">Activity</label>
          <div class="log" id="log"></div>
        </div>
      </div>
    </section>
  </main>
  <script>
    const logEl = document.getElementById('log');
    const transfersEl = document.getElementById('transfers');
    function log(message) {
      const time = new Date().toLocaleTimeString();
      logEl.textContent = `[${time}] ${message}\n` + logEl.textContent;
    }
    async function api(path, options = {}) {
      const response = await fetch(path, options);
      const data = await response.json().catch(() => ({}));
      if (!response.ok || data.ok === false) throw new Error(data.error || response.statusText);
      return data;
    }
    function fmtBytes(value) {
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let size = Number(value), i = 0;
      while (size >= 1024 && i < units.length - 1) { size /= 1024; i++; }
      return `${size.toFixed(size >= 10 || i === 0 ? 0 : 1)} ${units[i]}`;
    }
    function renderTransfers(items) {
      if (!items.length) {
        transfersEl.innerHTML = '<div class="empty">No transfer received yet.</div>';
        return;
      }
      transfersEl.innerHTML = items.map(item => {
        const pct = item.total_frames ? Math.round(item.received_frames * 100 / item.total_frames) : 0;
        const state = item.complete ? 'Complete' : `${item.missing_frames} missing`;
        return `<article class="transfer">
          <div class="row">
            <div>
              <div class="name">${escapeHtml(item.filename)}</div>
              <div class="meta">${escapeHtml(item.transfer_id)} · ${fmtBytes(item.size)} · ${item.received_frames}/${item.total_frames} frames</div>
            </div>
            <span class="pill">${state}</span>
          </div>
          <div class="bar"><div class="fill" style="width:${pct}%"></div></div>
          <div class="meta">SHA256 ${escapeHtml(item.sha256)}</div>
          <div class="actions">
            <button ${item.complete ? '' : 'disabled'} onclick="joinTransfer('${item.transfer_id}')">Join</button>
            <button class="secondary" onclick="copyId('${item.transfer_id}')">Copy ID</button>
          </div>
        </article>`;
      }).join('');
    }
    function escapeHtml(value) {
      return String(value).replace(/[&<>"']/g, ch => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[ch]));
    }
    async function refresh() {
      try {
        const data = await api('/api/transfers');
        renderTransfers(data.transfers || []);
      } catch (err) {
        log(err.message);
      }
    }
    async function joinTransfer(id) {
      try {
        const data = await api(`/api/join/${encodeURIComponent(id)}`, {method: 'POST'});
        log(`joined ${data.output}`);
        refresh();
      } catch (err) {
        log(err.message);
      }
    }
    async function copyId(id) {
      await navigator.clipboard.writeText(id).catch(() => {});
      log(`transfer id ${id}`);
    }
    function syncModeFields() {
      const mode = document.getElementById('transferMode').value;
      const network = document.getElementById('networkFields');
      const token = document.getElementById('authToken').closest('div');
      const mqttTopic = document.getElementById('mqttTopicField');
      const mqttAuth = document.getElementById('mqttAuthFields');
      network.style.display = mode === 'offline' ? 'none' : 'block';
      token.style.display = (mode === 'http' || mode === 'websocket' || mode === 'udp' || mode === 'quic') ? 'block' : 'none';
      mqttTopic.style.display = mode === 'mqtt' ? 'block' : 'none';
      mqttAuth.style.display = mode === 'mqtt' ? 'grid' : 'none';
      const url = document.getElementById('targetUrl');
      if (mode === 'http') url.placeholder = 'http://host:8080';
      if (mode === 'websocket') url.placeholder = 'ws://host:8090';
      if (mode === 'udp') url.placeholder = 'udp://host:9000';
      if (mode === 'quic') url.placeholder = 'quic://host:9443';
      if (mode === 'mqtt') url.placeholder = 'mqtt://broker:1883';
      const format = document.getElementById('frameFormat');
      format.disabled = mode !== 'offline';
      if (mode !== 'offline') format.value = 'msgpack';
    }
    document.getElementById('refreshBtn').addEventListener('click', refresh);
    document.getElementById('transferMode').addEventListener('change', syncModeFields);
    document.getElementById('transferBtn').addEventListener('click', async () => {
      const payload = {
        input: document.getElementById('transferInput').value,
        parts: document.getElementById('partsOutput').value,
        mode: document.getElementById('transferMode').value,
        url: document.getElementById('targetUrl').value,
        chunk_size: document.getElementById('chunkSize').value,
        format: document.getElementById('frameFormat').value,
        token: document.getElementById('authToken').value,
        topic: document.getElementById('mqttTopic').value,
        username: document.getElementById('mqttUsername').value,
        password: document.getElementById('mqttPassword').value
      };
      try {
        const data = await api('/api/transfer', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(payload)});
        log(data.message || `transfer ${data.mode} finished`);
        refresh();
      } catch (err) {
        log(err.message);
      }
    });
    syncModeFields();
    refresh();
    setInterval(refresh, 4000);
  </script>
</body>
</html>
"""


class OffsplitHTTPServer(http.server.ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_class: type[http.server.BaseHTTPRequestHandler], inbox: Path, completed: Path, token: str | None):
        super().__init__(server_address, handler_class)
        self.inbox = inbox
        self.completed = completed
        self.token = token


class OffsplitHandler(http.server.BaseHTTPRequestHandler):
    server: OffsplitHTTPServer

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} - {fmt % args}")

    def auth_ok(self) -> bool:
        if not self.server.token:
            return True
        header = self.headers.get("Authorization", "")
        query = parse.parse_qs(parse.urlparse(self.path).query)
        return header == f"Bearer {self.server.token}" or query.get("token") == [self.server.token]

    def send_json(self, status: int, value: dict[str, Any]) -> None:
        data = json_bytes(value)
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def send_error_json(self, status: int, message: str) -> None:
        self.send_json(status, {"ok": False, "error": message})

    def read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            raise FrameError("empty request body")
        return self.rfile.read(length)

    def do_GET(self) -> None:
        if not self.auth_ok():
            self.send_error_json(401, "unauthorized")
            return
        parsed = parse.urlparse(self.path)
        try:
            if parsed.path == "/":
                data = WEB_APP.encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            elif parsed.path == "/api/transfers":
                self.send_json(200, {"ok": True, "transfers": list_transfers(self.server.inbox)})
            elif parsed.path.startswith("/api/status/"):
                transfer_id = parse.unquote(parsed.path.rsplit("/", 1)[-1])
                manifest_path = find_manifest(self.server.inbox, transfer_id)
                self.send_json(200, {"ok": True, **scan_transfer(self.server.inbox, manifest_path)})
            else:
                self.send_error_json(404, "not found")
        except FrameError as exc:
            self.send_error_json(400, str(exc))

    def do_POST(self) -> None:
        if not self.auth_ok():
            self.send_error_json(401, "unauthorized")
            return
        parsed = parse.urlparse(self.path)
        try:
            if parsed.path == "/api/upload/manifest":
                self.handle_manifest_upload()
            elif parsed.path == "/api/upload/frame":
                self.handle_frame_upload()
            elif parsed.path.startswith("/api/join/"):
                transfer_id = parse.unquote(parsed.path.rsplit("/", 1)[-1])
                self.handle_join(transfer_id)
            elif parsed.path == "/api/split":
                self.handle_split()
            elif parsed.path == "/api/transfer":
                self.handle_transfer()
            else:
                self.send_error_json(404, "not found")
        except FrameError as exc:
            self.send_error_json(400, str(exc))

    def handle_manifest_upload(self) -> None:
        data = self.read_body()
        temp_path = self.server.inbox / "upload.ofm.tmp"
        replace_file(temp_path, data)
        metadata = read_msgpack_manifest(temp_path)
        target = self.server.inbox / manifest_name(metadata)
        os.replace(temp_path, target)
        self.send_json(200, {"ok": True, "transfer_id": metadata["transfer_id"], "manifest": str(target)})

    def handle_frame_upload(self) -> None:
        data = self.read_body()
        transfer_id = frame_transfer_id(data, self.server.inbox / "upload.ofs")
        manifest_path = find_manifest(self.server.inbox, transfer_id)
        metadata = read_msgpack_manifest(manifest_path)
        sequence = frame_sequence(data, self.server.inbox / "upload.ofs")
        temp_path = self.server.inbox / f"upload.{transfer_id}.{sequence}.ofs.tmp"
        replace_file(temp_path, data)
        _, sequence, _ = read_msgpack_part(temp_path, metadata)
        target = self.server.inbox / frame_name(metadata, sequence)
        os.replace(temp_path, target)
        self.send_json(200, {"ok": True, "transfer_id": transfer_id, "seq": sequence})

    def handle_join(self, transfer_id: str) -> None:
        manifest_path = find_manifest(self.server.inbox, transfer_id)
        metadata = read_msgpack_manifest(manifest_path)
        output = self.server.completed / str(metadata["filename"])
        result = join_frames(manifest_path, output, force=True, resume=True)
        self.send_json(200, {"ok": True, **result})

    def handle_split(self) -> None:
        payload = parse_json_bytes(self.read_body())
        source = str(payload.get("input", "")).strip()
        output = str(payload.get("output", "")).strip()
        if not source or not output:
            raise FrameError("input and output are required")
        split_args = argparse.Namespace(
            input=source,
            output=output,
            chunk_size=parse_size(str(payload.get("chunk_size", "1m"))),
            format=str(payload.get("format", "msgpack")),
            resume=True,
        )
        if split_args.format not in {"msgpack", "text"}:
            raise FrameError("format must be msgpack or text")
        command_split(split_args)
        metadata, _ = load_frames(Path(output))
        self.send_json(200, {"ok": True, "transfer_id": metadata["transfer_id"], "parts": metadata["total_frames"], "output": output})

    def handle_transfer(self) -> None:
        payload = parse_json_bytes(self.read_body())
        source = str(payload.get("input", "")).strip()
        if not source:
            raise FrameError("input is required")
        mode = str(payload.get("mode", "offline")).strip()
        if mode not in {"offline", "http", "websocket", "udp", "quic", "mqtt"}:
            raise FrameError("mode must be offline, http, websocket, udp, quic, or mqtt")
        target_url = str(payload.get("url", "")).strip()
        parts = str(payload.get("parts", "")).strip() or None
        transfer_args = argparse.Namespace(
            input=source,
            mode=mode,
            url=target_url or None,
            parts=parts,
            chunk_size=parse_size(str(payload.get("chunk_size", "1m"))),
            format=str(payload.get("format", "msgpack")).strip() or "msgpack",
            token=str(payload.get("token", "")).strip() or None,
            force=False,
            resume=True,
            retries=5,
            timeout=1.5,
            max_datagram=60000,
            insecure=True,
            topic=str(payload.get("topic", "offsplit")).strip() or "offsplit",
            client_id=None,
            username=str(payload.get("username", "")).strip() or None,
            password=str(payload.get("password", "")).strip() or None,
            retain_manifest=False,
        )
        command_transfer(transfer_args)
        parts_dir = Path(parts).expanduser().resolve() if parts else default_parts_dir(Path(source).expanduser().resolve()).resolve()
        metadata, _ = load_frames(parts_dir)
        message = f"{mode} transfer prepared"
        if mode != "offline":
            message = f"{mode} transfer finished"
        self.send_json(
            200,
            {
                "ok": True,
                "mode": mode,
                "transfer_id": metadata["transfer_id"],
                "parts": metadata["total_frames"],
                "parts_dir": str(parts_dir),
                "message": message,
            },
        )


def command_serve(args: argparse.Namespace) -> int:
    inbox = Path(args.inbox).expanduser().resolve()
    completed = Path(args.completed).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    completed.mkdir(parents=True, exist_ok=True)
    server = OffsplitHTTPServer((args.host, args.port), OffsplitHandler, inbox, completed, args.token)
    url = f"http://{args.host}:{server.server_port}"
    print(f"serving: {url}")
    print(f"inbox: {inbox}")
    print(f"completed: {completed}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        server.server_close()
    return 0


def post_bytes(url: str, data: bytes, token: str | None = None) -> dict[str, Any]:
    headers = {"Content-Type": "application/octet-stream"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = request.Request(url, data=data, headers=headers, method="POST")
    try:
        with request.urlopen(req, timeout=60) as response:
            return parse_json_bytes(response.read())
    except urlerror.HTTPError as exc:
        try:
            detail = parse_json_bytes(exc.read())
            raise FrameError(str(detail.get("error", exc))) from exc
        except FrameError:
            raise
    except urlerror.URLError as exc:
        raise FrameError(f"network error: {exc}") from exc


def get_json(url: str, token: str | None = None) -> dict[str, Any]:
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=60) as response:
            return parse_json_bytes(response.read())
    except urlerror.HTTPError as exc:
        try:
            detail = parse_json_bytes(exc.read())
            raise FrameError(str(detail.get("error", exc))) from exc
        except FrameError:
            raise
    except urlerror.URLError as exc:
        raise FrameError(f"network error: {exc}") from exc


def command_send(args: argparse.Namespace) -> int:
    source = Path(args.input).expanduser().resolve()
    manifest_path, parts_dir = collect_msgpack_manifest(source)
    metadata = read_msgpack_manifest(manifest_path)
    base_url = args.url.rstrip("/")
    post_bytes(f"{base_url}/api/upload/manifest", manifest_path.read_bytes(), args.token)
    status = get_json(f"{base_url}/api/status/{parse.quote(metadata['transfer_id'])}", args.token)
    missing = set(status.get("missing", []))
    total = int(metadata["total_frames"])
    sent = 0
    skipped = 0
    for sequence in range(total):
        path = parts_dir / frame_name(metadata, sequence)
        if sequence not in missing and not args.force:
            skipped += 1
            continue
        if not path.exists():
            raise FrameError(f"missing local frame: {path}")
        post_bytes(f"{base_url}/api/upload/frame", path.read_bytes(), args.token)
        sent += 1
        if sent % 25 == 0:
            print(f"sent: {sent}, skipped: {skipped}")
    print(f"transfer_id: {metadata['transfer_id']}")
    print(f"sent: {sent}")
    print(f"skipped: {skipped}")
    return 0


WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_accept_key(client_key: str) -> str:
    digest = hashlib.sha1((client_key + WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def ws_read_exact(sock: socket.socket, length: int) -> bytes:
    chunks = bytearray()
    while len(chunks) < length:
        chunk = sock.recv(length - len(chunks))
        if not chunk:
            raise FrameError("WebSocket connection closed")
        chunks.extend(chunk)
    return bytes(chunks)


def ws_read_message(sock: socket.socket) -> tuple[int, bytes]:
    first = ws_read_exact(sock, 2)
    opcode = first[0] & 0x0F
    masked = bool(first[1] & 0x80)
    length = first[1] & 0x7F
    if length == 126:
        length = struct.unpack("!H", ws_read_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", ws_read_exact(sock, 8))[0]
    mask = ws_read_exact(sock, 4) if masked else b""
    payload = bytearray(ws_read_exact(sock, length))
    if masked:
        for index in range(length):
            payload[index] ^= mask[index % 4]
    return opcode, bytes(payload)


def ws_send_message(sock: socket.socket, opcode: int, payload: bytes, masked: bool = False) -> None:
    first = 0x80 | (opcode & 0x0F)
    length = len(payload)
    header = bytearray([first])
    mask_bit = 0x80 if masked else 0
    if length < 126:
        header.append(mask_bit | length)
    elif length <= 0xFFFF:
        header.append(mask_bit | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(mask_bit | 127)
        header.extend(struct.pack("!Q", length))
    if masked:
        mask = os.urandom(4)
        header.extend(mask)
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(bytes(header) + payload)


def ws_send_json(sock: socket.socket, value: dict[str, Any], masked: bool = False) -> None:
    ws_send_message(sock, 1, json_bytes(value), masked)


def ws_read_json(sock: socket.socket) -> dict[str, Any]:
    opcode, payload = ws_read_message(sock)
    if opcode == 8:
        raise FrameError("WebSocket closed")
    if opcode != 1:
        raise FrameError("expected WebSocket text message")
    return parse_json_bytes(payload)


def ws_store_manifest(inbox: Path, data: bytes) -> dict[str, Any]:
    temp_path = inbox / "ws-upload.ofm.tmp"
    replace_file(temp_path, data)
    metadata = read_msgpack_manifest(temp_path)
    target = inbox / manifest_name(metadata)
    os.replace(temp_path, target)
    return metadata


def ws_store_frame(inbox: Path, data: bytes) -> tuple[str, int]:
    transfer_id = frame_transfer_id(data, inbox / "ws-upload.ofs")
    manifest_path = find_manifest(inbox, transfer_id)
    metadata = read_msgpack_manifest(manifest_path)
    sequence = frame_sequence(data, inbox / "ws-upload.ofs")
    temp_path = inbox / f"ws-upload.{transfer_id}.{sequence}.ofs.tmp"
    replace_file(temp_path, data)
    _, sequence, _ = read_msgpack_part(temp_path, metadata)
    target = inbox / frame_name(metadata, sequence)
    os.replace(temp_path, target)
    return transfer_id, sequence


class OffsplitWebSocketServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, server_address: tuple[str, int], handler_class: type[socketserver.BaseRequestHandler], inbox: Path, completed: Path, token: str | None, auto_join: bool):
        super().__init__(server_address, handler_class)
        self.inbox = inbox
        self.completed = completed
        self.token = token
        self.auto_join = auto_join


class OffsplitWebSocketHandler(socketserver.BaseRequestHandler):
    server: OffsplitWebSocketServer

    def handle(self) -> None:
        sock = self.request
        try:
            headers = self.read_http_headers(sock)
            token = self.extract_token(headers)
            if self.server.token and token != self.server.token:
                sock.sendall(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
                return
            key = headers.get("sec-websocket-key")
            if not key:
                sock.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                return
            response = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {ws_accept_key(key)}\r\n\r\n"
            )
            sock.sendall(response.encode("ascii"))
            self.run_session(sock)
        except FrameError as exc:
            try:
                ws_send_json(sock, {"ok": False, "error": str(exc)})
            except Exception:
                pass

    def read_http_headers(self, sock: socket.socket) -> dict[str, str]:
        data = bytearray()
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                raise FrameError("empty WebSocket handshake")
            data.extend(chunk)
            if len(data) > 32768:
                raise FrameError("WebSocket handshake too large")
        text = data.decode("iso-8859-1")
        lines = text.split("\r\n")
        headers: dict[str, str] = {}
        for line in lines[1:]:
            if ":" in line:
                key, value = line.split(":", 1)
                headers[key.strip().lower()] = value.strip()
        if "upgrade" not in headers.get("connection", "").lower() and headers.get("upgrade", "").lower() != "websocket":
            raise FrameError("not a WebSocket upgrade")
        return headers

    def extract_token(self, headers: dict[str, str]) -> str | None:
        protocol = headers.get("sec-websocket-protocol")
        if protocol:
            for value in (item.strip() for item in protocol.split(",")):
                if value.startswith("token."):
                    return value.removeprefix("token.")
        auth = headers.get("authorization", "")
        if auth.startswith("Bearer "):
            return auth.removeprefix("Bearer ")
        return None

    def run_session(self, sock: socket.socket) -> None:
        metadata: dict[str, Any] | None = None
        while True:
            opcode, payload = ws_read_message(sock)
            if opcode == 8:
                return
            if opcode == 1:
                control = parse_json_bytes(payload)
                if control.get("type") == "complete":
                    if metadata is None:
                        raise FrameError("manifest must be sent before complete")
                    transfer_id = str(metadata["transfer_id"])
                    manifest_path = find_manifest(self.server.inbox, transfer_id)
                    status = scan_transfer(self.server.inbox, manifest_path)
                    result: dict[str, Any] | None = None
                    if self.server.auto_join and status["complete"]:
                        output = self.server.completed / str(metadata["filename"])
                        result = join_frames(manifest_path, output, force=True, resume=True)
                    ws_send_json(sock, {"ok": True, "type": "complete", "status": status, "join": result})
                continue
            if opcode != 2:
                raise FrameError(f"unsupported WebSocket opcode: {opcode}")
            if payload.startswith(MSGPACK_META_MAGIC):
                metadata = ws_store_manifest(self.server.inbox, payload)
                status = scan_transfer(self.server.inbox, self.server.inbox / manifest_name(metadata))
                ws_send_json(sock, {"ok": True, "type": "status", **status})
            elif payload.startswith(MSGPACK_FRAME_MAGIC):
                transfer_id, sequence = ws_store_frame(self.server.inbox, payload)
                ws_send_json(sock, {"ok": True, "type": "ack", "transfer_id": transfer_id, "seq": sequence})
            else:
                raise FrameError("binary message must be .ofm or .ofs data")


def ws_client_connect(url: str, token: str | None = None) -> socket.socket:
    parsed = parse.urlparse(url)
    if parsed.scheme != "ws":
        raise FrameError("ws-send currently supports ws:// URLs")
    host = parsed.hostname
    if not host:
        raise FrameError("WebSocket URL needs a host")
    port = parsed.port or 80
    path = parsed.path or "/"
    if parsed.query:
        path += "?" + parsed.query
    sock = socket.create_connection((host, port), timeout=60)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    headers = [
        f"GET {path} HTTP/1.1",
        f"Host: {host}:{port}",
        "Upgrade: websocket",
        "Connection: Upgrade",
        f"Sec-WebSocket-Key: {key}",
        "Sec-WebSocket-Version: 13",
    ]
    if token:
        headers.append(f"Authorization: Bearer {token}")
    sock.sendall(("\r\n".join(headers) + "\r\n\r\n").encode("ascii"))
    response = bytearray()
    while b"\r\n\r\n" not in response:
        chunk = sock.recv(4096)
        if not chunk:
            raise FrameError("WebSocket server closed during handshake")
        response.extend(chunk)
        if len(response) > 32768:
            raise FrameError("WebSocket handshake response too large")
    header_text = response.decode("iso-8859-1")
    if " 101 " not in header_text.split("\r\n", 1)[0]:
        raise FrameError(f"WebSocket handshake failed: {header_text.splitlines()[0]}")
    if ws_accept_key(key) not in header_text:
        raise FrameError("WebSocket accept key mismatch")
    return sock


def command_ws_serve(args: argparse.Namespace) -> int:
    inbox = Path(args.inbox).expanduser().resolve()
    completed = Path(args.completed).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    completed.mkdir(parents=True, exist_ok=True)
    server = OffsplitWebSocketServer((args.host, args.port), OffsplitWebSocketHandler, inbox, completed, args.token, args.join)
    print(f"websocket: ws://{args.host}:{server.server_address[1]}")
    print(f"inbox: {inbox}")
    print(f"completed: {completed}")
    print(f"auto_join: {args.join}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        server.server_close()
    return 0


def command_ws_send(args: argparse.Namespace) -> int:
    source = Path(args.input).expanduser().resolve()
    manifest_path, parts_dir = collect_msgpack_manifest(source)
    metadata = read_msgpack_manifest(manifest_path)
    sock = ws_client_connect(args.url, args.token)
    try:
        ws_send_message(sock, 2, manifest_path.read_bytes(), masked=True)
        status = ws_read_json(sock)
        if not status.get("ok"):
            raise FrameError(str(status.get("error", "receiver rejected manifest")))
        missing = set(status.get("missing", []))
        total = int(metadata["total_frames"])
        sent = 0
        skipped = 0
        for sequence in range(total):
            path = parts_dir / frame_name(metadata, sequence)
            if sequence not in missing and not args.force:
                skipped += 1
                continue
            if not path.exists():
                raise FrameError(f"missing local frame: {path}")
            ws_send_message(sock, 2, path.read_bytes(), masked=True)
            ack = ws_read_json(sock)
            if not ack.get("ok") or int(ack.get("seq", -1)) != sequence:
                raise FrameError(f"bad ACK for frame {sequence}: {ack}")
            sent += 1
            if sent % 25 == 0:
                print(f"sent: {sent}, skipped: {skipped}")
        ws_send_json(sock, {"type": "complete"}, masked=True)
        complete = ws_read_json(sock)
        if not complete.get("ok"):
            raise FrameError(str(complete.get("error", "receiver rejected complete")))
        status = complete.get("status", {})
        print(f"transfer_id: {metadata['transfer_id']}")
        print(f"sent: {sent}")
        print(f"skipped: {skipped}")
        print(f"missing: {status.get('missing_frames')}")
        join_result = complete.get("join")
        if join_result:
            print(f"joined: {join_result['output']}")
        ws_send_message(sock, 8, b"", masked=True)
    finally:
        sock.close()
    return 0


MQTT_PROTOCOL_NAME = b"MQTT"


def mqtt_encode_remaining_length(length: int) -> bytes:
    encoded = bytearray()
    while True:
        byte = length % 128
        length //= 128
        if length > 0:
            byte |= 128
        encoded.append(byte)
        if length == 0:
            return bytes(encoded)


def mqtt_read_remaining_length(sock: socket.socket) -> int:
    multiplier = 1
    value = 0
    while True:
        byte = ws_read_exact(sock, 1)[0]
        value += (byte & 127) * multiplier
        if not byte & 128:
            return value
        multiplier *= 128
        if multiplier > 128**4:
            raise FrameError("malformed MQTT remaining length")


def mqtt_utf8(value: str) -> bytes:
    data = value.encode("utf-8")
    if len(data) > 0xFFFF:
        raise FrameError("MQTT string too long")
    return struct.pack("!H", len(data)) + data


def mqtt_packet(packet_type: int, flags: int, payload: bytes) -> bytes:
    return bytes([(packet_type << 4) | flags]) + mqtt_encode_remaining_length(len(payload)) + payload


def mqtt_read_packet(sock: socket.socket) -> tuple[int, int, bytes]:
    first = ws_read_exact(sock, 1)[0]
    remaining = mqtt_read_remaining_length(sock)
    return first >> 4, first & 0x0F, ws_read_exact(sock, remaining)


def mqtt_parse_url(url: str) -> tuple[str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme != "mqtt":
        raise FrameError("MQTT URL must look like mqtt://host:1883")
    if not parsed.hostname:
        raise FrameError("MQTT URL needs a host")
    return parsed.hostname, parsed.port or 1883


class MinimalMQTTClient:
    def __init__(self, url: str, client_id: str, username: str | None = None, password: str | None = None):
        self.host, self.port = mqtt_parse_url(url)
        self.client_id = client_id
        self.username = username
        self.password = password
        self.sock: socket.socket | None = None
        self.packet_id = 1

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port), timeout=60)
        flags = 0x02
        payload = mqtt_utf8(self.client_id)
        if self.username is not None:
            flags |= 0x80
        if self.password is not None:
            flags |= 0x40
        variable = mqtt_utf8(MQTT_PROTOCOL_NAME.decode("ascii")) + bytes([4, flags]) + struct.pack("!H", 60)
        if self.username is not None:
            payload += mqtt_utf8(self.username)
        if self.password is not None:
            payload += mqtt_utf8(self.password)
        self.sock.sendall(mqtt_packet(1, 0, variable + payload))
        packet_type, _, data = mqtt_read_packet(self.sock)
        if packet_type != 2 or len(data) < 2 or data[1] != 0:
            code = data[1] if len(data) >= 2 else "?"
            raise FrameError(f"MQTT CONNACK failed: {code}")

    def close(self) -> None:
        if self.sock:
            try:
                self.sock.sendall(mqtt_packet(14, 0, b""))
            except OSError:
                pass
            self.sock.close()
            self.sock = None

    def next_packet_id(self) -> int:
        packet_id = self.packet_id
        self.packet_id += 1
        if self.packet_id > 0xFFFF:
            self.packet_id = 1
        return packet_id

    def publish(self, topic: str, payload: bytes, qos: int = 1, retain: bool = False) -> None:
        if self.sock is None:
            raise FrameError("MQTT client is not connected")
        packet_id = self.next_packet_id() if qos else 0
        variable = mqtt_utf8(topic)
        if qos:
            variable += struct.pack("!H", packet_id)
        flags = (qos << 1) | (1 if retain else 0)
        self.sock.sendall(mqtt_packet(3, flags, variable + payload))
        if qos == 1:
            while True:
                packet_type, _, data = mqtt_read_packet(self.sock)
                if packet_type == 4 and len(data) >= 2 and struct.unpack("!H", data[:2])[0] == packet_id:
                    return
                if packet_type == 13:
                    continue
                raise FrameError(f"unexpected MQTT packet while waiting PUBACK: {packet_type}")

    def subscribe(self, topic: str, qos: int = 1) -> None:
        if self.sock is None:
            raise FrameError("MQTT client is not connected")
        packet_id = self.next_packet_id()
        payload = struct.pack("!H", packet_id) + mqtt_utf8(topic) + bytes([qos])
        self.sock.sendall(mqtt_packet(8, 2, payload))
        while True:
            packet_type, _, data = mqtt_read_packet(self.sock)
            if packet_type == 9 and len(data) >= 3 and struct.unpack("!H", data[:2])[0] == packet_id:
                if data[2] == 0x80:
                    raise FrameError("MQTT SUBSCRIBE rejected")
                return
            if packet_type == 13:
                continue
            raise FrameError(f"unexpected MQTT packet while waiting SUBACK: {packet_type}")

    def read_publish(self) -> tuple[str, bytes]:
        if self.sock is None:
            raise FrameError("MQTT client is not connected")
        while True:
            packet_type, flags, data = mqtt_read_packet(self.sock)
            if packet_type == 13:
                continue
            if packet_type != 3:
                continue
            qos = (flags >> 1) & 0x03
            if len(data) < 2:
                raise FrameError("bad MQTT PUBLISH")
            topic_len = struct.unpack("!H", data[:2])[0]
            topic_end = 2 + topic_len
            topic = data[2:topic_end].decode("utf-8")
            if qos:
                packet_id = struct.unpack("!H", data[topic_end : topic_end + 2])[0]
                payload = data[topic_end + 2 :]
                self.sock.sendall(mqtt_packet(4, 0, struct.pack("!H", packet_id)))
            else:
                payload = data[topic_end:]
            return topic, payload


def mqtt_base_topic(base: str, transfer_id: str | None = None) -> str:
    clean = base.strip("/")
    return f"{clean}/{transfer_id}" if transfer_id else clean


def mqtt_manifest_topic(base: str, transfer_id: str) -> str:
    return f"{mqtt_base_topic(base, transfer_id)}/manifest"


def mqtt_frame_topic(base: str, transfer_id: str, sequence: int) -> str:
    return f"{mqtt_base_topic(base, transfer_id)}/frame/{sequence}"


def command_mqtt_send(args: argparse.Namespace) -> int:
    source = Path(args.input).expanduser().resolve()
    manifest_path, parts_dir = collect_msgpack_manifest(source)
    metadata = read_msgpack_manifest(manifest_path)
    transfer_id = str(metadata["transfer_id"])
    client = MinimalMQTTClient(args.url, args.client_id or f"offsplit-send-{os.getpid()}", args.username, args.password)
    client.connect()
    try:
        client.publish(mqtt_manifest_topic(args.topic, transfer_id), manifest_path.read_bytes(), qos=1, retain=args.retain_manifest)
        total = int(metadata["total_frames"])
        sent = 0
        for sequence in range(total):
            path = parts_dir / frame_name(metadata, sequence)
            if not path.exists():
                raise FrameError(f"missing local frame: {path}")
            client.publish(mqtt_frame_topic(args.topic, transfer_id, sequence), path.read_bytes(), qos=1, retain=False)
            sent += 1
            if sent % 25 == 0:
                print(f"sent: {sent}")
        print(f"transfer_id: {transfer_id}")
        print(f"sent: {sent}")
    finally:
        client.close()
    return 0


def command_mqtt_recv(args: argparse.Namespace) -> int:
    inbox = Path(args.inbox).expanduser().resolve()
    completed = Path(args.completed).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    completed.mkdir(parents=True, exist_ok=True)
    client = MinimalMQTTClient(args.url, args.client_id or f"offsplit-recv-{os.getpid()}", args.username, args.password)
    client.connect()
    subscribed_topic = f"{args.topic.strip('/')}/#"
    client.subscribe(subscribed_topic, qos=1)
    print(f"mqtt: {args.url}")
    print(f"subscribed: {subscribed_topic}")
    print(f"inbox: {inbox}")
    print(f"completed: {completed}")
    seen_complete: set[str] = set()
    try:
        while True:
            topic, payload = client.read_publish()
            if payload.startswith(MSGPACK_META_MAGIC):
                metadata = ws_store_manifest(inbox, payload)
                status = scan_transfer(inbox, inbox / manifest_name(metadata))
                print(f"manifest: {metadata['transfer_id']} {status['received_frames']}/{status['total_frames']}")
            elif payload.startswith(MSGPACK_FRAME_MAGIC):
                transfer_id, sequence = ws_store_frame(inbox, payload)
                manifest_path = find_manifest(inbox, transfer_id)
                status = scan_transfer(inbox, manifest_path)
                print(f"frame: {transfer_id} seq={sequence} {status['received_frames']}/{status['total_frames']}")
                if args.join and status["complete"] and transfer_id not in seen_complete:
                    metadata = read_msgpack_manifest(manifest_path)
                    result = join_frames(manifest_path, completed / str(metadata["filename"]), force=True, resume=True)
                    seen_complete.add(transfer_id)
                    print(f"joined: {result['output']}")
                    if args.once:
                        return 0
            else:
                continue
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        client.close()
    return 0


def udp_parse_url(url: str) -> tuple[str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme != "udp":
        raise FrameError("UDP URL must look like udp://host:9000")
    if not parsed.hostname:
        raise FrameError("UDP URL needs a host")
    return parsed.hostname, parsed.port or 9000


def udp_pack(value: dict[str, Any]) -> bytes:
    payload = {"protocol": "OFFSPLIT-UDP/1", **value}
    return packb(payload)


def udp_unpack(data: bytes) -> dict[str, Any]:
    value = unpackb(Path("<udp>"), data)
    if value.get("protocol") != "OFFSPLIT-UDP/1":
        raise FrameError("bad UDP protocol")
    return value


def udp_send_wait(sock: socket.socket, address: tuple[str, int], packet: dict[str, Any], expected_type: str, retries: int, timeout: float, max_datagram: int) -> dict[str, Any]:
    data = udp_pack(packet)
    if len(data) > max_datagram:
        raise FrameError(f"UDP datagram too large ({len(data)} bytes); use a smaller chunk size")
    sock.settimeout(timeout)
    last_error: Exception | None = None
    for _ in range(retries):
        sock.sendto(data, address)
        try:
            response, _ = sock.recvfrom(65535)
            decoded = udp_unpack(response)
            if decoded.get("type") == expected_type:
                if not decoded.get("ok", True):
                    raise FrameError(str(decoded.get("error", "UDP receiver error")))
                return decoded
        except (socket.timeout, FrameError) as exc:
            last_error = exc
            continue
    raise FrameError(f"UDP timeout waiting for {expected_type}: {last_error}")


def command_udp_send(args: argparse.Namespace) -> int:
    source = Path(args.input).expanduser().resolve()
    manifest_path, parts_dir = collect_msgpack_manifest(source)
    metadata = read_msgpack_manifest(manifest_path)
    transfer_id = str(metadata["transfer_id"])
    address = udp_parse_url(args.url)
    retries = int(args.retries)
    timeout = float(args.timeout)
    max_datagram = int(args.max_datagram)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        status = udp_send_wait(
            sock,
            address,
            {"type": "manifest", "token": args.token, "data": manifest_path.read_bytes()},
            "status",
            retries,
            timeout,
            max_datagram,
        )
        missing = set(status.get("missing", []))
        total = int(metadata["total_frames"])
        sent = 0
        skipped = 0
        for sequence in range(total):
            path = parts_dir / frame_name(metadata, sequence)
            if sequence not in missing and not args.force:
                skipped += 1
                continue
            if not path.exists():
                raise FrameError(f"missing local frame: {path}")
            ack = udp_send_wait(
                sock,
                address,
                {"type": "frame", "token": args.token, "transfer_id": transfer_id, "seq": sequence, "data": path.read_bytes()},
                "ack",
                retries,
                timeout,
                max_datagram,
            )
            if ack.get("seq") != sequence:
                raise FrameError(f"bad UDP ACK for frame {sequence}")
            sent += 1
            if sent % 25 == 0:
                print(f"sent: {sent}, skipped: {skipped}")
        complete = udp_send_wait(
            sock,
            address,
            {"type": "complete", "token": args.token, "transfer_id": transfer_id},
            "complete",
            retries,
            timeout,
            max_datagram,
        )
        status = complete.get("status", {})
        print(f"transfer_id: {transfer_id}")
        print(f"sent: {sent}")
        print(f"skipped: {skipped}")
        print(f"missing: {status.get('missing_frames')}")
        join_result = complete.get("join")
        if join_result:
            print(f"joined: {join_result['output']}")
    finally:
        sock.close()
    return 0


def command_udp_serve(args: argparse.Namespace) -> int:
    inbox = Path(args.inbox).expanduser().resolve()
    completed = Path(args.completed).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    completed.mkdir(parents=True, exist_ok=True)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((args.host, args.port))
    print(f"udp: udp://{args.host}:{sock.getsockname()[1]}")
    print(f"inbox: {inbox}")
    print(f"completed: {completed}")
    print(f"auto_join: {args.join}")
    try:
        while True:
            data, address = sock.recvfrom(65535)
            try:
                packet = udp_unpack(data)
                if args.token and packet.get("token") != args.token:
                    sock.sendto(udp_pack({"type": "error", "ok": False, "error": "unauthorized"}), address)
                    continue
                packet_type = packet.get("type")
                if packet_type == "manifest":
                    raw = packet.get("data")
                    if not isinstance(raw, bytes):
                        raise FrameError("manifest packet data must be bytes")
                    metadata = ws_store_manifest(inbox, raw)
                    status = scan_transfer(inbox, inbox / manifest_name(metadata))
                    sock.sendto(udp_pack({"type": "status", "ok": True, **status}), address)
                elif packet_type == "frame":
                    raw = packet.get("data")
                    if not isinstance(raw, bytes):
                        raise FrameError("frame packet data must be bytes")
                    transfer_id, sequence = ws_store_frame(inbox, raw)
                    sock.sendto(udp_pack({"type": "ack", "ok": True, "transfer_id": transfer_id, "seq": sequence}), address)
                elif packet_type == "complete":
                    transfer_id = str(packet.get("transfer_id", ""))
                    manifest_path = find_manifest(inbox, transfer_id)
                    status = scan_transfer(inbox, manifest_path)
                    result: dict[str, Any] | None = None
                    if args.join and status["complete"]:
                        metadata = read_msgpack_manifest(manifest_path)
                        result = join_frames(manifest_path, completed / str(metadata["filename"]), force=True, resume=True)
                    sock.sendto(udp_pack({"type": "complete", "ok": True, "status": status, "join": result}), address)
                else:
                    raise FrameError(f"unknown UDP packet type: {packet_type}")
            except FrameError as exc:
                sock.sendto(udp_pack({"type": "error", "ok": False, "error": str(exc)}), address)
    except KeyboardInterrupt:
        print("\nstopped")
    finally:
        sock.close()
    return 0


def require_aioquic() -> tuple[Any, Any, Any]:
    try:
        from aioquic.asyncio import connect as quic_connect
        from aioquic.asyncio import serve as quic_serve
        from aioquic.quic.configuration import QuicConfiguration
    except ImportError as exc:
        raise FrameError("QUIC mode needs the Python 'aioquic' module") from exc
    return quic_connect, quic_serve, QuicConfiguration


async def quic_write_packet(writer: asyncio.StreamWriter, packet: dict[str, Any]) -> None:
    data = packb({"protocol": "OFFSPLIT-QUIC/1", **packet})
    writer.write(struct.pack("!I", len(data)) + data)
    await writer.drain()


async def quic_read_packet(reader: asyncio.StreamReader) -> dict[str, Any]:
    header = await reader.readexactly(4)
    length = struct.unpack("!I", header)[0]
    if length > 256 * 1024 * 1024:
        raise FrameError("QUIC packet too large")
    packet = unpackb(Path("<quic>"), await reader.readexactly(length))
    if packet.get("protocol") != "OFFSPLIT-QUIC/1":
        raise FrameError("bad QUIC protocol")
    return packet


def quic_parse_url(url: str) -> tuple[str, int]:
    parsed = parse.urlparse(url)
    if parsed.scheme != "quic":
        raise FrameError("QUIC URL must look like quic://host:9443")
    if not parsed.hostname:
        raise FrameError("QUIC URL needs a host")
    return parsed.hostname, parsed.port or 9443


def make_self_signed_cert() -> tuple[Path, Path]:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from cryptography.x509.oid import NameOID
    from datetime import timedelta

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "offsplit.local")])
    cert = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(timezone.utc))
        .not_valid_after(datetime.now(timezone.utc) + timedelta(days=7))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName("localhost")]), critical=False)
        .sign(key, hashes.SHA256())
    )
    cert_path = Path(tempfile.gettempdir()) / f"offsplit-quic-{os.getpid()}.crt"
    key_path = Path(tempfile.gettempdir()) / f"offsplit-quic-{os.getpid()}.key"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.TraditionalOpenSSL,
            serialization.NoEncryption(),
        )
    )
    return cert_path, key_path


async def quic_server_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, inbox: Path, completed: Path, token: str | None, auto_join: bool) -> None:
    metadata: dict[str, Any] | None = None
    try:
        while True:
            packet = await quic_read_packet(reader)
            if token and packet.get("token") != token:
                await quic_write_packet(writer, {"type": "error", "ok": False, "error": "unauthorized"})
                continue
            packet_type = packet.get("type")
            if packet_type == "manifest":
                raw = packet.get("data")
                if not isinstance(raw, bytes):
                    raise FrameError("manifest packet data must be bytes")
                metadata = ws_store_manifest(inbox, raw)
                status = scan_transfer(inbox, inbox / manifest_name(metadata))
                await quic_write_packet(writer, {"type": "status", "ok": True, **status})
            elif packet_type == "frame":
                raw = packet.get("data")
                if not isinstance(raw, bytes):
                    raise FrameError("frame packet data must be bytes")
                transfer_id, sequence = ws_store_frame(inbox, raw)
                await quic_write_packet(writer, {"type": "ack", "ok": True, "transfer_id": transfer_id, "seq": sequence})
            elif packet_type == "complete":
                transfer_id = str(packet.get("transfer_id", ""))
                manifest_path = find_manifest(inbox, transfer_id)
                status = scan_transfer(inbox, manifest_path)
                result: dict[str, Any] | None = None
                if auto_join and status["complete"]:
                    complete_metadata = read_msgpack_manifest(manifest_path)
                    result = join_frames(manifest_path, completed / str(complete_metadata["filename"]), force=True, resume=True)
                await quic_write_packet(writer, {"type": "complete", "ok": True, "status": status, "join": result})
            else:
                raise FrameError(f"unknown QUIC packet type: {packet_type}")
    except (asyncio.IncompleteReadError, ConnectionError):
        return
    except FrameError as exc:
        await quic_write_packet(writer, {"type": "error", "ok": False, "error": str(exc)})


async def quic_serve_main(args: argparse.Namespace) -> None:
    _, quic_serve, QuicConfiguration = require_aioquic()
    inbox = Path(args.inbox).expanduser().resolve()
    completed = Path(args.completed).expanduser().resolve()
    inbox.mkdir(parents=True, exist_ok=True)
    completed.mkdir(parents=True, exist_ok=True)
    cert_path = Path(args.cert).expanduser().resolve() if args.cert else None
    key_path = Path(args.key).expanduser().resolve() if args.key else None
    if not cert_path or not key_path:
        cert_path, key_path = make_self_signed_cert()
    configuration = QuicConfiguration(is_client=False, alpn_protocols=["offsplit-quic"])
    configuration.load_cert_chain(cert_path, key_path)

    def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        asyncio.create_task(quic_server_session(reader, writer, inbox, completed, args.token, args.join))

    server = await quic_serve(args.host, args.port, configuration=configuration, stream_handler=handler)
    print(f"quic: quic://{args.host}:{args.port}")
    print(f"inbox: {inbox}")
    print(f"completed: {completed}")
    print(f"auto_join: {args.join}")
    try:
        await asyncio.Future()
    finally:
        server.close()


def command_quic_serve(args: argparse.Namespace) -> int:
    try:
        asyncio.run(quic_serve_main(args))
    except KeyboardInterrupt:
        print("\nstopped")
    return 0


async def quic_send_main(args: argparse.Namespace) -> dict[str, Any]:
    quic_connect, _, QuicConfiguration = require_aioquic()
    source = Path(args.input).expanduser().resolve()
    manifest_path, parts_dir = collect_msgpack_manifest(source)
    metadata = read_msgpack_manifest(manifest_path)
    transfer_id = str(metadata["transfer_id"])
    host, port = quic_parse_url(args.url)
    configuration = QuicConfiguration(is_client=True, alpn_protocols=["offsplit-quic"])
    if args.insecure:
        configuration.verify_mode = ssl.CERT_NONE
    async with quic_connect(host, port, configuration=configuration) as client:
        reader, writer = await client.create_stream()
        await quic_write_packet(writer, {"type": "manifest", "token": args.token, "data": manifest_path.read_bytes()})
        status = await quic_read_packet(reader)
        if not status.get("ok"):
            raise FrameError(str(status.get("error", "receiver rejected manifest")))
        missing = set(status.get("missing", []))
        sent = 0
        skipped = 0
        total = int(metadata["total_frames"])
        for sequence in range(total):
            path = parts_dir / frame_name(metadata, sequence)
            if sequence not in missing and not args.force:
                skipped += 1
                continue
            if not path.exists():
                raise FrameError(f"missing local frame: {path}")
            await quic_write_packet(writer, {"type": "frame", "token": args.token, "transfer_id": transfer_id, "seq": sequence, "data": path.read_bytes()})
            ack = await quic_read_packet(reader)
            if not ack.get("ok") or int(ack.get("seq", -1)) != sequence:
                raise FrameError(f"bad QUIC ACK for frame {sequence}: {ack}")
            sent += 1
            if sent % 25 == 0:
                print(f"sent: {sent}, skipped: {skipped}")
        await quic_write_packet(writer, {"type": "complete", "token": args.token, "transfer_id": transfer_id})
        complete = await quic_read_packet(reader)
        if not complete.get("ok"):
            raise FrameError(str(complete.get("error", "receiver rejected complete")))
        writer.close()
        return {"transfer_id": transfer_id, "sent": sent, "skipped": skipped, "complete": complete}


def command_quic_send(args: argparse.Namespace) -> int:
    result = asyncio.run(quic_send_main(args))
    status = result["complete"].get("status", {})
    print(f"transfer_id: {result['transfer_id']}")
    print(f"sent: {result['sent']}")
    print(f"skipped: {result['skipped']}")
    print(f"missing: {status.get('missing_frames')}")
    join_result = result["complete"].get("join")
    if join_result:
        print(f"joined: {join_result['output']}")
    return 0


def default_parts_dir(source: Path) -> Path:
    return source.with_suffix(source.suffix + ".parts")


def command_transfer(args: argparse.Namespace) -> int:
    source = Path(args.input).expanduser().resolve()
    parts_dir = Path(args.parts).expanduser().resolve() if args.parts else default_parts_dir(source).resolve()
    frame_format = args.format
    if frame_format not in {"msgpack", "text"}:
        raise FrameError("--format must be msgpack or text")
    if args.mode != "offline" and frame_format != "msgpack":
        raise FrameError("network transfer modes require --format msgpack")

    split_args = argparse.Namespace(
        input=str(source),
        output=str(parts_dir),
        chunk_size=args.chunk_size,
        format=frame_format,
        resume=args.resume,
    )
    command_split(split_args)

    if args.mode == "offline":
        print(f"mode: offline")
        print(f"copy parts directory manually: {parts_dir}")
        return 0

    if not args.url:
        raise FrameError(f"--url is required for mode {args.mode}")

    if args.mode == "http":
        send_args = argparse.Namespace(input=str(parts_dir), url=args.url, token=args.token, force=args.force)
        return command_send(send_args)

    if args.mode == "websocket":
        send_args = argparse.Namespace(input=str(parts_dir), url=args.url, token=args.token, force=args.force)
        return command_ws_send(send_args)

    if args.mode == "udp":
        send_args = argparse.Namespace(
            input=str(parts_dir),
            url=args.url,
            token=args.token,
            force=args.force,
            retries=args.retries,
            timeout=args.timeout,
            max_datagram=args.max_datagram,
        )
        return command_udp_send(send_args)

    if args.mode == "quic":
        send_args = argparse.Namespace(input=str(parts_dir), url=args.url, token=args.token, force=args.force, insecure=args.insecure)
        return command_quic_send(send_args)

    if args.mode == "mqtt":
        send_args = argparse.Namespace(
            input=str(parts_dir),
            url=args.url,
            topic=args.topic,
            client_id=args.client_id,
            username=args.username,
            password=args.password,
            retain_manifest=args.retain_manifest,
        )
        return command_mqtt_send(send_args)

    raise FrameError(f"unknown transfer mode: {args.mode}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="offsplit",
        description="Split binary files into framed offline-transfer parts and join them again.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    transfer = subparsers.add_parser("transfer", help="split a file and optionally send it with a selected transport")
    transfer.add_argument("input", help="file to split and transfer")
    transfer.add_argument("--mode", choices=("offline", "http", "websocket", "udp", "quic", "mqtt"), default="offline", help="transport mode")
    transfer.add_argument("--url", help="target URL for http/websocket/udp/quic/mqtt modes")
    transfer.add_argument("--parts", help="parts directory to create/reuse")
    transfer.add_argument("-s", "--chunk-size", type=parse_size, default=DEFAULT_CHUNK_SIZE, help="bytes per frame, e.g. 64k or 4m")
    transfer.add_argument("--format", choices=("msgpack", "text"), default="msgpack", help="frame format for local parts")
    transfer.add_argument("--token", help="HTTP/WebSocket bearer token")
    transfer.add_argument("-f", "--force", action="store_true", help="send all frames instead of only missing frames when supported")
    transfer.add_argument("--no-resume", dest="resume", action="store_false", help="rewrite local parts instead of resuming split")
    transfer.add_argument("--retries", type=int, default=5, help="UDP retry count per packet")
    transfer.add_argument("--timeout", type=float, default=1.5, help="UDP ACK timeout in seconds")
    transfer.add_argument("--max-datagram", type=int, default=60000, help="maximum UDP datagram size")
    transfer.add_argument("--secure-quic", dest="insecure", action="store_false", help="verify QUIC TLS certificates")
    transfer.add_argument("--topic", default="offsplit", help="base MQTT topic")
    transfer.add_argument("--client-id", help="MQTT client id")
    transfer.add_argument("--username", help="MQTT username")
    transfer.add_argument("--password", help="MQTT password")
    transfer.add_argument("--retain-manifest", action="store_true", help="publish MQTT manifest as retained message")
    transfer.set_defaults(resume=True)
    transfer.set_defaults(insecure=True)
    transfer.set_defaults(func=command_transfer)

    split = subparsers.add_parser("split", help="split a file into .ofs frame files")
    split.add_argument("input", help="file to split")
    split.add_argument("-o", "--output", help="output directory for .ofs parts")
    split.add_argument("-s", "--chunk-size", type=parse_size, default=DEFAULT_CHUNK_SIZE, help="bytes per frame, e.g. 512k or 10m")
    split.add_argument("--format", choices=("msgpack", "text"), default="msgpack", help="frame format to write")
    split.add_argument("--no-resume", dest="resume", action="store_false", help="rewrite existing part files instead of skipping valid ones")
    split.set_defaults(resume=True)
    split.set_defaults(func=command_split)

    join = subparsers.add_parser("join", help="join .ofs frame files into the original file")
    join.add_argument("input", help="directory containing .ofs parts, or a single .ofs file")
    join.add_argument("-o", "--output", help="output file path")
    join.add_argument("-f", "--force", action="store_true", help="overwrite output if it already exists")
    join.add_argument("--no-resume", dest="resume", action="store_false", help="ignore an existing .partial file and start the join from zero")
    join.set_defaults(resume=True)
    join.set_defaults(func=command_join)

    serve = subparsers.add_parser("serve", help="run the HTTP receiver and web UI")
    serve.add_argument("--host", default="127.0.0.1", help="host/interface to bind")
    serve.add_argument("--port", type=int, default=8080, help="port to listen on")
    serve.add_argument("--inbox", default="offsplit-inbox", help="directory for received .ofm/.ofs files")
    serve.add_argument("--completed", default="offsplit-completed", help="directory for joined output files")
    serve.add_argument("--token", help="optional bearer token for HTTP API and UI")
    serve.set_defaults(func=command_serve)

    send = subparsers.add_parser("send", help="send MessagePack parts to an HTTP receiver")
    send.add_argument("input", help="MessagePack parts directory or .ofm manifest")
    send.add_argument("url", help="receiver base URL, e.g. http://host:8080")
    send.add_argument("--token", help="bearer token if the receiver requires one")
    send.add_argument("-f", "--force", action="store_true", help="upload all frames instead of only missing frames")
    send.set_defaults(func=command_send)

    ws_serve = subparsers.add_parser("ws-serve", help="run a WebSocket receiver")
    ws_serve.add_argument("--host", default="127.0.0.1", help="host/interface to bind")
    ws_serve.add_argument("--port", type=int, default=8090, help="port to listen on")
    ws_serve.add_argument("--inbox", default="offsplit-ws-inbox", help="directory for received .ofm/.ofs files")
    ws_serve.add_argument("--completed", default="offsplit-ws-completed", help="directory for joined output files")
    ws_serve.add_argument("--token", help="optional bearer token")
    ws_serve.add_argument("--no-join", dest="join", action="store_false", help="receive only; do not auto-join when complete")
    ws_serve.set_defaults(join=True)
    ws_serve.set_defaults(func=command_ws_serve)

    ws_send = subparsers.add_parser("ws-send", help="send MessagePack parts to a WebSocket receiver")
    ws_send.add_argument("input", help="MessagePack parts directory or .ofm manifest")
    ws_send.add_argument("url", help="receiver URL, e.g. ws://host:8090")
    ws_send.add_argument("--token", help="bearer token if the receiver requires one")
    ws_send.add_argument("-f", "--force", action="store_true", help="send all frames instead of only missing frames")
    ws_send.set_defaults(func=command_ws_send)

    udp_serve = subparsers.add_parser("udp-serve", help="run a reliable UDP receiver")
    udp_serve.add_argument("--host", default="127.0.0.1", help="host/interface to bind")
    udp_serve.add_argument("--port", type=int, default=9000, help="UDP port to listen on")
    udp_serve.add_argument("--inbox", default="offsplit-udp-inbox", help="directory for received .ofm/.ofs files")
    udp_serve.add_argument("--completed", default="offsplit-udp-completed", help="directory for joined output files")
    udp_serve.add_argument("--token", help="optional shared token")
    udp_serve.add_argument("--no-join", dest="join", action="store_false", help="receive only; do not auto-join when complete")
    udp_serve.set_defaults(join=True)
    udp_serve.set_defaults(func=command_udp_serve)

    udp_send = subparsers.add_parser("udp-send", help="send MessagePack parts to a UDP receiver")
    udp_send.add_argument("input", help="MessagePack parts directory or .ofm manifest")
    udp_send.add_argument("url", help="receiver URL, e.g. udp://host:9000")
    udp_send.add_argument("--token", help="shared token if the receiver requires one")
    udp_send.add_argument("-f", "--force", action="store_true", help="send all frames instead of only missing frames")
    udp_send.add_argument("--retries", type=int, default=5, help="retry count per packet")
    udp_send.add_argument("--timeout", type=float, default=1.5, help="ACK timeout in seconds")
    udp_send.add_argument("--max-datagram", type=int, default=60000, help="maximum UDP datagram size")
    udp_send.set_defaults(func=command_udp_send)

    quic_serve = subparsers.add_parser("quic-serve", help="run a QUIC receiver")
    quic_serve.add_argument("--host", default="127.0.0.1", help="host/interface to bind")
    quic_serve.add_argument("--port", type=int, default=9443, help="QUIC UDP port to listen on")
    quic_serve.add_argument("--inbox", default="offsplit-quic-inbox", help="directory for received .ofm/.ofs files")
    quic_serve.add_argument("--completed", default="offsplit-quic-completed", help="directory for joined output files")
    quic_serve.add_argument("--token", help="optional shared token")
    quic_serve.add_argument("--cert", help="TLS certificate path; self-signed temporary cert is used if omitted")
    quic_serve.add_argument("--key", help="TLS private key path; self-signed temporary key is used if omitted")
    quic_serve.add_argument("--no-join", dest="join", action="store_false", help="receive only; do not auto-join when complete")
    quic_serve.set_defaults(join=True)
    quic_serve.set_defaults(func=command_quic_serve)

    quic_send = subparsers.add_parser("quic-send", help="send MessagePack parts to a QUIC receiver")
    quic_send.add_argument("input", help="MessagePack parts directory or .ofm manifest")
    quic_send.add_argument("url", help="receiver URL, e.g. quic://host:9443")
    quic_send.add_argument("--token", help="shared token if the receiver requires one")
    quic_send.add_argument("-f", "--force", action="store_true", help="send all frames instead of only missing frames")
    quic_send.add_argument("--secure", dest="insecure", action="store_false", help="verify QUIC TLS certificates")
    quic_send.set_defaults(insecure=True)
    quic_send.set_defaults(func=command_quic_send)

    mqtt_send = subparsers.add_parser("mqtt-send", help="send MessagePack parts through an MQTT broker")
    mqtt_send.add_argument("input", help="MessagePack parts directory or .ofm manifest")
    mqtt_send.add_argument("url", help="broker URL, e.g. mqtt://host:1883")
    mqtt_send.add_argument("--topic", default="offsplit", help="base MQTT topic")
    mqtt_send.add_argument("--client-id", help="MQTT client id")
    mqtt_send.add_argument("--username", help="MQTT username")
    mqtt_send.add_argument("--password", help="MQTT password")
    mqtt_send.add_argument("--retain-manifest", action="store_true", help="publish manifest as retained message")
    mqtt_send.set_defaults(func=command_mqtt_send)

    mqtt_recv = subparsers.add_parser("mqtt-recv", help="receive MessagePack parts from an MQTT broker")
    mqtt_recv.add_argument("url", help="broker URL, e.g. mqtt://host:1883")
    mqtt_recv.add_argument("--topic", default="offsplit", help="base MQTT topic")
    mqtt_recv.add_argument("--inbox", default="offsplit-mqtt-inbox", help="directory for received .ofm/.ofs files")
    mqtt_recv.add_argument("--completed", default="offsplit-mqtt-completed", help="directory for joined output files")
    mqtt_recv.add_argument("--client-id", help="MQTT client id")
    mqtt_recv.add_argument("--username", help="MQTT username")
    mqtt_recv.add_argument("--password", help="MQTT password")
    mqtt_recv.add_argument("--no-join", dest="join", action="store_false", help="receive only; do not auto-join when complete")
    mqtt_recv.add_argument("--once", action="store_true", help="exit after the first complete joined transfer")
    mqtt_recv.set_defaults(join=True)
    mqtt_recv.set_defaults(func=command_mqtt_recv)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except FrameError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
