import os
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional

DEFAULT_VAULT_PILOT_DIR = "/Users/liumingwei/vaults/AresVault/05_Team_Knowledge_Maintenance/00_Inbox/continuous_intelligence_pilot"

def is_subpath(target_path: str, base_dir: str) -> bool:
    """
    Checks if target_path is strictly within base_dir after resolving symlinks and '..'.
    """
    real_target = os.path.realpath(target_path)
    real_base = os.path.realpath(base_dir)
    # Ensure real_base has trailing slash for accurate prefix match
    real_base_prefix = os.path.join(real_base, "")
    return real_target.startswith(real_base_prefix) or real_target == real_base

def export_briefing_to_vault(
    briefing_md: str,
    run_id: str,
    target_dir: str = DEFAULT_VAULT_PILOT_DIR,
    allowed_root: Optional[str] = None,
    custom_filename: Optional[str] = None
) -> Dict[str, Any]:
    """
    Exports generated candidate intelligence briefing to the AresVault pilot directory.
    Strict security hardening:
    - Resolves real paths using os.path.realpath
    - Whitelists allowed root directory (prevents path traversal and symlink escapes)
    - Strictly forbids touching 02_Team_Archives (TEAM_ARCHIVE_WRITE = 0)
    - Rejects silent overwrite if the destination file already exists
    - Dynamically generates current UTC date in filename
    - Performs mandatory independent post-write physical readback verification
    """
    effective_allowed_root = os.path.realpath(allowed_root or DEFAULT_VAULT_PILOT_DIR)
    resolved_target_dir = os.path.realpath(target_dir)

    # 1. Enforce directory boundary security
    if not is_subpath(resolved_target_dir, effective_allowed_root):
        raise PermissionError(
            f"[SECURITY VIOLATION] Target directory '{resolved_target_dir}' is outside the authorized vault root '{effective_allowed_root}'."
        )

    # 2. Hard line: Absolute prohibition of writing to 02_Team_Archives
    if "02_Team_Archives" in resolved_target_dir:
        raise PermissionError(
            f"[CRITICAL RED LINE VIOLATION] Write to 02_Team_Archives is strictly prohibited! Target: {resolved_target_dir}"
        )

    os.makedirs(resolved_target_dir, exist_ok=True)

    # 3. Dynamic date in filename
    current_date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    filename = custom_filename or f"{current_date_str}_EPL_Candidate_Intelligence_Briefing_{run_id}.md"
    target_path = os.path.join(resolved_target_dir, filename)
    real_target_path = os.path.realpath(target_path)

    # Final boundary check on the exact file path
    if not is_subpath(real_target_path, effective_allowed_root):
        raise PermissionError(f"[SECURITY VIOLATION] Target file path '{real_target_path}' escapes the authorized root '{effective_allowed_root}'.")

    # 4. Refuse silent overwrite if destination already exists
    if os.path.exists(real_target_path):
        raise FileExistsError(
            f"[OVERWRITE PROHIBITED] Target file already exists: {real_target_path}. Silent overwrite is strictly prohibited."
        )

    content_bytes = briefing_md.encode('utf-8')
    expected_sha = hashlib.sha256(content_bytes).hexdigest()
    expected_len = len(content_bytes)

    # Physical atomic write
    temp_target = real_target_path + ".tmp"
    with open(temp_target, "wb") as f:
        f.write(content_bytes)
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp_target, real_target_path)

    # 5. Independent post-write physical readback verification
    if not os.path.exists(real_target_path):
        raise FileNotFoundError(f"Verification failure: Target file {real_target_path} was not created.")

    with open(real_target_path, "rb") as f:
        readback_bytes = f.read()

    readback_sha = hashlib.sha256(readback_bytes).hexdigest()
    readback_len = len(readback_bytes)

    if readback_sha != expected_sha or readback_len != expected_len:
        raise IOError(
            f"Physical readback integrity mismatch for {real_target_path}: "
            f"expected {expected_sha} ({expected_len}b), got {readback_sha} ({readback_len}b)"
        )

    return {
        "status": "EXPORT_VERIFIED_SUCCESS",
        "target_path": real_target_path,
        "byte_length": readback_len,
        "sha256": readback_sha
    }
