import os
import json
import re
import hashlib
import tempfile
import shutil
from datetime import datetime, timezone
from typing import Dict, Any, Optional

def normalize_text(text: str) -> str:
    """
    Normalizes text for semantic deduplication:
    - Removes punctuation
    - Collapses whitespaces
    - Lowercases
    """
    if not text:
        return ""
    # Strip HTML tags if any residual
    text = re.sub(r'<[^>]+>', ' ', text)
    # Strip non-alphanumeric (keep english/latin/digits/chinese)
    text = re.sub(r'[^\w\s\u4e00-\u9fa5]', ' ', text)
    # Collapse whitespaces
    text = re.sub(r'\s+', ' ', text).strip().lower()
    return text

def compute_semantic_fingerprint(title: str, paragraphs: list) -> str:
    """
    Computes deterministic SHA-256 over normalized Title + Main Body.
    Excludes navigation, recommendation widgets, and ads.
    """
    norm_title = normalize_text(title)
    norm_body = " ".join([normalize_text(p) for p in paragraphs if normalize_text(p)])
    payload = f"TITLE:{norm_title}\nBODY:{norm_body}"
    return hashlib.sha256(payload.encode('utf-8')).hexdigest()

class FingerprintRegistry:
    def __init__(self, registry_file: str):
        self.registry_file = os.path.abspath(registry_file)
        self.records: Dict[str, Any] = {}
        self.is_initial_empty: bool = True
        self.load()

    def load(self):
        """
        Loads records from JSON.
        Strict error handling: If the file exists but contains corrupted JSON,
        it creates a backup and raises an error instead of silently wiping it.
        """
        if os.path.exists(self.registry_file):
            try:
                with open(self.registry_file, "r", encoding="utf-8") as f:
                    self.records = json.load(f)
                if not isinstance(self.records, dict):
                    raise ValueError(f"Registry format invalid: root must be a dict, got {type(self.records)}")
                self.is_initial_empty = (len(self.records) == 0)
            except Exception as e:
                # Backup corrupted registry to prevent data loss
                backup_path = f"{self.registry_file}.corrupted_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
                shutil.copy2(self.registry_file, backup_path)
                raise ValueError(
                    f"[CRITICAL INTEGRITY ERROR] Fingerprint registry at {self.registry_file} is corrupted ({e}). "
                    f"Corrupted file preserved at: {backup_path}. Refusing to silently overwrite."
                )
        else:
            self.records = {}
            self.is_initial_empty = True

    def save(self):
        """
        Saves registry atomically using tempfile and os.replace.
        """
        target_dir = os.path.dirname(self.registry_file)
        os.makedirs(target_dir, exist_ok=True)

        # Write to temporary file in the same directory (ensures same filesystem for os.replace)
        with tempfile.NamedTemporaryFile("w", dir=target_dir, delete=False, encoding="utf-8") as tf:
            temp_path = tf.name
            json.dump(self.records, tf, indent=2, ensure_ascii=False)
            tf.flush()
            os.fsync(tf.fileno())

        os.replace(temp_path, self.registry_file)

    def evaluate_and_register(
        self,
        url: str,
        raw_sha256: str,
        title: str,
        paragraphs: list,
        fetch_utc: str,
        is_dry_run: bool = False
    ) -> Dict[str, Any]:
        """
        Evaluates deduplication status:
        - STATUS_BASELINE_NEW: First run initialization across batch when registry was originally empty
        - STATUS_NEW: New URL discovered on subsequent runs
        - STATUS_DUPLICATE: Same URL, identical semantic fingerprint (ignored in briefing)
        - STATUS_MUTATED: Same URL, semantic fingerprint changed (supersedes old version)
        """
        sem_fp = compute_semantic_fingerprint(title, paragraphs)
        existing = self.records.get(url)

        if not existing:
            # If the registry was empty when loaded for this batch, all new entries are baseline
            status = "STATUS_BASELINE_NEW" if self.is_initial_empty else "STATUS_NEW"
            entry = {
                "url": url,
                "first_seen_utc": fetch_utc,
                "latest_seen_utc": fetch_utc,
                "raw_sha256": raw_sha256,
                "semantic_fingerprint": sem_fp,
                "title": title,
                "version": 1,
                "mutation_history": []
            }
            if not is_dry_run:
                self.records[url] = entry
                self.save()
            return {
                "status": status,
                "semantic_fingerprint": sem_fp,
                "raw_sha256": raw_sha256,
                "version": 1,
                "supersedes": None,
                "is_dry_run": is_dry_run
            }

        # Existing record check
        if existing.get("semantic_fingerprint") == sem_fp:
            # Semantic content unchanged
            if not is_dry_run:
                existing["latest_seen_utc"] = fetch_utc
                existing["raw_sha256"] = raw_sha256
                self.save()
            return {
                "status": "STATUS_DUPLICATE",
                "semantic_fingerprint": sem_fp,
                "raw_sha256": raw_sha256,
                "version": existing.get("version", 1),
                "supersedes": None,
                "is_dry_run": is_dry_run
            }
        else:
            # Semantic content genuinely changed
            old_version = existing.get("version", 1)
            new_version = old_version + 1
            old_fp = existing.get("semantic_fingerprint")
            old_raw = existing.get("raw_sha256")

            history_entry = {
                "superseded_version": old_version,
                "superseded_semantic_fingerprint": old_fp,
                "superseded_raw_sha256": old_raw,
                "mutated_at_utc": fetch_utc
            }

            if not is_dry_run:
                existing["version"] = new_version
                existing["semantic_fingerprint"] = sem_fp
                existing["raw_sha256"] = raw_sha256
                existing["title"] = title
                existing["latest_seen_utc"] = fetch_utc
                if "mutation_history" not in existing:
                    existing["mutation_history"] = []
                existing["mutation_history"].append(history_entry)
                self.save()

            return {
                "status": "STATUS_MUTATED",
                "semantic_fingerprint": sem_fp,
                "raw_sha256": raw_sha256,
                "version": new_version,
                "supersedes": {
                    "version": old_version,
                    "semantic_fingerprint": old_fp
                },
                "is_dry_run": is_dry_run
            }
