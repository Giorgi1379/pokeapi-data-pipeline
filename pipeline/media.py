import base64  # Built-in library used to encode the MD5 checksum into Base64 (the format Google Cloud Storage uses)
import hashlib  # Built-in library used to compute MD5 checksums of raw image bytes
import logging  # Built-in library for logging media download/upload progress and warnings
import os  # Built-in library for checking environment variables and file paths
import threading  # Built-in threading module providing local() and Lock()
import time  # Built-in time module used for exponential backoff sleep between retries
from concurrent.futures import ThreadPoolExecutor, as_completed  # Thread pool manager for parallel sprite processing
from typing import Any, Dict, List, Optional, Tuple  # Type hints for clean function signatures
import pandas as pd  # Used to receive the sprites DataFrame and check for null/NaN URLs
import requests  # Synchronous HTTP client used to download raw PNG and GIF bytes
from google.cloud import storage  # Official Google Cloud Storage SDK for uploading blobs to your bucket

from pipeline.config import PipelineConfig  # Imports our frozen pipeline configuration dataclass
from pipeline.loader import PostgresLoader  # Imports PostgresLoader so worker threads can update the sprites table

logger = logging.getLogger(__name__)  # Creates a module-level logger for media.py


class ThreadedMediaArchiver:  # Main class handling Stage 5 (Threaded Sprite Downloading & GCS Archiving)
    """
    Stage 5 (Media): Downloads static PNG and animated GIF sprites in a thread pool,
    caches raw bytes on disk, uploads to GCS based on response Content-Type,
    verifies MD5 checksums for idempotency, and updates PostgreSQL.
    """

    def __init__(self, config: PipelineConfig, loader: PostgresLoader) -> None:  # Constructor receiving config and DB loader
        self.config = config  # Stores the pipeline configuration (worker_count, gcs_bucket, retries, cache_dir)
        self.loader = loader  # Stores the PostgresLoader instance (uses ThreadedConnectionPool for safe DB updates)
        self._thread_local = threading.local()  # Thread-local storage so each worker thread gets its own Session and GCS Client
        self._lock = threading.Lock()  # Mutex lock protecting shared counters (self.uploaded_count) and self.errors
        self.uploaded_count = 0  # Thread-safe counter tracking how many sprites were successfully processed
        self.errors: List[str] = []  # Thread-safe list storing any download or GCS upload error messages

        self.sprite_cache_dir = self.config.cache_dir / "sprites"  # Path to local sprite disk cache (cache/sprites/)
        self.sprite_cache_dir.mkdir(parents=True, exist_ok=True)  # Creates cache/sprites/ if it doesn't exist yet

        cred_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")  # Reads the GCP service account key file path from .env
        self.gcs_enabled = bool(cred_path and os.path.isfile(cred_path))  # True only if the path is set AND points to a real file on disk
        if not self.gcs_enabled:  # If no valid GCP key file is configured yet
            logger.warning(  # Logs a friendly warning and runs in local metadata mode instead of crashing
                "GOOGLE_APPLICATION_CREDENTIALS not pointing to a valid key file yet. "
                "Sprites will be downloaded, checksummed, and recorded in PostgreSQL."
            )

    def _get_session(self) -> requests.Session:  # Returns the calling thread's private HTTP Session
        if not hasattr(self._thread_local, "session"):  # Checks if THIS worker thread has created a requests.Session yet
            s = requests.Session()  # Creates a new HTTP session (reuses TCP/TLS connections via Keep-Alive)
            s.headers.update({"User-Agent": "PokeAPI-Threaded-Data-Pipeline/1.0"})  # Sets a custom User-Agent header
            self._thread_local.session = s  # Stores the Session on this thread's private storage
        return self._thread_local.session  # Returns this thread's dedicated Session

    def _get_storage_client(self) -> storage.Client:  # Returns the calling thread's private GCS Client
        if not hasattr(self._thread_local, "gcs_client"):  # Checks if THIS worker thread has created a GCS Client yet
            self._thread_local.gcs_client = storage.Client()  # Instantiates one google.cloud.storage.Client per worker thread
        return self._thread_local.gcs_client  # Returns this thread's dedicated GCS Client (never shared across threads!)

    def _record_error(self, msg: str) -> None:  # Thread-safe helper to record download or upload failures
        with self._lock:  # Acquires the lock before modifying the shared errors list
            self.errors.append(msg)  # Appends the error message to self.errors
        logger.error(msg)  # Prints the error message to the terminal log

    def _download_bytes(self, url: str, cache_key: str) -> Optional[Tuple[bytes, str]]:  # Downloads raw image bytes (or reads from disk cache)
        cache_path = self.sprite_cache_dir / f"{cache_key}.bin"  # File path for the cached raw image bytes (e.g., 1_png.bin)
        meta_path = self.sprite_cache_dir / f"{cache_key}.type"  # File path for the cached HTTP Content-Type string (e.g., 1_png.type)

        if cache_path.exists() and meta_path.exists():  # If this sprite is already cached locally on disk
            return cache_path.read_bytes(), meta_path.read_text(encoding="utf-8").strip()  # Returns cached bytes and Content-Type immediately

        session = self._get_session()  # Gets this worker thread's private requests.Session()
        for attempt in range(1, self.config.max_retries + 1):  # Retries up to max_retries times (Attempts 1, 2, 3)
            try:  # Attempts to download the sprite over HTTP
                resp = session.get(url, timeout=4)  # Performs HTTP GET with a strict 4-second timeout
                resp.raise_for_status()  # Raises an exception if HTTP status is 4xx or 5xx
                content_type = (  # Extracts and cleans the HTTP Content-Type header (e.g., "image/png" or "image/gif")
                    resp.headers.get("Content-Type", "").split(";")[0].strip().lower()
                )
                cache_path.write_bytes(resp.content)  # Saves the raw binary image bytes to cache/sprites/
                meta_path.write_text(content_type, encoding="utf-8")  # Saves the Content-Type header string to cache/sprites/
                return resp.content, content_type  # Returns the raw bytes and Content-Type string
            except Exception as exc:  # Catches any network timeout or HTTP error
                if attempt < self.config.max_retries:  # If we have retries remaining
                    sleep_s = self.config.backoff_factor * (2 ** (attempt - 1))  # Exponential backoff: 1s, 2s, 4s
                    time.sleep(sleep_s)  # Sleeps this worker thread before retrying
                else:  # If all 3 attempts failed
                    self._record_error(  # Logs and records the failure in self.errors
                        f"Failed to download sprite {url} after {self.config.max_retries} attempts: {exc}"
                    )
        return None  # Returns None if downloading failed after all retries

    def _upload_to_gcs(  # Uploads the raw image bytes to Google Cloud Storage idempotently
        self, blob_name: str, raw_bytes: bytes, content_type: str, md5_b64: str
    ) -> Optional[str]:
        if not self.gcs_enabled:  # If GCP credentials aren't enabled yet (dry-run mode)
            return f"gs://{self.config.gcs_bucket}/{blob_name}"  # Returns the formatted gs:// URI so PostgreSQL is still populated

        client = self._get_storage_client()  # Gets this worker thread's private GCS storage.Client()
        bucket = client.bucket(self.config.gcs_bucket)  # References your target GCS bucket (e.g., "pokemon-giorgi")
        blob = bucket.blob(blob_name)  # References the target object path (e.g., "pngs/1_bulbasaur.png")

        for attempt in range(1, self.config.max_retries + 1):  # Retries GCS operations up to 3 times with backoff
            try:  # Attempts the idempotency check and upload
                if blob.exists():  # Checks if the file already exists in your GCS bucket
                    blob.reload()  # Fetches the existing object's metadata (including its MD5 hash) from GCS
                    if blob.md5_hash == md5_b64:  # If the remote GCS MD5 matches our local file's MD5 checksum
                        return f"gs://{self.config.gcs_bucket}/{blob_name}"  # Skips re-uploading (idempotent!) and returns the gs:// URI

                blob.upload_from_string(raw_bytes, content_type=content_type)  # Uploads the raw bytes as-is with the exact Content-Type
                return f"gs://{self.config.gcs_bucket}/{blob_name}"  # Returns the gs:// URI after successful upload
            except Exception as exc:  # Catches any transient GCS network/API error
                if attempt < self.config.max_retries:  # If retries remain
                    sleep_s = self.config.backoff_factor * (2 ** (attempt - 1))  # Calculates exponential backoff wait time
                    time.sleep(sleep_s)  # Pauses this worker thread before retrying
                else:  # If all 3 upload attempts failed
                    self._record_error(  # Records the upload error for the pipeline_runs audit table
                        f"Failed to upload {blob_name} to GCS after {self.config.max_retries} attempts: {exc}"
                    )
        return None  # Returns None if the upload failed after all retries

    def _process_single_sprite(self, row: Dict[str, Any]) -> None:  # Worker function run by a thread for 1 sprite row
        pokemon_id = int(row["pokemon_id"])  # Extracts the integer Pokémon ID (e.g., 1)
        pokemon_name = str(row["pokemon_name"])  # Extracts the Pokémon slug name (e.g., "bulbasaur")
        sprite_kind = str(row["sprite_kind"])  # Extracts the sprite kind ('png' or 'gif')
        source_url = row.get("source_url")  # Extracts the PokéAPI image URL (or None if missing)

        if not source_url or pd.isna(source_url):  # Rule: If a sprite URL is null, record it as missing instead of failing
            self.loader.update_sprite_metadata(  # Updates the sprites row in PostgreSQL with is_missing=True
                pokemon_id=pokemon_id,
                sprite_kind=sprite_kind,
                gcs_uri=None,  # No GCS URI since the API had no URL
                content_type=None,  # No Content-Type
                size_bytes=None,  # No byte size
                checksum_md5=None,  # No MD5 checksum
                is_missing=True,  # Flags that this sprite is missing in the upstream API
            )
            return  # Exits cleanly without raising an error

        downloaded = self._download_bytes(str(source_url), f"{pokemon_id}_{sprite_kind}")  # Downloads (or reads cached) bytes + Content-Type
        if not downloaded:  # If the download failed after 3 retries
            return  # Exits cleanly (the error was already logged by _download_bytes)

        raw_bytes, content_type = downloaded  # Unpacks the raw image bytes and HTTP Content-Type header
        if content_type == "image/gif":  # Rule: Choose the folder from the HTTP response Content-Type, not the URL extension
            folder, ext = "gifs", "gif"  # Routes animated GIFs into gs://<bucket>/gifs/<id>_<name>.gif
        elif content_type == "image/png":  # If the HTTP header says image/png
            folder, ext = "pngs", "png"  # Routes static PNGs into gs://<bucket>/pngs/<id>_<name>.png
        else:  # Fallback if an upstream server ever omits the Content-Type header
            folder = "gifs" if sprite_kind == "gif" else "pngs"  # Infers folder from sprite_kind
            ext = "gif" if sprite_kind == "gif" else "png"  # Infers extension from sprite_kind
            content_type = "image/gif" if ext == "gif" else "image/png"  # Normalizes content_type string

        blob_name = f"{folder}/{pokemon_id}_{pokemon_name}.{ext}"  # Builds the exact GCS object path: e.g., "gifs/1_bulbasaur.gif"
        size_bytes = len(raw_bytes)  # Measures the exact file size in bytes
        md5_digest = hashlib.md5(raw_bytes).digest()  # Computes the raw 16-byte MD5 digest of the image bytes
        md5_hex = md5_digest.hex()  # Converts MD5 to a 32-character hex string to store in PostgreSQL (checksum_md5)
        md5_b64 = base64.b64encode(md5_digest).decode("utf-8")  # Converts MD5 to Base64 to compare against GCS blob.md5_hash

        gcs_uri = self._upload_to_gcs(blob_name, raw_bytes, content_type, md5_b64)  # Uploads to GCS (or skips if unchanged)
        if gcs_uri:  # If upload (or checksum match) succeeded
            self.loader.update_sprite_metadata(  # Writes gcs_uri, content_type, size_bytes, and checksum_md5 to PostgreSQL
                pokemon_id=pokemon_id,
                sprite_kind=sprite_kind,
                gcs_uri=gcs_uri,
                content_type=content_type,
                size_bytes=size_bytes,
                checksum_md5=md5_hex,
                is_missing=False,
            )
            with self._lock:  # Acquires the lock before incrementing the shared uploaded_count integer
                self.uploaded_count += 1  # Safely increments the processed sprite counter by 1

    def run_media_archiver(self, sprites_df: pd.DataFrame) -> int:  # Entry point that runs Stage 5 across all 100 sprites
        records = sprites_df.to_dict("records")  # Converts the pandas DataFrame of 100 sprites into a list of dicts
        logger.info(  # Logs the start of the threaded media stage
            f"Archiving {len(records)} sprites using {self.config.worker_count} worker threads..."
        )
        with ThreadPoolExecutor(  # Spawns the pool of MediaWorker threads
            max_workers=self.config.worker_count, thread_name_prefix="MediaWorker"
        ) as executor:
            futures = [  # Submits all 100 sprite tasks to the thread pool concurrently
                executor.submit(self._process_single_sprite, rec) for rec in records
            ]
            for f in as_completed(futures):  # Waits for every worker task to complete
                f.result()  # Surfaces any unexpected exception if one occurred

        logger.info(  # Logs final summary of processed sprites and error count
            f"Media stage complete! Processed {self.uploaded_count} sprites. Errors: {len(self.errors)}"
        )
        return self.uploaded_count  # Returns total count (100) to record in pipeline_runs
