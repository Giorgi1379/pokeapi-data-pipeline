import json  # Built-in library for reading and writing JSON files to the local cache
import logging  # Built-in library for printing timestamped, thread-aware log messages
import threading  # Built-in threading module providing Lock(), local(), and get_ident()
import time  # Built-in time module used for sleeping during exponential backoff retries
from concurrent.futures import ThreadPoolExecutor, as_completed  # Thread pool manager and helper to process finished tasks as they complete
from pathlib import Path  # Object-oriented filesystem paths
from typing import Any, Dict, List, Optional, Set, Tuple  # Type hints for clean, self-documenting function signatures
import requests  # Synchronous HTTP client library (used instead of async libraries per project rules)

from pipeline.config import PipelineConfig, load_config  # Imports our frozen config dataclass and config loader

logging.basicConfig(  # Configures the global logger output format
    level=logging.INFO,  # Logs INFO, WARNING, and ERROR messages
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",  # Includes [threadName] so you can see ExtractorWorker_0, _1, etc. working in parallel
)
logger = logging.getLogger(__name__)  # Creates a logger instance for this module


class ThreadedPokeExtractor:  # Main class handling Stage 1 (Discover) and Stage 2 (Threaded Extract)
    """
    Multi-threaded PokéAPI extractor adhering strictly to project rules:
    - Uses ThreadPoolExecutor (no asyncio).
    - Uses one requests.Session() per worker thread via threading.local().
    - Protects all shared state (URL cache, seen ID sets, error counters) with threading.Lock().
    - Caches raw JSON responses on disk to respect PokéAPI fair-use policy.
    """

    def __init__(self, config: PipelineConfig) -> None:  # Constructor that receives our PipelineConfig object
        self.config = config  # Stores the configuration (limit, offset, worker_count, cache_dir, etc.)
        self._thread_local = threading.local()  # Thread-local storage: each worker thread gets its own isolated attributes on this object

        # Lock protecting shared state across worker threads
        self._state_lock = threading.Lock()  # Mutual-exclusion lock (Mutex) so only 1 thread can read/modify shared sets at a time

        # Shared state protected by self._state_lock
        self.fetched_urls: Set[str] = set()  # Tracks all URLs already fetched during this run
        self.seen_species_ids: Set[int] = set()  # Tracks which species IDs have already been claimed by a thread
        self.seen_chain_ids: Set[int] = set()  # Tracks which evolution-chain IDs have already been claimed (prevents duplicate fetches)
        self.seen_type_ids: Set[int] = set()  # Tracks which type IDs (e.g., Grass, Poison) have already been claimed
        self.seen_ability_ids: Set[int] = set()  # Tracks which ability IDs have already been claimed
        self.errors: List[str] = []  # Thread-safe list of error messages if any endpoint fails after all retries

        # Create cache subdirectories
        for subdir in [  # Loops over each PokéAPI endpoint category we cache on disk
            "pokemon",  # Folder for /pokemon/{id} JSON files
            "species",  # Folder for /pokemon-species/{id} JSON files
            "encounters",  # Folder for /pokemon/{id}/encounters JSON files
            "evolution_chains",  # Folder for /evolution-chain/{id} JSON files
            "types",  # Folder for /type/{id} JSON files
            "abilities",  # Folder for /ability/{id} JSON files
        ]:
            (self.config.cache_dir / subdir).mkdir(parents=True, exist_ok=True)  # Creates cache/<subdir> if it doesn't exist yet

    def _get_session(self) -> requests.Session:  # Returns the calling thread's private HTTP Session
        """Returns a thread-local requests.Session instance."""
        if not hasattr(self._thread_local, "session"):  # Checks if THIS specific worker thread has created a Session yet
            session = requests.Session()  # Creates a new HTTP session (reuses TCP/TLS connections via Keep-Alive)
            session.headers.update(  # Adds a polite User-Agent header as required by PokéAPI fair-use policy
                {"User-Agent": "PokeAPI-Threaded-Data-Pipeline/1.0"}
            )
            self._thread_local.session = session  # Saves the Session onto this thread's private storage
        return self._thread_local.session  # Returns this thread's dedicated Session

    def _record_error(self, message: str) -> None:  # Thread-safe helper to log and store failed requests
        with self._state_lock:  # Acquires the lock so two threads don't append to self.errors simultaneously
            self.errors.append(message)  # Appends the error string to the shared errors list
        logger.error(message)  # Prints the error to the terminal log

    @staticmethod  # Utility method that doesn't need 'self'
    def extract_id_from_url(url: str) -> int:  # Parses the numeric ID from a PokéAPI URL like "https://.../pokemon/25/"
        """Extracts the numeric PokéAPI ID from a resource URL."""
        return int(url.rstrip("/").split("/")[-1])  # Strips trailing slash, splits by '/', takes the last segment, and converts to int

    def fetch_json(self, url: str, cache_path: Optional[Path] = None) -> Optional[Any]:  # Fetches JSON from disk cache or HTTP GET
        """
        Fetches JSON from local disk cache if available, otherwise performs
        an HTTP GET using the thread-local Session with up to 3 retries + backoff.
        """
        if cache_path and cache_path.exists():  # Checks if we already downloaded and cached this JSON file on disk
            try:  # Attempts to read the cached JSON file
                with open(cache_path, "r", encoding="utf-8") as f:  # Opens the local cache file for reading
                    data = json.load(f)  # Parses the JSON file into a Python dict/list
                with self._state_lock:  # Acquires the lock before updating the shared fetched_urls set
                    self.fetched_urls.add(url)  # Marks this URL as fetched
                return data  # Returns the cached data immediately without making an HTTP call
            except Exception as e:  # If the cache file was corrupted or partially written
                logger.warning(f"Corrupt cache file {cache_path}, re-fetching: {e}")  # Logs a warning and falls through to re-fetch via HTTP

        session = self._get_session()  # Gets the current worker thread's private requests.Session()
        for attempt in range(1, self.config.max_retries + 1):  # Loops up to max_retries times (Attempts 1, 2, 3)
            try:  # Tries the HTTP GET request
                resp = session.get(url, timeout=4)  # Sends GET request with a 4-second timeout so a slow call never hangs a thread
                resp.raise_for_status()  # Raises an exception if HTTP status is 4xx or 5xx
                data = resp.json()  # Parses the HTTP response body as JSON

                if cache_path:  # If a disk cache path was provided, save the JSON atomically
                    temp_path = cache_path.with_suffix(  # Creates a unique temp filename using the current thread ID
                        f".{threading.get_ident()}.tmp"
                    )
                    with open(temp_path, "w", encoding="utf-8") as f:  # Opens the temporary file for writing
                        json.dump(data, f)  # Writes the JSON data to the temporary file
                    temp_path.replace(cache_path)  # Atomically renames the temp file to the final cache_path (prevents half-written reads!)

                with self._state_lock:  # Acquires the lock before updating fetched_urls
                    self.fetched_urls.add(url)  # Records that this URL succeeded
                return data  # Returns the parsed JSON data
            except Exception as exc:  # Catches network timeouts, connection errors, or HTTP 5xx errors
                if attempt < self.config.max_retries:  # If we still have retry attempts left
                    sleep_time = self.config.backoff_factor * (2 ** (attempt - 1))  # Calculates exponential backoff: 1s, then 2s, then 4s
                    logger.warning(  # Logs a warning showing which retry attempt is happening
                        f"Retry {attempt}/{self.config.max_retries} for {url} after {sleep_time}s: {exc}"
                    )
                    time.sleep(sleep_time)  # Pauses only this worker thread before retrying
                else:  # If all 3 attempts failed
                    self._record_error(  # Records the permanent failure in self.errors for the pipeline_runs audit table
                        f"Failed to fetch {url} after {self.config.max_retries} attempts: {exc}"
                    )
        return None  # Returns None if all retries failed so the pipeline continues without crashing

    def discover_pokemon_ids(self) -> List[int]:  # Stage 1 (Discover): gets the list of target Pokémon IDs
        """Stage 1 (Discover): Fetch paginated /pokemon list and return target Pokémon IDs."""
        url = (  # Builds the paginated seed URL: https://pokeapi.co/api/v2/pokemon?limit=50&offset=0
            f"{self.config.api_base_url}/pokemon"
            f"?limit={self.config.pokemon_limit}&offset={self.config.pokemon_offset}"
        )
        seed_cache = (  # Defines the cache file path for this specific limit and offset slice
            self.config.cache_dir
            / f"seed_limit_{self.config.pokemon_limit}_offset_{self.config.pokemon_offset}.json"
        )
        data = self.fetch_json(url, seed_cache)  # Fetches the seed list from cache or PokéAPI
        if not data or "results" not in data:  # Validates that the seed list returned a "results" array
            raise RuntimeError("Failed to fetch seed Pokémon list from PokéAPI")  # Stops if the initial seed list is unreachable

        pokemon_ids = [  # Extracts the integer ID (1..50) from each Pokémon's URL in the results list
            self.extract_id_from_url(item["url"]) for item in data["results"]
        ]
        logger.info(  # Logs how many Pokémon were discovered and their ID range
            f"Discovered {len(pokemon_ids)} Pokémon (IDs #{min(pokemon_ids)} to #{max(pokemon_ids)})"
        )
        return pokemon_ids  # Returns the list of 50 Pokémon IDs

    def _extract_single_pokemon(  # Worker function executed by a thread for 1 Pokémon ID
        self, pokemon_id: int
    ) -> Tuple[List[int], List[int], Optional[int]]:
        """
        Worker task for a single Pokémon:
        Fetches /pokemon/{id}, /pokemon/{id}/encounters, and /pokemon-species/{species_id}.
        Returns newly discovered (type_ids, ability_ids, chain_id) to fetch next.
        """
        poke_url = f"{self.config.api_base_url}/pokemon/{pokemon_id}"  # URL for /pokemon/{id}
        poke_cache = self.config.cache_dir / "pokemon" / f"{pokemon_id}.json"  # Local cache path cache/pokemon/{id}.json
        poke_data = self.fetch_json(poke_url, poke_cache)  # Fetches the Pokémon JSON
        if not poke_data:  # If fetching this Pokémon failed after 3 retries
            return [], [], None  # Returns empty lists so the worker exits cleanly

        # Fetch encounters for this Pokémon
        enc_url = f"{self.config.api_base_url}/pokemon/{pokemon_id}/encounters"  # URL for catch locations per game version
        enc_cache = self.config.cache_dir / "encounters" / f"{pokemon_id}.json"  # Local cache path cache/encounters/{id}.json
        self.fetch_json(enc_url, enc_cache)  # Fetches and caches the encounters list

        # Fetch species for this Pokémon
        species_id = self.extract_id_from_url(poke_data["species"]["url"])  # Gets the species_id referenced by this Pokémon
        should_fetch_species = False  # Flag indicating whether THIS thread is responsible for fetching the species
        with self._state_lock:  # Locks shared state to prevent a check-then-act race condition on seen_species_ids
            if species_id not in self.seen_species_ids:  # Checks if any thread has claimed this species_id yet
                self.seen_species_ids.add(species_id)  # Claims this species_id
                should_fetch_species = True  # Marks that this thread will fetch it

        chain_id: Optional[int] = None  # Will hold the evolution chain ID if newly discovered
        species_cache = self.config.cache_dir / "species" / f"{species_id}.json"  # Local cache path cache/species/{id}.json
        if should_fetch_species or not species_cache.exists():  # Fetches species JSON if claimed or not yet cached
            species_url = f"{self.config.api_base_url}/pokemon-species/{species_id}"  # URL for /pokemon-species/{id}
            species_data = self.fetch_json(species_url, species_cache)  # Downloads or reads the species JSON
        else:  # If another thread already cached it
            with open(species_cache, "r", encoding="utf-8") as f:  # Opens the cached species JSON
                species_data = json.load(f)  # Loads it from disk

        if species_data and species_data.get("evolution_chain"):  # Checks if the species has an evolution_chain URL
            chain_url = species_data["evolution_chain"]["url"]  # Extracts the /evolution-chain/{id} URL
            cid = self.extract_id_from_url(chain_url)  # Extracts the numeric chain_id
            with self._state_lock:  # Acquires the lock so Bulbasaur, Ivysaur, and Venusaur don't all fetch chain #1
                if cid not in self.seen_chain_ids:  # Checks if this chain_id was already claimed by another thread
                    self.seen_chain_ids.add(cid)  # Claims this chain_id
                    chain_id = cid  # Schedules this chain_id to be fetched in Wave 2

        # Check which types and abilities haven't been scheduled yet (Lock-protected!)
        new_types: List[int] = []  # Holds type IDs newly discovered by this thread
        for t in poke_data.get("types", []):  # Loops through this Pokémon's 1 or 2 types
            tid = self.extract_id_from_url(t["type"]["url"])  # Extracts the integer type_id
            with self._state_lock:  # Locks seen_type_ids so 15 Poison Pokémon only trigger 1 fetch to /type/4
                if tid not in self.seen_type_ids:  # If this type hasn't been claimed yet
                    self.seen_type_ids.add(tid)  # Marks this type_id as claimed
                    new_types.append(tid)  # Adds it to the list of types to fetch in Wave 2

        new_abilities: List[int] = []  # Holds ability IDs newly discovered by this thread
        for a in poke_data.get("abilities", []):  # Loops through this Pokémon's abilities
            aid = self.extract_id_from_url(a["ability"]["url"])  # Extracts the integer ability_id
            with self._state_lock:  # Locks seen_ability_ids to prevent duplicate ability fetches across threads
                if aid not in self.seen_ability_ids:  # If this ability hasn't been claimed yet
                    self.seen_ability_ids.add(aid)  # Marks this ability_id as claimed
                    new_abilities.append(aid)  # Adds it to the list of abilities to fetch in Wave 2

        return new_types, new_abilities, chain_id  # Returns newly claimed IDs back to the main thread

    def _collect_chain_species_ids(self, chain_node: Dict[str, Any]) -> List[int]:  # Helper that walks an evolution tree
        """Recursively collects all species IDs inside an evolution chain tree."""
        ids = [self.extract_id_from_url(chain_node["species"]["url"])]  # Gets the species_id at the current node (e.g., Pichu #172)
        for child in chain_node.get("evolves_to", []):  # Loops over every next evolution branch (handles Oddish's split branch too)
            ids.extend(self._collect_chain_species_ids(child))  # Recursively collects species IDs from child branches
        return ids  # Returns all species IDs in the chain

    def run_extraction(self) -> List[int]:  # Main entry point that orchestrates the 3 threaded extraction waves
        """
        Executes Stage 1 (Discover) and Stage 2 (Threaded Extract) across all endpoints.
        Returns the list of target pokemon_ids.
        """
        pokemon_ids = self.discover_pokemon_ids()  # Stage 1: Gets the list of 50 target Pokémon IDs
        types_to_fetch: List[int] = []  # Will collect unique type IDs to fetch in Wave 2
        abilities_to_fetch: List[int] = []  # Will collect unique ability IDs to fetch in Wave 2
        chains_to_fetch: List[int] = []  # Will collect unique evolution chain IDs to fetch in Wave 2

        logger.info(  # Logs that the thread pool is starting
            f"Starting ThreadPoolExecutor with {self.config.worker_count} workers..."
        )
        with ThreadPoolExecutor(  # Creates the pool of worker threads (automatically joins/closes threads on exit)
            max_workers=self.config.worker_count, thread_name_prefix="ExtractorWorker"
        ) as executor:
            # 1. Wave 1: Fetch Pokémon, Encounters, and primary Species concurrently
            futures = {  # Submits 50 tasks (1 per Pokémon) to the thread pool
                executor.submit(self._extract_single_pokemon, pid): pid
                for pid in pokemon_ids
            }
            for future in as_completed(futures):  # Yields each future as soon as a worker thread finishes it
                new_t, new_a, new_c = future.result()  # Unpacks the newly discovered type, ability, and chain IDs
                types_to_fetch.extend(new_t)  # Adds newly discovered type IDs to Wave 2 list
                abilities_to_fetch.extend(new_a)  # Adds newly discovered ability IDs to Wave 2 list
                if new_c is not None:  # If a new evolution chain ID was claimed
                    chains_to_fetch.append(new_c)  # Adds it to the Wave 2 chain list

            # 2. Wave 2: Fetch discovered Types, Abilities, and Evolution Chains concurrently
            secondary_futures = []  # Holds futures for /type/{id} and /ability/{id} tasks
            for tid in types_to_fetch:  # Schedules one task per unique type_id
                url = f"{self.config.api_base_url}/type/{tid}"  # URL for /type/{id}
                cache = self.config.cache_dir / "types" / f"{tid}.json"  # Cache file path cache/types/{id}.json
                secondary_futures.append(executor.submit(self.fetch_json, url, cache))  # Submits task to thread pool

            for aid in abilities_to_fetch:  # Schedules one task per unique ability_id
                url = f"{self.config.api_base_url}/ability/{aid}"  # URL for /ability/{id}
                cache = self.config.cache_dir / "abilities" / f"{aid}.json"  # Cache file path cache/abilities/{id}.json
                secondary_futures.append(executor.submit(self.fetch_json, url, cache))  # Submits task to thread pool

            chain_futures = {}  # Holds futures for /evolution-chain/{id} tasks
            for cid in chains_to_fetch:  # Schedules one task per unique chain_id
                url = f"{self.config.api_base_url}/evolution-chain/{cid}"  # URL for /evolution-chain/{id}
                cache = self.config.cache_dir / "evolution_chains" / f"{cid}.json"  # Cache file path cache/evolution_chains/{id}.json
                chain_futures[executor.submit(self.fetch_json, url, cache)] = cid  # Submits task to thread pool

            for f in as_completed(secondary_futures):  # Waits for all Type and Ability downloads to finish
                f.result()  # Propagates any unexpected exception if one occurred

            # 3. Wave 3: Ensure any out-of-range species in evolution chains (e.g. Pichu #172, Bellossom #182)
            # are also fetched into cache/species/ so foreign keys on pokemon_species never break!
            extra_species_to_fetch: List[int] = []  # Holds species IDs found inside evolution chains that are outside #1-#50
            for f in as_completed(chain_futures):  # Processes each finished evolution chain JSON
                chain_data = f.result()  # Gets the parsed evolution chain JSON dict
                if chain_data and "chain" in chain_data:  # Verifies the chain root exists
                    for sid in self._collect_chain_species_ids(chain_data["chain"]):  # Walks every species in the evolution tree
                        with self._state_lock:  # Acquires lock before checking seen_species_ids
                            if sid not in self.seen_species_ids:  # If this species wasn't in our initial 50 (like Pichu #172)
                                self.seen_species_ids.add(sid)  # Claims this out-of-range species_id
                                extra_species_to_fetch.append(sid)  # Schedules it to be fetched in Wave 3

            extra_species_futures = [  # Submits tasks to fetch those extra evolution species concurrently
                executor.submit(
                    self.fetch_json,
                    f"{self.config.api_base_url}/pokemon-species/{sid}",
                    self.config.cache_dir / "species" / f"{sid}.json",
                )
                for sid in extra_species_to_fetch
            ]
            for f in as_completed(extra_species_futures):  # Waits for all extra species downloads to finish
                f.result()

        logger.info(  # Prints the final summary of everything cached in Stage 2
            f"Extraction complete! Cached {len(pokemon_ids)} Pokémon, "
            f"{len(self.seen_species_ids)} species, {len(self.seen_type_ids)} types, "
            f"{len(self.seen_ability_ids)} abilities, {len(self.seen_chain_ids)} evolution chains. "
            f"Errors: {len(self.errors)}"
        )
        return pokemon_ids  # Returns the 50 target Pokémon IDs for Stage 3 (Transform)


if __name__ == "__main__":  # Allows running python -m pipeline.extractor standalone for testing
    cfg = load_config()  # Loads settings from .env and CLI flags
    extractor = ThreadedPokeExtractor(cfg)  # Creates the extractor instance
    extractor.run_extraction()  # Runs Stage 1 and Stage 2
