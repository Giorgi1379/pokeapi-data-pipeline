import argparse  # Built-in library for reading command-line flags like --limit and --workers
import os  # Built-in library for reading environment variables like os.getenv("DB_URL")
from dataclasses import dataclass  # Decorator that automatically creates __init__ for our config class
from pathlib import Path  # Object-oriented file path library (cleaner than string paths)
from dotenv import load_dotenv  # Loads key-value pairs from your local .env file into os.environ

load_dotenv()  # Reads your .env file right now so os.getenv() can see your settings

BASE_DIR = Path(__file__).resolve().parent.parent  # Finds the root project folder (pokeapi-data-pipeline/)

@dataclass(frozen=True)  # frozen=True makes the config read-only (thread-safe; no thread can accidentally modify it)
class PipelineConfig:  # Data container that holds all pipeline settings in one place
    pokemon_limit: int  # How many Pokémon to extract (50 by default, or 151 for the Stretch Goal)
    pokemon_offset: int  # Which Pokémon index to start from (0 means start at #1 Bulbasaur)
    worker_count: int  # How many threads to run in ThreadPoolExecutor (e.g., 10 or 20)
    db_url: str  # PostgreSQL connection string (user, password, host, port, and database name)
    gcs_bucket: str  # Name of your Google Cloud Storage bucket (e.g., "pokemon-giorgi")
    cache_dir: Path  # Folder path where raw JSON and sprite cache files are saved locally (cache/)
    schema_path: Path  # File path to your 3NF SQL DDL script (sql/schema.sql)
    api_base_url: str = "https://pokeapi.co/api/v2"  # Base URL for all PokéAPI REST endpoints
    max_retries: int = 3  # Maximum number of attempts if an HTTP or GCS request fails
    backoff_factor: float = 1.0  # Base wait time (in seconds) for exponential backoff between retries (1s, 2s, 4s)

def load_config() -> PipelineConfig:  # Function that builds and returns the PipelineConfig object
    parser = argparse.ArgumentParser(description="PokéAPI Threaded Data Pipeline")  # Creates the CLI argument parser
    parser.add_argument(  # Defines the optional --limit CLI flag
        "--limit",  # Flag name you can pass in terminal (e.g., python -m pipeline.main --limit 151)
        type=int,  # Converts the command-line value into a Python integer
        default=int(os.getenv("POKEMON_LIMIT", "50")),  # Falls back to POKEMON_LIMIT in .env, or 50 if not set
        help="Number of Pokémon to extract (default: 50, stretch goal: 151)",  # Description shown in --help
    )
    parser.add_argument(  # Defines the optional --offset CLI flag
        "--offset",  # Flag name you can pass in terminal (e.g., --offset 0)
        type=int,  # Converts the command-line value into a Python integer
        default=int(os.getenv("POKEMON_OFFSET", "0")),  # Falls back to POKEMON_OFFSET in .env, or 0 if not set
        help="Starting offset for Pokémon extraction (default: 0)",  # Description shown in --help
    )
    parser.add_argument(  # Defines the optional --workers CLI flag
        "--workers",  # Flag name you can pass in terminal (e.g., --workers 20)
        type=int,  # Converts the command-line value into a Python integer
        default=int(os.getenv("WORKER_COUNT", "10")),  # Falls back to WORKER_COUNT in .env, or 10 if not set
        help="Number of worker threads in ThreadPoolExecutor (default: 10)",  # Description shown in --help
    )
    args, _ = parser.parse_known_args()  # Parses the flags above and ignores any extra flags passed by test runners

    cache_dir = BASE_DIR / "cache"  # Points to pokeapi-data-pipeline/cache
    cache_dir.mkdir(parents=True, exist_ok=True)  # Creates the cache/ folder if it doesn't already exist

    return PipelineConfig(  # Instantiates and returns the immutable PipelineConfig object
        pokemon_limit=args.limit,  # Sets pokemon_limit from CLI flag or .env
        pokemon_offset=args.offset,  # Sets pokemon_offset from CLI flag or .env
        worker_count=args.workers,  # Sets worker_count from CLI flag or .env
        db_url=os.getenv(  # Reads DB_URL from .env, or uses the local PostgreSQL default below
            "DB_URL", "postgresql://pokeuser:pokepassword@localhost:5432/pokedex"
        ),
        gcs_bucket=os.getenv("GCS_BUCKET", "pokemon-giorgi"),  # Reads GCS_BUCKET from .env, or defaults to "pokemon-giorgi"
        cache_dir=cache_dir,  # Passes the local cache/ directory Path object
        schema_path=BASE_DIR / "sql" / "schema.sql",  # Points to pokeapi-data-pipeline/sql/schema.sql
    )
