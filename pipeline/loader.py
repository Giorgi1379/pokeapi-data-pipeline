import logging  # Built-in library for logging database initialization and load progress
from typing import Any, Dict, List, Optional, Tuple  # Type hints for DataFrames, tuples, and optional values
import pandas as pd  # Used to receive the normalized table DataFrames and convert NaNs to SQL NULLs
import psycopg2  # Official PostgreSQL database driver for Python
from psycopg2 import extras, pool  # extras provides fast batch execute_values(); pool provides ThreadedConnectionPool

from pipeline.config import PipelineConfig  # Imports our frozen configuration dataclass

logger = logging.getLogger(__name__)  # Creates a module-level logger for loader.py


class PostgresLoader:  # Main database class handling schema setup, audit logging, and 3NF table upserts
    """
    Stage 4 (Load): Uses psycopg2.pool.ThreadedConnectionPool and parameterized
    execute_values upserts (INSERT ... ON CONFLICT) in strict foreign-key order.
    """

    def __init__(self, config: PipelineConfig) -> None:  # Constructor that initializes the thread-safe connection pool
        self.config = config  # Stores the pipeline configuration (db_url, schema_path, worker_count)
        self.conn_pool = psycopg2.pool.ThreadedConnectionPool(  # Creates a thread-safe pool of PostgreSQL connections
            minconn=1,  # Keeps at least 1 open database connection ready at all times
            maxconn=max(4, config.worker_count + 2),  # Allows enough concurrent connections for all worker threads + main thread
            dsn=config.db_url,  # Connects using the DB_URL from .env (postgresql://pokeuser:pokepassword@localhost:5432/pokedex)
        )

    def close(self) -> None:  # Cleanly closes all pooled database connections when the pipeline finishes
        if self.conn_pool:  # Checks that the connection pool exists
            self.conn_pool.closeall()  # Closes every connection in the pool to free database resources

    def init_schema(self) -> None:  # Runs sql/schema.sql from Python (required by project spec)
        """Executes sql/schema.sql from Python as required by the project spec."""
        schema_sql = self.config.schema_path.read_text(encoding="utf-8")  # Reads the entire sql/schema.sql file into a string
        conn = self.conn_pool.getconn()  # Borrows a connection from the thread-safe pool
        try:  # Ensures the connection is always returned to the pool even if an error happens
            with conn:  # Starts a database transaction (automatically commits on success, rolls back on exception)
                with conn.cursor() as cur:  # Opens a database cursor to execute SQL commands
                    cur.execute(schema_sql)  # Runs all 15 CREATE TABLE IF NOT EXISTS statements
            logger.info("Database schema verified/initialized from sql/schema.sql")  # Logs that all 15 tables are ready
        finally:
            self.conn_pool.putconn(conn)  # Returns the borrowed connection back to the pool

    def start_pipeline_run(self, target_count: int) -> int:  # Creates a new audit row in pipeline_runs at the start of a run
        conn = self.conn_pool.getconn()  # Borrows a database connection from the pool
        try:
            with conn:  # Opens a transaction that commits automatically when the block exits
                with conn.cursor() as cur:  # Opens a cursor
                    cur.execute(  # Executes a parameterized INSERT to record the start of this run
                        """
                        INSERT INTO pipeline_runs (pokemon_target_count)
                        VALUES (%s)
                        RETURNING run_id;
                        """,
                        (target_count,),  # Safely binds target_count (e.g., 50) to the %s placeholder
                    )
                    run_id = cur.fetchone()[0]  # Fetches the auto-generated run_id (e.g., 1, 2, 3...)
            return int(run_id)  # Returns the run_id so main.py can update this exact row at the end
        finally:
            self.conn_pool.putconn(conn)  # Returns the connection back to the pool

    def finish_pipeline_run(  # Updates the pipeline_runs row when the pipeline finishes
        self,
        run_id: int,  # The ID of the current run created by start_pipeline_run()
        pokemon_loaded: int,  # Number of Pokémon successfully upserted into PostgreSQL
        sprites_uploaded: int,  # Number of sprites processed/uploaded in Stage 5
        errors: List[str],  # Any errors collected during extraction or media archiving
    ) -> None:
        error_log = "\n".join(errors) if errors else None  # Joins errors into a single string, or SQL NULL if 0 errors
        conn = self.conn_pool.getconn()  # Borrows a database connection from the pool
        try:
            with conn:  # Starts a transaction
                with conn.cursor() as cur:  # Opens a cursor
                    cur.execute(  # Parameterized UPDATE setting ended_at and final run metrics
                        """
                        UPDATE pipeline_runs
                        SET ended_at = NOW(),
                            pokemon_loaded_count = %s,
                            sprites_uploaded_count = %s,
                            error_count = %s,
                            error_log = %s
                        WHERE run_id = %s;
                        """,
                        (  # Safely binds the 5 values to the 5 %s placeholders in order
                            pokemon_loaded,
                            sprites_uploaded,
                            len(errors),
                            error_log,
                            run_id,
                        ),
                    )
        finally:
            self.conn_pool.putconn(conn)  # Returns the connection back to the pool

    @staticmethod  # Helper function that doesn't need 'self'
    def _df_to_tuples(df: pd.DataFrame, columns: List[str]) -> List[Tuple[Any, ...]]:  # Converts a pandas DataFrame into a list of Python tuples for psycopg2
        if df.empty:  # If the DataFrame has 0 rows
            return []  # Return an empty list so we skip executing an empty SQL INSERT
        clean_df = df[columns].astype(object).where(pd.notnull(df[columns]), None)  # Selects columns in exact order and converts pandas NaN/NaT to Python None (SQL NULL)
        return [tuple(row) for row in clean_df.to_numpy()]  # Converts each row into a Python tuple ready for execute_values()

    def load_all_tables(self, tables: Dict[str, pd.DataFrame]) -> int:  # Batch-upserts all 14 data tables in strict FK order
        """
        Inserts all normalized DataFrames inside a single transaction in strict
        parent-to-child foreign-key dependency order using ON CONFLICT upserts.
        """
        conn = self.conn_pool.getconn()  # Borrows a connection from the pool
        try:
            with conn:  # Wraps all 14 table loads in ONE atomic transaction (either all succeed, or all roll back!)
                with conn.cursor() as cur:  # Opens a cursor for batch upserts
                    # 1. evolution_chains (Loaded first because pokemon_species and evolutions reference chain_id)
                    rows = self._df_to_tuples(tables["evolution_chains"], ["chain_id"])  # Extracts (chain_id,) tuples
                    if rows:  # Only execute if there are rows to insert
                        extras.execute_values(  # Fast batch insert of all evolution chain IDs in one SQL statement
                            cur,
                            """
                            INSERT INTO evolution_chains (chain_id)
                            VALUES %s
                            ON CONFLICT (chain_id) DO NOTHING;
                            """,  # ON CONFLICT DO NOTHING ensures re-running never fails or duplicates chain_id
                            rows,
                        )

                    # 2. pokemon_species (Loaded before pokemon and evolutions because both reference species_id)
                    rows = self._df_to_tuples(  # Extracts species tuples in exact column order
                        tables["pokemon_species"],
                        [
                            "species_id",
                            "name",
                            "generation",
                            "habitat",
                            "is_legendary",
                            "is_mythical",
                            "chain_id",
                        ],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts all species rows (including Pichu #172, Bellossom #182, etc.)
                            cur,
                            """
                            INSERT INTO pokemon_species (
                                species_id, name, generation, habitat,
                                is_legendary, is_mythical, chain_id
                            ) VALUES %s
                            ON CONFLICT (species_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                generation = EXCLUDED.generation,
                                habitat = EXCLUDED.habitat,
                                is_legendary = EXCLUDED.is_legendary,
                                is_mythical = EXCLUDED.is_mythical,
                                chain_id = EXCLUDED.chain_id;
                            """,  # If species_id already exists, updates columns in-place using EXCLUDED (the incoming row)
                            rows,
                        )

                    # 3. pokemon (Core entity table; references pokemon_species.species_id)
                    rows = self._df_to_tuples(  # Extracts the 50 target Pokémon rows
                        tables["pokemon"],
                        ["pokemon_id", "name", "species_id", "height", "weight"],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts all 50 Pokémon
                            cur,
                            """
                            INSERT INTO pokemon (pokemon_id, name, species_id, height, weight)
                            VALUES %s
                            ON CONFLICT (pokemon_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                species_id = EXCLUDED.species_id,
                                height = EXCLUDED.height,
                                weight = EXCLUDED.weight;
                            """,  # Idempotent upsert keyed on pokemon_id
                            rows,
                        )

                    # 4. types (Independent 3NF lookup table for type names)
                    rows = self._df_to_tuples(tables["types"], ["type_id", "name"])  # Extracts unique (type_id, name) tuples
                    if rows:
                        extras.execute_values(  # Batch upserts unique Pokémon types (grass, poison, fire, etc.)
                            cur,
                            """
                            INSERT INTO types (type_id, name)
                            VALUES %s
                            ON CONFLICT (type_id) DO UPDATE SET name = EXCLUDED.name;
                            """,
                            rows,
                        )

                    # 5. abilities (Independent 3NF lookup table for ability names and English effect text)
                    rows = self._df_to_tuples(  # Extracts unique (ability_id, name, effect_text) tuples
                        tables["abilities"], ["ability_id", "name", "effect_text"]
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts unique abilities
                            cur,
                            """
                            INSERT INTO abilities (ability_id, name, effect_text)
                            VALUES %s
                            ON CONFLICT (ability_id) DO UPDATE SET
                                name = EXCLUDED.name,
                                effect_text = EXCLUDED.effect_text;
                            """,
                            rows,
                        )

                    # 6. stats (Independent 3NF lookup table for the 6 base stat names)
                    rows = self._df_to_tuples(tables["stats"], ["stat_id", "name"])  # Extracts (stat_id, name) tuples
                    if rows:
                        extras.execute_values(  # Batch upserts the 6 stat definitions (hp, attack, defense, etc.)
                            cur,
                            """
                            INSERT INTO stats (stat_id, name)
                            VALUES %s
                            ON CONFLICT (stat_id) DO UPDATE SET name = EXCLUDED.name;
                            """,
                            rows,
                        )

                    # 7. location_areas (Independent 3NF lookup table for encounter location areas)
                    rows = self._df_to_tuples(  # Extracts unique (location_area_id, name) tuples
                        tables["location_areas"], ["location_area_id", "name"]
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts unique location areas (e.g., viridian-forest-area)
                            cur,
                            """
                            INSERT INTO location_areas (location_area_id, name)
                            VALUES %s
                            ON CONFLICT (location_area_id) DO UPDATE SET name = EXCLUDED.name;
                            """,
                            rows,
                        )

                    # 8. game_versions (Independent 3NF lookup table for game versions)
                    rows = self._df_to_tuples(  # Extracts unique (version_id, name) tuples
                        tables["game_versions"], ["version_id", "name"]
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts game versions (red, blue, yellow, etc.)
                            cur,
                            """
                            INSERT INTO game_versions (version_id, name)
                            VALUES %s
                            ON CONFLICT (version_id) DO UPDATE SET name = EXCLUDED.name;
                            """,
                            rows,
                        )

                    # 9. pokemon_types (Many-to-Many junction table linking pokemon <-> types)
                    rows = self._df_to_tuples(  # Extracts (pokemon_id, type_id, slot) tuples
                        tables["pokemon_types"], ["pokemon_id", "type_id", "slot"]
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts Pokémon type assignments (slot 1 = primary, slot 2 = secondary)
                            cur,
                            """
                            INSERT INTO pokemon_types (pokemon_id, type_id, slot)
                            VALUES %s
                            ON CONFLICT (pokemon_id, slot) DO UPDATE SET
                                type_id = EXCLUDED.type_id;
                            """,
                            rows,
                        )

                    # 10. pokemon_abilities (Many-to-Many junction table linking pokemon <-> abilities)
                    rows = self._df_to_tuples(  # Extracts (pokemon_id, ability_id, slot, is_hidden) tuples
                        tables["pokemon_abilities"],
                        ["pokemon_id", "ability_id", "slot", "is_hidden"],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts Pokémon ability links with slot and is_hidden flag
                            cur,
                            """
                            INSERT INTO pokemon_abilities (pokemon_id, ability_id, slot, is_hidden)
                            VALUES %s
                            ON CONFLICT (pokemon_id, slot) DO UPDATE SET
                                ability_id = EXCLUDED.ability_id,
                                is_hidden = EXCLUDED.is_hidden;
                            """,
                            rows,
                        )

                    # 11. pokemon_stats (Many-to-Many junction table linking pokemon <-> stats)
                    rows = self._df_to_tuples(  # Extracts (pokemon_id, stat_id, base_stat, effort) tuples
                        tables["pokemon_stats"],
                        ["pokemon_id", "stat_id", "base_stat", "effort"],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts all 300 stat rows (6 stats x 50 Pokémon)
                            cur,
                            """
                            INSERT INTO pokemon_stats (pokemon_id, stat_id, base_stat, effort)
                            VALUES %s
                            ON CONFLICT (pokemon_id, stat_id) DO UPDATE SET
                                base_stat = EXCLUDED.base_stat,
                                effort = EXCLUDED.effort;
                            """,
                            rows,
                        )

                    # 12. evolutions (Adjacency-list table storing directed evolution edges from_species_id -> to_species_id)
                    rows = self._df_to_tuples(  # Extracts evolution edges with trigger, min_level, and item_name
                        tables["evolutions"],
                        [
                            "chain_id",
                            "from_species_id",
                            "to_species_id",
                            "trigger_name",
                            "min_level",
                            "item_name",
                        ],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts evolution edges (supports branching like Gloom -> Vileplume / Bellossom)
                            cur,
                            """
                            INSERT INTO evolutions (
                                chain_id, from_species_id, to_species_id,
                                trigger_name, min_level, item_name
                            ) VALUES %s
                            ON CONFLICT (from_species_id, to_species_id) DO UPDATE SET
                                chain_id = EXCLUDED.chain_id,
                                trigger_name = EXCLUDED.trigger_name,
                                min_level = EXCLUDED.min_level,
                                item_name = EXCLUDED.item_name;
                            """,
                            rows,
                        )

                    # 13. pokemon_encounters (Links pokemon, location_areas, and game_versions)
                    rows = self._df_to_tuples(  # Extracts encounter tuples
                        tables["pokemon_encounters"],
                        [
                            "pokemon_id",
                            "location_area_id",
                            "version_id",
                            "min_level",
                            "max_level",
                            "max_chance",
                        ],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts all encounter rows keyed on (pokemon_id, location_area_id, version_id)
                            cur,
                            """
                            INSERT INTO pokemon_encounters (
                                pokemon_id, location_area_id, version_id,
                                min_level, max_level, max_chance
                            ) VALUES %s
                            ON CONFLICT (pokemon_id, location_area_id, version_id) DO UPDATE SET
                                min_level = EXCLUDED.min_level,
                                max_level = EXCLUDED.max_level,
                                max_chance = EXCLUDED.max_chance;
                            """,
                            rows,
                        )

                    # 14. sprites (Initial seed rows before Stage 5 downloads bytes and uploads to GCS)
                    rows = self._df_to_tuples(  # Extracts initial (pokemon_id, sprite_kind, source_url, is_missing) tuples
                        tables["sprites"],
                        ["pokemon_id", "sprite_kind", "source_url", "is_missing"],
                    )
                    if rows:
                        extras.execute_values(  # Batch upserts the 100 sprite rows (50 PNG + 50 GIF)
                            cur,
                            """
                            INSERT INTO sprites (pokemon_id, sprite_kind, source_url, is_missing)
                            VALUES %s
                            ON CONFLICT (pokemon_id, sprite_kind) DO UPDATE SET
                                source_url = EXCLUDED.source_url,
                                is_missing = EXCLUDED.is_missing,
                                updated_at = NOW();
                            """,
                            rows,
                        )

            loaded_count = len(tables["pokemon"])  # Counts how many Pokémon were loaded
            logger.info(  # Logs completion of Stage 4
                f"Successfully upserted all 14 data tables ({loaded_count} Pokémon) into PostgreSQL!"
            )
            return loaded_count  # Returns the count (50) for the pipeline_runs audit log
        finally:
            self.conn_pool.putconn(conn)  # Always returns the connection back to the pool

    def update_sprite_metadata(  # Called by Stage 5 (MediaWorker threads) after each sprite is downloaded/uploaded
        self,
        pokemon_id: int,  # ID of the Pokémon whose sprite was processed
        sprite_kind: str,  # Either 'png' or 'gif'
        gcs_uri: Optional[str],  # Full GCS path (e.g., gs://pokemon-giorgi/gifs/1_bulbasaur.gif) or None if missing
        content_type: Optional[str],  # HTTP Content-Type header (image/png or image/gif)
        size_bytes: Optional[int],  # Size of the raw sprite file in bytes
        checksum_md5: Optional[str],  # Hex MD5 hash of the raw sprite bytes
        is_missing: bool,  # True if PokéAPI returned null for the sprite URL, False otherwise
    ) -> None:
        """Thread-safe parameterized update for a single sprite after GCS upload."""
        conn = self.conn_pool.getconn()  # Borrows a dedicated connection from the thread-safe pool for this worker thread
        try:
            with conn:  # Commits this sprite update transaction automatically
                with conn.cursor() as cur:  # Opens a cursor
                    cur.execute(  # Parameterized SQL UPDATE (no string formatting!)
                        """
                        UPDATE sprites
                        SET gcs_uri = %s,
                            content_type = %s,
                            size_bytes = %s,
                            checksum_md5 = %s,
                            is_missing = %s,
                            updated_at = NOW()
                        WHERE pokemon_id = %s AND sprite_kind = %s;
                        """,
                        (  # Safely binds all 7 parameters to the %s placeholders
                            gcs_uri,
                            content_type,
                            size_bytes,
                            checksum_md5,
                            is_missing,
                            pokemon_id,
                            sprite_kind,
                        ),
                    )
        finally:
            self.conn_pool.putconn(conn)  # Returns the connection back to the pool so other MediaWorker threads can reuse it
