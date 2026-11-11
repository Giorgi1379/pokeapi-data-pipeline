import json  # Built-in library for parsing cached JSON files from disk
import logging  # Built-in library for logging how many rows were generated per table
from pathlib import Path  # Object-oriented filesystem path library
from typing import Any, Dict, List  # Type hints for dictionaries, lists, and DataFrame mappings
import pandas as pd  # Data manipulation library required by project spec to flatten and normalize nested JSON

from pipeline.config import PipelineConfig  # Imports our frozen configuration dataclass

logger = logging.getLogger(__name__)  # Creates a module-level logger for transformer.py


class PokeTransformer:  # Main class handling Stage 3 (Pandas Flattening & 3NF Normalization)
    """
    Stage 3 (Transform): Reads cached raw JSON files and uses pandas
    to flatten and normalize them into one DataFrame per 3NF database table.
    """

    def __init__(self, config: PipelineConfig) -> None:  # Constructor receiving our PipelineConfig object
        self.config = config  # Stores the configuration (specifically self.config.cache_dir)

    @staticmethod  # Helper method that does not require 'self'
    def _extract_id(url: str) -> int:  # Extracts the integer ID from the end of a PokéAPI URL (e.g., ".../species/25/" -> 25)
        return int(url.rstrip("/").split("/")[-1])  # Removes trailing slash, splits on '/', grabs the last element, and converts to int

    @staticmethod  # Helper method that does not require 'self'
    def _read_json(path: Path) -> Any:  # Reads and parses a single cached JSON file from disk
        with open(path, "r", encoding="utf-8") as f:  # Opens the JSON file in UTF-8 read mode
            return json.load(f)  # Parses the JSON text into a Python dictionary or list

    def _parse_evolution_tree(  # Recursive helper that walks PokéAPI's nested chain.evolves_to tree
        self, chain_id: int, node: Dict[str, Any], rows: List[Dict[str, Any]]
    ) -> None:
        """Recursively flattens an evolution chain tree (supports branches like Oddish)."""
        from_species_id = self._extract_id(node["species"]["url"])  # Gets the parent species ID at the current tree node (e.g., Gloom = 44)
        for child in node.get("evolves_to", []):  # Loops over every child branch (1 for linear, 2+ for branching like Vileplume & Bellossom)
            to_species_id = self._extract_id(child["species"]["url"])  # Gets the evolved species ID (e.g., Vileplume = 45 or Bellossom = 182)
            details = child.get("evolution_details", [])  # Grabs the list of evolution trigger conditions for this step
            if details:  # If PokéAPI provides evolution details (level, stone, trade, etc.)
                det = details[0]  # Takes the primary evolution condition object
                trigger_name = (  # Extracts the trigger name (e.g., "level-up", "use-item", "trade")
                    det.get("trigger", {}).get("name")
                    if det.get("trigger")
                    else "unknown"
                )
                min_level = det.get("min_level")  # Extracts minimum level (e.g., 16 or 21), or None for stone/trade evolutions
                item_name = (  # Extracts evolution item name (e.g., "leaf-stone", "sun-stone"), or None for level-up evolutions
                    det.get("item", {}).get("name") if det.get("item") else None
                )
            else:  # Fallback if evolution_details is empty
                trigger_name = "unknown"  # Default trigger string
                min_level = None  # Null min_level
                item_name = None  # Null item_name

            rows.append(  # Appends one directed evolution edge (from_species_id -> to_species_id) to our flat list
                {
                    "chain_id": chain_id,  # Foreign key to evolution_chains.chain_id
                    "from_species_id": from_species_id,  # Foreign key to pokemon_species.species_id (pre-evolution)
                    "to_species_id": to_species_id,  # Foreign key to pokemon_species.species_id (post-evolution)
                    "trigger_name": trigger_name,  # How it evolves (e.g., "level-up" or "use-item")
                    "min_level": min_level,  # Required level (or None)
                    "item_name": item_name,  # Required item (or None)
                }
            )
            self._parse_evolution_tree(chain_id, child, rows)  # Recursively processes the next stage in the chain (e.g., Stage 2 -> Stage 3)

    def transform_all(self, pokemon_ids: List[int]) -> Dict[str, pd.DataFrame]:  # Main method that builds all 14 normalized DataFrames
        """Flattens all cached JSON responses into normalized pandas DataFrames."""
        raw_pokemon: List[Dict[str, Any]] = []  # Will hold the 50 raw /pokemon/{id} JSON dicts
        raw_encounters: List[Dict[str, Any]] = []  # Will hold all raw /pokemon/{id}/encounters JSON objects

        for pid in pokemon_ids:  # Loops through our target 50 Pokémon IDs
            p_path = self.config.cache_dir / "pokemon" / f"{pid}.json"  # Path to cache/pokemon/{id}.json
            if p_path.exists():  # Checks that the cached Pokémon file exists
                raw_pokemon.append(self._read_json(p_path))  # Loads the Pokémon JSON dict into memory

            e_path = self.config.cache_dir / "encounters" / f"{pid}.json"  # Path to cache/encounters/{id}.json
            if e_path.exists():  # Checks that the cached encounters file exists
                enc_list = self._read_json(e_path)  # Loads the list of location areas where this Pokémon can be caught
                for enc in enc_list:  # Loops over each location encounter entry
                    enc["pokemon_id"] = pid  # Injects pokemon_id into the dict because PokéAPI's /encounters response omits the Pokémon's own ID!
                    raw_encounters.append(enc)  # Appends the tagged encounter object to raw_encounters

        raw_species = [  # Loads all cached /pokemon-species/{id}.json files (includes out-of-range chain species like Pichu #172)
            self._read_json(p)
            for p in sorted((self.config.cache_dir / "species").glob("*.json"))
        ]
        raw_types = [  # Loads all cached /type/{id}.json files
            self._read_json(p)
            for p in sorted((self.config.cache_dir / "types").glob("*.json"))
        ]
        raw_abilities = [  # Loads all cached /ability/{id}.json files
            self._read_json(p)
            for p in sorted((self.config.cache_dir / "abilities").glob("*.json"))
        ]
        raw_chains = [  # Loads all cached /evolution-chain/{id}.json files
            self._read_json(p)
            for p in sorted(
                (self.config.cache_dir / "evolution_chains").glob("*.json")
            )
        ]

        # 1. evolution_chains DataFrame
        chains_df = (  # Builds a 1-column DataFrame of unique evolution chain IDs
            pd.DataFrame([{"chain_id": int(c["id"])} for c in raw_chains])
            .drop_duplicates(subset=["chain_id"])  # Ensures no duplicate chain_id rows
            .sort_values("chain_id")  # Sorts cleanly by chain_id ascending
        )

        # 2. pokemon_species DataFrame (flattened with pd.json_normalize)
        species_norm = pd.json_normalize(raw_species)  # Flattens nested dicts like generation.name, habitat.name, and evolution_chain.url into dot-columns
        species_df = pd.DataFrame(  # Selects and casts exact columns for the pokemon_species table
            {
                "species_id": species_norm["id"].astype(int),  # Primary Key species_id
                "name": species_norm["name"].astype(str),  # Unique species slug name (e.g., "pikachu")
                "generation": species_norm["generation.name"].astype(str),  # Flattened generation name (e.g., "generation-i")
                "habitat": species_norm.get(  # Safely extracts habitat.name (nullable because some species have habitat: null in PokéAPI)
                    "habitat.name", pd.Series([None] * len(species_norm))
                ).where(pd.notnull, None),  # Converts pandas NaN into Python None (SQL NULL)
                "is_legendary": species_norm["is_legendary"].astype(bool),  # Boolean flag for legendary status
                "is_mythical": species_norm["is_mythical"].astype(bool),  # Boolean flag for mythical status
                "chain_id": species_norm["evolution_chain.url"].apply(  # Extracts the integer chain_id from evolution_chain.url
                    lambda u: self._extract_id(u) if pd.notnull(u) else None
                ),
            }
        ).drop_duplicates(subset=["species_id"])  # Deduplicates on primary key species_id

        # 3. pokemon DataFrame
        poke_norm = pd.json_normalize(raw_pokemon)  # Flattens the 50 raw Pokémon dicts (turning species.url into a flat column)
        pokemon_df = pd.DataFrame(  # Builds the core pokemon table DataFrame
            {
                "pokemon_id": poke_norm["id"].astype(int),  # Primary Key pokemon_id (1..50)
                "name": poke_norm["name"].astype(str),  # Unique Pokémon name
                "species_id": poke_norm["species.url"].apply(self._extract_id),  # Foreign Key referencing pokemon_species.species_id
                "height": poke_norm["height"].astype(int),  # Height in decimetres
                "weight": poke_norm["weight"].astype(int),  # Weight in hectograms
            }
        ).drop_duplicates(subset=["pokemon_id"])  # Deduplicates on primary key pokemon_id

        # 4. types & pokemon_types DataFrames (flattened via pd.json_normalize(record_path=...))
        pt_norm = pd.json_normalize(  # Explodes the nested "types" array inside each Pokémon while keeping the parent Pokémon's "id"
            raw_pokemon, record_path=["types"], meta=["id"], record_prefix="type_"
        )
        pt_norm["type_id"] = pt_norm["type_type.url"].apply(self._extract_id)  # Extracts the integer type_id from type.url
        pokemon_types_df = pd.DataFrame(  # Builds the 3NF junction table linking pokemon_id <-> type_id with slot (1 or 2)
            {
                "pokemon_id": pt_norm["id"].astype(int),  # Foreign Key to pokemon.pokemon_id
                "type_id": pt_norm["type_id"].astype(int),  # Foreign Key to types.type_id
                "slot": pt_norm["type_slot"].astype(int),  # Slot number (1 = primary type, 2 = secondary type)
            }
        ).drop_duplicates(subset=["pokemon_id", "slot"])  # Deduplicates on composite primary key (pokemon_id, slot)

        types_from_api = [  # Extracts (type_id, name) from the dedicated /type/{id}.json files
            {"type_id": int(t["id"]), "name": str(t["name"])} for t in raw_types
        ]
        types_from_poke = (  # Also grabs (type_id, name) from the Pokémon JSONs as a fallback safety net
            pt_norm[["type_id", "type_type.name"]]
            .rename(columns={"type_type.name": "name"})
            .to_dict("records")
        )
        types_df = pd.DataFrame(types_from_api + types_from_poke).drop_duplicates(  # Combines and deduplicates to form the 3NF types lookup table
            subset=["type_id"]
        )

        # 5. abilities & pokemon_abilities DataFrames
        pa_norm = pd.json_normalize(  # Explodes the nested "abilities" array inside each Pokémon while keeping parent Pokémon "id"
            raw_pokemon,
            record_path=["abilities"],
            meta=["id"],
            record_prefix="ability_",
        )
        pa_norm["ability_id"] = pa_norm["ability_ability.url"].apply(self._extract_id)  # Extracts integer ability_id from ability.url
        pokemon_abilities_df = pd.DataFrame(  # Builds the 3NF junction table linking pokemon_id <-> ability_id
            {
                "pokemon_id": pa_norm["id"].astype(int),  # Foreign Key to pokemon.pokemon_id
                "ability_id": pa_norm["ability_id"].astype(int),  # Foreign Key to abilities.ability_id
                "slot": pa_norm["ability_slot"].astype(int),  # Ability slot (1, 2, or 3)
                "is_hidden": pa_norm["ability_is_hidden"].astype(bool),  # True if this ability is hidden on this specific Pokémon
            }
        ).drop_duplicates(subset=["pokemon_id", "slot"])  # Deduplicates on composite primary key (pokemon_id, slot)

        ability_rows = []  # Will hold rows for the 3NF abilities lookup table
        for a in raw_abilities:  # Loops through each cached /ability/{id}.json object
            en_effect = None  # Default to None if no English effect description exists
            for entry in a.get("effect_entries", []):  # Loops over localized effect descriptions (German, English, etc.)
                if entry.get("language", {}).get("name") == "en":  # Filters strictly for the English ("en") entry per project spec
                    en_effect = entry.get("short_effect") or entry.get("effect")  # Grabs the concise English effect text
                    break  # Stops looping once English is found
            ability_rows.append(  # Appends the normalized ability lookup row
                {
                    "ability_id": int(a["id"]),  # Primary Key ability_id
                    "name": str(a["name"]),  # Unique ability slug name (e.g., "overgrow")
                    "effect_text": en_effect,  # English effect description stored once in 3NF
                }
            )
        abilities_df = pd.DataFrame(ability_rows).drop_duplicates(subset=["ability_id"])  # Builds and deduplicates the abilities DataFrame

        # 6. stats & pokemon_stats DataFrames
        ps_norm = pd.json_normalize(  # Explodes the nested "stats" array (6 stats per Pokémon) while keeping parent Pokémon "id"
            raw_pokemon, record_path=["stats"], meta=["id"], record_prefix="stat_"
        )
        ps_norm["stat_id"] = ps_norm["stat_stat.url"].apply(self._extract_id)  # Extracts integer stat_id (1=hp, 2=attack, etc.)
        stats_df = (  # Builds the 3NF stats lookup table (only 6 unique rows: hp, attack, defense, special-attack, special-defense, speed)
            pd.DataFrame(
                {
                    "stat_id": ps_norm["stat_id"].astype(int),  # Primary Key stat_id
                    "name": ps_norm["stat_stat.name"].astype(str),  # Unique stat name
                }
            )
            .drop_duplicates(subset=["stat_id"])  # Deduplicates 300 rows down to the 6 unique stats
            .sort_values("stat_id")  # Sorts by stat_id (1 to 6)
        )
        pokemon_stats_df = pd.DataFrame(  # Builds the 3NF junction table storing each Pokémon's base_stat and effort (EV) values
            {
                "pokemon_id": ps_norm["id"].astype(int),  # Foreign Key to pokemon.pokemon_id
                "stat_id": ps_norm["stat_id"].astype(int),  # Foreign Key to stats.stat_id
                "base_stat": ps_norm["stat_base_stat"].astype(int),  # Base stat value (e.g., HP = 45)
                "effort": ps_norm["stat_effort"].astype(int),  # Effort value (EV yield)
            }
        ).drop_duplicates(subset=["pokemon_id", "stat_id"])  # Deduplicates on composite primary key (pokemon_id, stat_id)

        # 7. evolutions DataFrame
        evo_rows: List[Dict[str, Any]] = []  # Will hold flattened evolution edges across all chains
        for c in raw_chains:  # Loops over every cached /evolution-chain/{id}.json object
            if "chain" in c:  # Verifies the root "chain" node exists
                self._parse_evolution_tree(int(c["id"]), c["chain"], evo_rows)  # Recursively flattens the tree into evo_rows
        evolutions_df = (  # Converts evo_rows into the evolutions DataFrame
            pd.DataFrame(evo_rows).drop_duplicates(
                subset=["from_species_id", "to_species_id"]  # Deduplicates on composite primary key (from_species_id, to_species_id)
            )
            if evo_rows  # If at least one evolution exists
            else pd.DataFrame(  # Fallback empty DataFrame with exact column names if 0 evolutions
                columns=[
                    "chain_id",
                    "from_species_id",
                    "to_species_id",
                    "trigger_name",
                    "min_level",
                    "item_name",
                ]
            )
        )

        # 8. location_areas, game_versions, and pokemon_encounters DataFrames
        if raw_encounters:  # If any Pokémon in our slice has wild encounter locations
            enc_norm = pd.json_normalize(  # Explodes nested version_details inside each location encounter while keeping pokemon_id and location_area
                raw_encounters,
                record_path=["version_details"],
                meta=["pokemon_id", ["location_area", "name"], ["location_area", "url"]],
            )
            enc_norm["location_area_id"] = enc_norm["location_area.url"].apply(  # Extracts integer location_area_id from URL
                self._extract_id
            )
            enc_norm["version_id"] = enc_norm["version.url"].apply(self._extract_id)  # Extracts integer version_id from URL

            def _min_lvl(details: List[Dict[str, Any]]) -> int:  # Helper to find the lowest encounter level in a location/version
                return min((d.get("min_level", 1) for d in details), default=1)

            def _max_lvl(details: List[Dict[str, Any]]) -> int:  # Helper to find the highest encounter level in a location/version
                return max((d.get("max_level", 1) for d in details), default=1)

            enc_norm["min_level"] = enc_norm["encounter_details"].apply(_min_lvl)  # Computes min_level across encounter_details slots
            enc_norm["max_level"] = enc_norm["encounter_details"].apply(_max_lvl)  # Computes max_level across encounter_details slots

            location_areas_df = pd.DataFrame(  # Builds the 3NF location_areas lookup table (stores each location name once)
                {
                    "location_area_id": enc_norm["location_area_id"].astype(int),  # Primary Key location_area_id
                    "name": enc_norm["location_area.name"].astype(str),  # Unique location area slug name
                }
            ).drop_duplicates(subset=["location_area_id"])  # Deduplicates on location_area_id

            game_versions_df = pd.DataFrame(  # Builds the 3NF game_versions lookup table (stores each game version name once)
                {
                    "version_id": enc_norm["version_id"].astype(int),  # Primary Key version_id
                    "name": enc_norm["version.name"].astype(str),  # Unique game version name (e.g., "red", "blue", "yellow")
                }
            ).drop_duplicates(subset=["version_id"])  # Deduplicates on version_id

            pokemon_encounters_df = pd.DataFrame(  # Builds the 3NF pokemon_encounters junction table
                {
                    "pokemon_id": enc_norm["pokemon_id"].astype(int),  # Foreign Key to pokemon.pokemon_id
                    "location_area_id": enc_norm["location_area_id"].astype(int),  # Foreign Key to location_areas.location_area_id
                    "version_id": enc_norm["version_id"].astype(int),  # Foreign Key to game_versions.version_id
                    "min_level": enc_norm["min_level"].astype(int),  # Minimum wild level at this location in this game
                    "max_level": enc_norm["max_level"].astype(int),  # Maximum wild level at this location in this game
                    "max_chance": enc_norm["max_chance"].astype(int),  # Encounter probability percentage
                }
            ).drop_duplicates(subset=["pokemon_id", "location_area_id", "version_id"])  # Deduplicates on 3-column composite PK
        else:  # Fallback if raw_encounters is empty
            location_areas_df = pd.DataFrame(columns=["location_area_id", "name"])
            game_versions_df = pd.DataFrame(columns=["version_id", "name"])
            pokemon_encounters_df = pd.DataFrame(
                columns=[
                    "pokemon_id",
                    "location_area_id",
                    "version_id",
                    "min_level",
                    "max_level",
                    "max_chance",
                ]
            )

        # 9. sprites seed DataFrame (PNG at sprites.front_default, GIF at sprites.other.showdown.front_default)
        sprite_rows: List[Dict[str, Any]] = []  # Will hold 2 sprite rows per Pokémon (1 PNG + 1 GIF = 100 rows)
        for p in raw_pokemon:  # Loops through all 50 raw Pokémon JSON objects
            pid = int(p["id"])  # Pokémon integer ID
            pname = str(p["name"])  # Pokémon slug name (used by media.py to name files <id>_<name>.png/.gif)
            sprites_obj = p.get("sprites", {}) or {}  # Safely accesses the top-level "sprites" dict
            png_url = sprites_obj.get("front_default")  # Extracts static PNG URL at sprites.front_default
            gif_url = (  # Extracts animated GIF URL at sprites.other.showdown.front_default per project spec
                (sprites_obj.get("other", {}) or {})
                .get("showdown", {})
                .get("front_default")
            )
            sprite_rows.append(  # Appends the static PNG row
                {
                    "pokemon_id": pid,  # Foreign Key to pokemon.pokemon_id
                    "pokemon_name": pname,  # Passed to media.py for GCS filename construction
                    "sprite_kind": "png",  # Identifies this row as the static PNG sprite
                    "source_url": png_url,  # Upstream URL (or None if missing)
                    "is_missing": png_url is None,  # True if PokéAPI returned null for front_default
                }
            )
            sprite_rows.append(  # Appends the animated GIF row
                {
                    "pokemon_id": pid,  # Foreign Key to pokemon.pokemon_id
                    "pokemon_name": pname,  # Passed to media.py for GCS filename construction
                    "sprite_kind": "gif",  # Identifies this row as the animated GIF sprite
                    "source_url": gif_url,  # Upstream URL (or None if missing)
                    "is_missing": gif_url is None,  # True if PokéAPI returned null for showdown.front_default
                }
            )
        sprites_df = pd.DataFrame(sprite_rows).drop_duplicates(  # Builds and deduplicates the sprites DataFrame
            subset=["pokemon_id", "sprite_kind"]
        )

        tables = {  # Packages all 14 normalized DataFrames into a dictionary keyed by PostgreSQL table name
            "evolution_chains": chains_df,
            "pokemon_species": species_df,
            "pokemon": pokemon_df,
            "types": types_df,
            "abilities": abilities_df,
            "stats": stats_df,
            "location_areas": location_areas_df,
            "game_versions": game_versions_df,
            "pokemon_types": pokemon_types_df,
            "pokemon_abilities": pokemon_abilities_df,
            "pokemon_stats": pokemon_stats_df,
            "evolutions": evolutions_df,
            "pokemon_encounters": pokemon_encounters_df,
            "sprites": sprites_df,
        }
        for name, df in tables.items():  # Logs the row count of every normalized DataFrame
            logger.info(f"Transformed table '{name}': {len(df)} rows")
        return tables  # Returns the dictionary of DataFrames to Stage 4 (PostgresLoader)
