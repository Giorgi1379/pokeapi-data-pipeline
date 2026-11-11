import os  # Built-in library for reading environment variables like DB_URL
import re  # Built-in regular expressions module used to clean punctuation and spaces in Pokémon names
from typing import Any, Dict, Optional  # Type hints required by Google ADK so the LLM knows each tool's parameter and return types
import psycopg2  # Official PostgreSQL driver for Python
from psycopg2.extras import RealDictCursor  # Makes SQL queries return dictionary-like rows (row["name"]) instead of plain tuples
from dotenv import load_dotenv  # Loads environment variables from your local .env file

load_dotenv()  # Reads your .env file right now so os.getenv("DB_URL") finds your database URL


def _get_connection():  # Helper function that opens a connection to your local PostgreSQL database
    db_url = os.getenv(  # Reads DB_URL from .env, or falls back to your local pokedex database
        "DB_URL", "postgresql://pokeuser:pokepassword@localhost:5432/pokedex"
    )
    return psycopg2.connect(dsn=db_url, cursor_factory=RealDictCursor)  # Connects to PostgreSQL and sets cursor rows to dicts


def normalize_pokemon_name(name: str) -> str:  # Helper that cleans any user-typed Pokémon name into PokéAPI's slug format
    """
    Normalizes user input Pokémon name to PokéAPI slug format (case-insensitive).
    Examples:
      "Mr. Mime" -> "mr-mime"
      "Pikachu"  -> "pikachu"
      "Nidoran♀" -> "nidoran-f"
      "Nidoran♂" -> "nidoran-m"
    """
    cleaned = name.strip().lower()  # Removes leading/trailing spaces and converts to lowercase ("Pikachu" -> "pikachu")
    cleaned = cleaned.replace("♀", "-f").replace("♂", "-m")  # Converts Nidoran gender symbols to PokéAPI's "-f" and "-m"
    cleaned = re.sub(r"[.\s_]+", "-", cleaned)  # Replaces dots, spaces, and underscores with hyphens ("mr. mime" -> "mr-mime")
    cleaned = re.sub(r"[^a-z0-9-]", "", cleaned)  # Removes any remaining special characters (like apostrophes in "Farfetch'd")
    cleaned = re.sub(r"-+", "-", cleaned).strip("-")  # Collapses multiple hyphens ("--") into a single "-" and strips edge hyphens
    return cleaned  # Returns the clean PokéAPI slug


def get_pokemon_profile(name: str) -> Dict[str, Any]:  # Tool 1: Synchronous function (plain 'def', never 'async def') for Pokémon profile
    """
    Returns the profile of a Pokémon from the loaded database, including its ID,
    normalized name, height, weight, types (ordered by slot), abilities (ordered by slot,
    with hidden flag and effect text), and base stats.

    Args:
        name: The name of the Pokémon (e.g., "Charizard", "Pikachu", "Bulbasaur").

    Returns:
        A dictionary containing the Pokémon's profile data, or an status message if not found
        in the loaded dataset.
    """
    slug = normalize_pokemon_name(name)  # Normalizes the input name (e.g., "Charizard" -> "charizard")
    with _get_connection() as conn:  # Opens a database connection (automatically closes transaction on exit)
        with conn.cursor() as cur:  # Opens a dictionary cursor
            cur.execute(  # Queries core Pokémon info joined with species info using parameterized SQL (%s)
                """
                SELECT p.pokemon_id, p.name, p.height, p.weight,
                       ps.generation, ps.habitat, ps.is_legendary, ps.is_mythical
                FROM pokemon p
                JOIN pokemon_species ps ON p.species_id = ps.species_id
                WHERE LOWER(p.name) = %s;
                """,
                (slug,),  # Safely binds slug to %s (prevents SQL injection!)
            )
            poke = cur.fetchone()  # Fetches the single matching Pokémon row (or None if not in the top 50)
            if not poke:  # If the Pokémon is not in our loaded database (e.g., Mewtwo #150)
                return {  # Returns available=False so the agent explicitly states it's not in the loaded dataset
                    "available": False,
                    "pokemon": slug,
                    "message": f"'{name}' is not in the loaded database (only the configured Pokémon dataset is loaded).",
                }

            pid = poke["pokemon_id"]  # Extracts the integer pokemon_id (e.g., 6 for Charizard)

            cur.execute(  # Queries this Pokémon's types ordered by slot (slot 1 = primary, slot 2 = secondary)
                """
                SELECT pt.slot, t.name AS type_name
                FROM pokemon_types pt
                JOIN types t ON pt.type_id = t.type_id
                WHERE pt.pokemon_id = %s
                ORDER BY pt.slot;
                """,
                (pid,),  # Safely binds pokemon_id to %s
            )
            types = [dict(r) for r in cur.fetchall()]  # Converts the returned type rows into a list of Python dicts

            cur.execute(  # Queries this Pokémon's abilities ordered by slot, including is_hidden and English effect_text
                """
                SELECT pa.slot, a.name AS ability_name, pa.is_hidden, a.effect_text
                FROM pokemon_abilities pa
                JOIN abilities a ON pa.ability_id = a.ability_id
                WHERE pa.pokemon_id = %s
                ORDER BY pa.slot;
                """,
                (pid,),  # Safely binds pokemon_id to %s
            )
            abilities = [dict(r) for r in cur.fetchall()]  # Converts the ability rows into a list of Python dicts

            cur.execute(  # Queries this Pokémon's 6 base stats (hp, attack, defense, special-attack, special-defense, speed)
                """
                SELECT s.name AS stat_name, ps.base_stat, ps.effort
                FROM pokemon_stats ps
                JOIN stats s ON ps.stat_id = s.stat_id
                WHERE ps.pokemon_id = %s
                ORDER BY s.stat_id;
                """,
                (pid,),  # Safely binds pokemon_id to %s
            )
            stats = [dict(r) for r in cur.fetchall()]  # Converts the 6 stat rows into a list of Python dicts

            return {  # Returns the complete structured profile dictionary to the ADK agent
                "available": True,  # Confirms the Pokémon was found in our database
                "pokemon_id": pid,  # Pokémon ID
                "name": poke["name"],  # Pokémon name
                "height": poke["height"],  # Height in decimetres
                "weight": poke["weight"],  # Weight in hectograms
                "types": [t["type_name"] for t in types],  # Simple list of type names (e.g., ["fire", "flying"])
                "type_slots": types,  # Detailed list with slot numbers
                "abilities": abilities,  # List of abilities with slot, is_hidden, and effect_text
                "base_stats": stats,  # List of the 6 base stats and effort values
            }


def get_pokemon_habitat(name: str) -> Dict[str, Any]:  # Tool 2: Synchronous tool for species habitat and legendary/mythical flags
    """
    Returns the natural habitat, generation, and legendary/mythical flags for a Pokémon
    from the loaded database.

    Args:
        name: The name of the Pokémon (e.g., "Pikachu", "Mewtwo").

    Returns:
        A dictionary with habitat, generation, is_legendary, and is_mythical flags,
        or available=False if the Pokémon is not in the loaded database.
    """
    slug = normalize_pokemon_name(name)  # Normalizes the user-provided Pokémon name
    with _get_connection() as conn:  # Opens a PostgreSQL connection
        with conn.cursor() as cur:  # Opens a dictionary cursor
            cur.execute(  # Joins pokemon and pokemon_species to get habitat, generation, and legendary/mythical flags
                """
                SELECT p.pokemon_id, p.name, ps.habitat, ps.generation,
                       ps.is_legendary, ps.is_mythical
                FROM pokemon p
                JOIN pokemon_species ps ON p.species_id = ps.species_id
                WHERE LOWER(p.name) = %s;
                """,
                (slug,),  # Safely binds slug to %s
            )
            row = cur.fetchone()  # Fetches the single matching row
            if not row:  # If not found in the pokemon table (e.g., Mewtwo #150)
                return {  # Tells the LLM that this Pokémon is outside the loaded 50 dataset
                    "available": False,
                    "pokemon": slug,
                    "message": f"'{name}' is not in the loaded database (outside the loaded 50 Pokémon).",
                }
            return {  # Returns the species habitat and metadata dictionary
                "available": True,
                "pokemon_id": row["pokemon_id"],
                "name": row["name"],
                "habitat": row["habitat"],  # e.g., "forest" for Pikachu
                "generation": row["generation"],  # e.g., "generation-i"
                "is_legendary": row["is_legendary"],  # Boolean flag
                "is_mythical": row["is_mythical"],  # Boolean flag
            }


def get_pokemon_locations(  # Tool 3: Synchronous tool for encounter locations per game version
    name: str, version: Optional[str] = None
) -> Dict[str, Any]:
    """
    Returns where a Pokémon lives and can be caught, including its species habitat
    and catch location areas grouped by game version from the loaded database.

    Args:
        name: The name of the Pokémon (e.g., "Pikachu", "Bulbasaur").
        version: Optional game version filter (e.g., "red", "blue", "yellow").

    Returns:
        A dictionary containing the species habitat and catch locations per game version,
        or available=False if the Pokémon is not in the loaded database.
    """
    slug = normalize_pokemon_name(name)  # Normalizes the Pokémon name
    with _get_connection() as conn:  # Opens a PostgreSQL connection
        with conn.cursor() as cur:  # Opens a dictionary cursor
            cur.execute(  # Checks if the Pokémon exists in our loaded dataset and grabs its habitat
                """
                SELECT p.pokemon_id, p.name, ps.habitat
                FROM pokemon p
                JOIN pokemon_species ps ON p.species_id = ps.species_id
                WHERE LOWER(p.name) = %s;
                """,
                (slug,),  # Safely binds slug to %s
            )
            poke = cur.fetchone()  # Fetches the Pokémon row
            if not poke:  # If not in the loaded 50 Pokémon
                return {  # Returns available=False so the agent never guesses locations from memory
                    "available": False,
                    "pokemon": slug,
                    "message": f"'{name}' is not in the loaded database (outside the loaded 50 Pokémon).",
                }

            pid = poke["pokemon_id"]  # Gets the integer pokemon_id
            if version:  # If the user asked about a specific game version (e.g., "Where can I catch Pikachu in Red?")
                ver_slug = normalize_pokemon_name(version)  # Normalizes the game version name (e.g., "Red" -> "red")
                cur.execute(  # Queries distinct location areas for this Pokémon and specific game version
                    """
                    SELECT DISTINCT gv.name AS game_version, la.name AS location_area
                    FROM pokemon_encounters pe
                    JOIN location_areas la ON pe.location_area_id = la.location_area_id
                    JOIN game_versions gv ON pe.version_id = gv.version_id
                    WHERE pe.pokemon_id = %s AND LOWER(gv.name) = %s
                    ORDER BY gv.name, la.name;
                    """,
                    (pid, ver_slug),  # Safely binds both pokemon_id and game version slug
                )
            else:  # If no specific game version was passed, fetch encounter locations across all game versions
                cur.execute(  # Queries distinct (game_version, location_area) pairs to keep token count compact and fast for Ollama
                    """
                    SELECT DISTINCT gv.name AS game_version, la.name AS location_area
                    FROM pokemon_encounters pe
                    JOIN location_areas la ON pe.location_area_id = la.location_area_id
                    JOIN game_versions gv ON pe.version_id = gv.version_id
                    WHERE pe.pokemon_id = %s
                    ORDER BY gv.name, la.name;
                    """,
                    (pid,),  # Safely binds pokemon_id
                )
            encounters = cur.fetchall()  # Fetches all matching encounter rows

            by_version: Dict[str, list] = {}  # Dictionary grouping location areas under each game version name
            for enc in encounters:  # Loops through each (game_version, location_area) row
                by_version.setdefault(enc["game_version"], []).append(  # Appends location_area to that game version's list
                    enc["location_area"]
                )

            return {  # Returns both species habitat and game-version encounter locations
                "available": True,
                "pokemon_id": pid,
                "name": poke["name"],
                "habitat": poke["habitat"],  # e.g., "forest"
                "locations_by_game": by_version,  # e.g., {"red": ["power-plant-area", "viridian-forest-area"], ...}
            }


def get_evolution_chain(name: str) -> Dict[str, Any]:  # Tool 4: Synchronous tool for linear and branching evolution chains
    """
    Returns the complete evolution chain (including branching evolutions such as Oddish ->
    Gloom -> Vileplume or Bellossom) with evolution trigger, min_level, and item for a Pokémon.

    Args:
        name: The name of the Pokémon (e.g., "Oddish", "Pikachu", "Bulbasaur").

    Returns:
        A dictionary listing all evolution steps in that Pokémon's evolution chain,
        or available=False if the Pokémon is not in the loaded database.
    """
    slug = normalize_pokemon_name(name)  # Normalizes the Pokémon name (e.g., "Oddish" -> "oddish")
    with _get_connection() as conn:  # Opens a PostgreSQL connection
        with conn.cursor() as cur:  # Opens a dictionary cursor
            cur.execute(  # Finds the Pokémon and looks up its evolution chain_id from pokemon_species
                """
                SELECT p.pokemon_id, p.name, ps.chain_id
                FROM pokemon p
                JOIN pokemon_species ps ON p.species_id = ps.species_id
                WHERE LOWER(p.name) = %s;
                """,
                (slug,),  # Safely binds slug to %s
            )
            poke = cur.fetchone()  # Fetches the Pokémon row
            if not poke:  # If the Pokémon is not in our loaded database
                return {  # Returns available=False
                    "available": False,
                    "pokemon": slug,
                    "message": f"'{name}' is not in the loaded database (outside the loaded 50 Pokémon).",
                }

            chain_id = poke["chain_id"]  # Extracts the integer chain_id (e.g., chain_id = 18 for Oddish)
            cur.execute(  # Joins evolutions twice against pokemon_species (as s_from and s_to) to get human-readable species names!
                """
                SELECT s_from.name AS from_species,
                       s_to.name AS evolves_into,
                       e.trigger_name AS trigger,
                       e.min_level,
                       e.item_name AS item
                FROM evolutions e
                JOIN pokemon_species s_from ON e.from_species_id = s_from.species_id
                JOIN pokemon_species s_to ON e.to_species_id = s_to.species_id
                WHERE e.chain_id = %s
                ORDER BY e.from_species_id, e.to_species_id;
                """,
                (chain_id,),  # Safely binds chain_id to %s
            )
            steps = [dict(r) for r in cur.fetchall()]  # Converts all evolution edges in the chain into a list of dicts
            return {  # Returns the full evolution chain (including branches like Gloom -> Vileplume and Gloom -> Bellossom)
                "available": True,
                "pokemon": poke["name"],
                "chain_id": chain_id,
                "evolutions": steps,
            }


def get_sprite_uri(name: str, animated: bool = False) -> Dict[str, Any]:  # Tool 5: Synchronous tool for GCS sprite URIs
    """
    Returns the Google Cloud Storage (GCS) URI for a Pokémon's sprite image.
    Returns the static PNG URI by default, or the animated GIF URI when animated=True.

    Args:
        name: The name of the Pokémon (e.g., "Bulbasaur", "Pikachu").
        animated: False for static PNG (pngs/), True for animated GIF (gifs/).

    Returns:
        A dictionary containing the gcs_uri, content_type, size_bytes, and checksum_md5.
    """
    slug = normalize_pokemon_name(name)  # Normalizes the Pokémon name (e.g., "Bulbasaur" -> "bulbasaur")
    sprite_kind = "gif" if animated else "png"  # Selects 'gif' if animated=True, otherwise 'png'
    with _get_connection() as conn:  # Opens a PostgreSQL connection
        with conn.cursor() as cur:  # Opens a dictionary cursor
            cur.execute(  # Queries the sprites table for this Pokémon and sprite_kind ('png' or 'gif')
                """
                SELECT p.pokemon_id, p.name, s.sprite_kind, s.gcs_uri,
                       s.content_type, s.size_bytes, s.checksum_md5, s.is_missing
                FROM pokemon p
                JOIN sprites s ON p.pokemon_id = s.pokemon_id
                WHERE LOWER(p.name) = %s AND s.sprite_kind = %s;
                """,
                (slug, sprite_kind),  # Safely binds both slug and sprite_kind to %s
            )
            row = cur.fetchone()  # Fetches the matching sprite metadata row
            if not row:  # If the Pokémon is not in our database
                return {  # Returns available=False
                    "available": False,
                    "pokemon": slug,
                    "message": f"Sprite for '{name}' is not in the loaded database.",
                }
            return {  # Returns the GCS URI (e.g., gs://pokemon-giorgi/gifs/1_bulbasaur.gif) and checksum metadata
                "available": True,
                "pokemon_id": row["pokemon_id"],
                "name": row["name"],
                "sprite_kind": row["sprite_kind"],
                "gcs_uri": row["gcs_uri"],
                "content_type": row["content_type"],
                "size_bytes": row["size_bytes"],
                "checksum_md5": row["checksum_md5"],
                "is_missing": row["is_missing"],
            }
