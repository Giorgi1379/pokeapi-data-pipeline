-- 1. Pipeline Run Audit Table (Logs every execution of the pipeline for observability)
CREATE TABLE IF NOT EXISTS pipeline_runs ( -- Creates the table only if it doesn't already exist (idempotent DDL)
    run_id SERIAL PRIMARY KEY, -- Auto-incrementing unique ID for each pipeline run (Run #1, #2, #3...)
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Timestamp (with timezone) automatically set when the run starts
    ended_at TIMESTAMPTZ, -- Nullable timestamp set when the run finishes (NULL while a run is still in progress)
    pokemon_target_count INTEGER NOT NULL, -- Target number of Pokémon configured for this run (e.g., 50 or 151)
    pokemon_loaded_count INTEGER NOT NULL DEFAULT 0, -- How many Pokémon were successfully upserted into PostgreSQL
    sprites_uploaded_count INTEGER NOT NULL DEFAULT 0, -- How many sprites (PNG + GIF) were processed in Stage 5
    error_count INTEGER NOT NULL DEFAULT 0, -- Total number of HTTP or GCS errors encountered during the run
    error_log TEXT -- Nullable text column storing detailed error messages (NULL when error_count is 0)
);

-- 2. Evolution Chains (Parent table grouping related species into an evolution family)
CREATE TABLE IF NOT EXISTS evolution_chains ( -- Loaded before pokemon_species and evolutions so foreign keys never fail
    chain_id INTEGER PRIMARY KEY -- Natural integer ID from PokéAPI's /evolution-chain/{id} endpoint
);

-- 3. Pokémon Species (Stores species-level data shared across varieties; includes out-of-50 chain relatives like Pichu #172)
CREATE TABLE IF NOT EXISTS pokemon_species ( -- 3NF: Separates species attributes from individual Pokémon form attributes
    species_id INTEGER PRIMARY KEY, -- Natural integer ID from PokéAPI's /pokemon-species/{id} endpoint
    name VARCHAR(100) NOT NULL UNIQUE, -- Unique species slug name (e.g., 'bulbasaur', 'pikachu', 'pichu')
    generation VARCHAR(50) NOT NULL, -- Generation this species debuted in (e.g., 'generation-i', 'generation-ii')
    habitat VARCHAR(50), -- Nullable natural habitat (e.g., 'forest', 'grassland'; NULL when PokéAPI has no habitat)
    is_legendary BOOLEAN NOT NULL DEFAULT FALSE, -- True for Legendary species (e.g., Articuno, Zapdos, Moltres, Mewtwo)
    is_mythical BOOLEAN NOT NULL DEFAULT FALSE, -- True for Mythical species (e.g., Mew)
    chain_id INTEGER REFERENCES evolution_chains(chain_id) ON DELETE SET NULL -- Foreign Key linking this species to its evolution chain
);

-- 4. Pokémon (Core entity table storing the configured slice of Pokémon, e.g., #1 to #50)
CREATE TABLE IF NOT EXISTS pokemon ( -- 3NF: Every column here depends strictly on pokemon_id
    pokemon_id INTEGER PRIMARY KEY, -- Natural integer ID from PokéAPI's /pokemon/{id} endpoint
    name VARCHAR(100) NOT NULL UNIQUE, -- Unique Pokémon slug name (e.g., 'charizard', 'mr-mime')
    species_id INTEGER NOT NULL REFERENCES pokemon_species(species_id) ON DELETE CASCADE, -- Foreign Key to pokemon_species
    height INTEGER NOT NULL, -- Pokémon height in decimetres (from /pokemon/{id})
    weight INTEGER NOT NULL -- Pokémon weight in hectograms (from /pokemon/{id})
);

-- 5. Types Lookup Table (3NF dimension table so each type name like 'fire' or 'poison' is stored only once)
CREATE TABLE IF NOT EXISTS types ( -- Eliminates repeating type strings across hundreds of rows
    type_id INTEGER PRIMARY KEY, -- Natural integer ID from PokéAPI's /type/{id} endpoint
    name VARCHAR(50) NOT NULL UNIQUE -- Unique type name (e.g., 'grass', 'poison', 'fire', 'flying')
);

-- 6. Abilities Lookup Table (3NF dimension table storing each ability's name and English effect description once)
CREATE TABLE IF NOT EXISTS abilities ( -- Eliminates duplicating long English effect paragraphs on every Pokémon
    ability_id INTEGER PRIMARY KEY, -- Natural integer ID from PokéAPI's /ability/{id} endpoint
    name VARCHAR(100) NOT NULL UNIQUE, -- Unique ability slug name (e.g., 'overgrow', 'chlorophyll', 'static')
    effect_text TEXT -- Nullable English ('en') short effect description from /ability/{id}
);

-- 7. Base Stats Lookup Table (3NF dimension table storing the 6 stat names once)
CREATE TABLE IF NOT EXISTS stats ( -- Stores only the 6 stat definitions (1=hp, 2=attack, 3=defense, etc.)
    stat_id INTEGER PRIMARY KEY, -- Natural integer ID from PokéAPI's stat URL (1 through 6)
    name VARCHAR(50) NOT NULL UNIQUE -- Unique stat name ('hp', 'attack', 'defense', 'special-attack', 'special-defense', 'speed')
);

-- 8. Location Areas Lookup Table (3NF dimension table storing each catch location area name once)
CREATE TABLE IF NOT EXISTS location_areas ( -- Prevents repeating long location strings across thousands of encounters
    location_area_id INTEGER PRIMARY KEY, -- Natural integer ID extracted from location_area.url
    name VARCHAR(150) NOT NULL UNIQUE -- Unique location area slug (e.g., 'viridian-forest-area', 'power-plant-area')
);

-- 9. Game Versions Lookup Table (3NF dimension table storing each Pokémon game version name once)
CREATE TABLE IF NOT EXISTS game_versions ( -- Prevents repeating game version names ('red', 'blue', 'yellow') across encounters
    version_id INTEGER PRIMARY KEY, -- Natural integer ID extracted from version.url
    name VARCHAR(50) NOT NULL UNIQUE -- Unique game version slug (e.g., 'red', 'blue', 'yellow', 'firered')
);

-- 10. Pokémon <-> Types Junction Table (Many-to-Many relationship between pokemon and types)
CREATE TABLE IF NOT EXISTS pokemon_types ( -- 3NF: 'slot' lives here because it describes the relationship between a Pokémon and a Type
    pokemon_id INTEGER NOT NULL REFERENCES pokemon(pokemon_id) ON DELETE CASCADE, -- Foreign Key to pokemon
    type_id INTEGER NOT NULL REFERENCES types(type_id) ON DELETE CASCADE, -- Foreign Key to types
    slot INTEGER NOT NULL, -- 1 = Primary Type, 2 = Secondary Type
    PRIMARY KEY (pokemon_id, slot), -- Composite PK: a Pokémon can only have one type in slot 1 and one in slot 2
    UNIQUE (pokemon_id, type_id) -- Prevents assigning the exact same type twice to the same Pokémon
);

-- 11. Pokémon <-> Abilities Junction Table (Many-to-Many relationship between pokemon and abilities)
CREATE TABLE IF NOT EXISTS pokemon_abilities ( -- 3NF: 'slot' and 'is_hidden' live here because an ability can be hidden on one Pokémon and standard on another
    pokemon_id INTEGER NOT NULL REFERENCES pokemon(pokemon_id) ON DELETE CASCADE, -- Foreign Key to pokemon
    ability_id INTEGER NOT NULL REFERENCES abilities(ability_id) ON DELETE CASCADE, -- Foreign Key to abilities
    slot INTEGER NOT NULL, -- Ability slot number (1, 2, or 3)
    is_hidden BOOLEAN NOT NULL DEFAULT FALSE, -- True if this ability is a Hidden Ability on this specific Pokémon
    PRIMARY KEY (pokemon_id, slot), -- Composite PK ensuring one ability per slot per Pokémon
    UNIQUE (pokemon_id, ability_id, slot) -- Secondary uniqueness constraint on the triplet
);

-- 12. Pokémon <-> Stats Junction Table (Many-to-Many relationship storing a Pokémon's 6 base stats)
CREATE TABLE IF NOT EXISTS pokemon_stats ( -- 3NF: base_stat and effort depend on BOTH pokemon_id and stat_id
    pokemon_id INTEGER NOT NULL REFERENCES pokemon(pokemon_id) ON DELETE CASCADE, -- Foreign Key to pokemon
    stat_id INTEGER NOT NULL REFERENCES stats(stat_id) ON DELETE CASCADE, -- Foreign Key to stats
    base_stat INTEGER NOT NULL, -- The Pokémon's base value for this stat (e.g., Pikachu Speed = 90)
    effort INTEGER NOT NULL DEFAULT 0, -- Effort Value (EV) yield granted when defeating this Pokémon
    PRIMARY KEY (pokemon_id, stat_id) -- Composite PK: each Pokémon has exactly one row per stat_id
);

-- 13. Evolution Edges (Adjacency-list table modeling linear and branching evolutions like Oddish -> Gloom -> Vileplume / Bellossom)
CREATE TABLE IF NOT EXISTS evolutions ( -- Each row is a directed edge (from_species_id -> to_species_id)
    chain_id INTEGER NOT NULL REFERENCES evolution_chains(chain_id) ON DELETE CASCADE, -- Foreign Key grouping edges in the same family
    from_species_id INTEGER NOT NULL REFERENCES pokemon_species(species_id) ON DELETE CASCADE, -- Pre-evolution species (e.g., Gloom #44)
    to_species_id INTEGER NOT NULL REFERENCES pokemon_species(species_id) ON DELETE CASCADE, -- Evolved species (e.g., Vileplume #45 or Bellossom #182)
    trigger_name VARCHAR(50) NOT NULL, -- Evolution trigger type (e.g., 'level-up', 'use-item', 'trade')
    min_level INTEGER, -- Nullable: required level for level-up evolutions (NULL for stone/trade evolutions)
    item_name VARCHAR(100), -- Nullable: required stone/item for 'use-item' evolutions (NULL for level-up evolutions)
    PRIMARY KEY (from_species_id, to_species_id) -- Composite PK allowing one species (Gloom) to branch into multiple target species!
);

-- 14. Pokémon Encounters (Many-to-Many-to-Many table linking Pokémon, Location Areas, and Game Versions)
CREATE TABLE IF NOT EXISTS pokemon_encounters ( -- 3NF: min_level, max_level, and max_chance depend on (pokemon_id, location_area_id, version_id)
    pokemon_id INTEGER NOT NULL REFERENCES pokemon(pokemon_id) ON DELETE CASCADE, -- Foreign Key to pokemon
    location_area_id INTEGER NOT NULL REFERENCES location_areas(location_area_id) ON DELETE CASCADE, -- Foreign Key to location_areas
    version_id INTEGER NOT NULL REFERENCES game_versions(version_id) ON DELETE CASCADE, -- Foreign Key to game_versions
    min_level INTEGER NOT NULL, -- Lowest wild encounter level in this location and game version
    max_level INTEGER NOT NULL, -- Highest wild encounter level in this location and game version
    max_chance INTEGER NOT NULL, -- Encounter probability percentage
    PRIMARY KEY (pokemon_id, location_area_id, version_id) -- Composite PK preventing duplicate encounter rows on re-runs
);

-- 15. GCS Archived Sprites (Stores metadata and gs:// URIs for the 1 static PNG and 1 animated GIF per Pokémon)
CREATE TABLE IF NOT EXISTS sprites ( -- Tracks both uploaded sprites and missing (null) upstream sprite URLs
    sprite_id SERIAL PRIMARY KEY, -- Surrogate primary key for the sprite row
    pokemon_id INTEGER NOT NULL REFERENCES pokemon(pokemon_id) ON DELETE CASCADE, -- Foreign Key to pokemon
    sprite_kind VARCHAR(20) NOT NULL CHECK (sprite_kind IN ('png', 'gif')), -- Restricts values strictly to 'png' or 'gif'
    source_url TEXT, -- Nullable upstream PokéAPI URL (NULL if PokéAPI has no sprite for this Pokémon)
    gcs_uri TEXT, -- Nullable Google Cloud Storage URI (e.g., 'gs://pokemon-giorgi/gifs/1_bulbasaur.gif')
    content_type VARCHAR(50), -- Nullable HTTP Content-Type header ('image/png' or 'image/gif')
    size_bytes INTEGER, -- Nullable file size in bytes of the raw image
    checksum_md5 VARCHAR(64), -- Nullable hexadecimal MD5 checksum used to verify file integrity and idempotency
    is_missing BOOLEAN NOT NULL DEFAULT FALSE, -- True if source_url was null in PokéAPI (prevents failing the pipeline)
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), -- Timestamp of the last metadata upsert
    UNIQUE (pokemon_id, sprite_kind) -- Enforces at most 1 PNG row and 1 GIF row per Pokémon (used by ON CONFLICT upserts)
);
