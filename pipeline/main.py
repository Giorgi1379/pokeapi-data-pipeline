import logging
import time
from pipeline.config import load_config
from pipeline.extractor import ThreadedPokeExtractor
from pipeline.transformer import PokeTransformer
from pipeline.loader import PostgresLoader
from pipeline.media import ThreadedMediaArchiver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
)
logger = logging.getLogger(__name__)


def run_pipeline() -> None:
    total_start = time.perf_counter()
    config = load_config()
    loader = PostgresLoader(config)

    try:
        loader.init_schema()
        run_id = loader.start_pipeline_run(target_count=config.pokemon_limit)
        logger.info(
            f"=== Starting Pipeline Run #{run_id} (limit={config.pokemon_limit}, "
            f"offset={config.pokemon_offset}, workers={config.worker_count}) ==="
        )

        # Stage 1 & 2: Discover & Threaded Extract
        t0 = time.perf_counter()
        extractor = ThreadedPokeExtractor(config)
        pokemon_ids = extractor.run_extraction()
        extract_sec = time.perf_counter() - t0

        # Stage 3: Transform with Pandas
        t0 = time.perf_counter()
        transformer = PokeTransformer(config)
        tables = transformer.transform_all(pokemon_ids)
        transform_sec = time.perf_counter() - t0

        # Stage 4: Load into PostgreSQL
        t0 = time.perf_counter()
        loaded_count = loader.load_all_tables(tables)
        load_sec = time.perf_counter() - t0

        # Stage 5: Media Archiver
        t0 = time.perf_counter()
        media_archiver = ThreadedMediaArchiver(config, loader)
        uploaded_count = media_archiver.run_media_archiver(tables["sprites"])
        media_sec = time.perf_counter() - t0

        all_errors = extractor.errors + media_archiver.errors
        loader.finish_pipeline_run(
            run_id=run_id,
            pokemon_loaded=loaded_count,
            sprites_uploaded=uploaded_count,
            errors=all_errors,
        )
        total_sec = time.perf_counter() - total_start

        logger.info(
            f"=== Pipeline Run #{run_id} Finished in {total_sec:.2f}s! "
            f"(Extract: {extract_sec:.2f}s | Transform: {transform_sec:.2f}s | "
            f"DB Load: {load_sec:.2f}s | Media: {media_sec:.2f}s) ==="
        )
    finally:
        loader.close()


if __name__ == "__main__":
    run_pipeline()

