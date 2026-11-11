import os  # Built-in library for reading environment variables like OLLAMA_MODEL
from dotenv import load_dotenv  # Loads key-value pairs from your local .env file into os.environ
from google.adk.agents import Agent  # Google Agent Development Kit (ADK) core Agent class
from google.adk.models.lite_llm import LiteLlm  # ADK wrapper that connects to local Ollama models via LiteLLM

from pokedex_agent.tools import (  # Imports our 5 synchronous PostgreSQL query tools
    get_pokemon_profile,  # Tool 1: Fetches height, weight, types, abilities, and base stats
    get_pokemon_habitat,  # Tool 2: Fetches species habitat, generation, and legendary/mythical flags
    get_pokemon_locations,  # Tool 3: Fetches catch location areas per game version
    get_evolution_chain,  # Tool 4: Fetches full evolution chain (including branching evolutions)
    get_sprite_uri,  # Tool 5: Fetches the gs:// URI for static PNG or animated GIF sprites
)

load_dotenv()  # Reads your .env file right now so OLLAMA_MODEL and OLLAMA_API_BASE are loaded

MODEL_NAME = os.getenv("OLLAMA_MODEL", "ollama_chat/qwen2.5:7b")  # Reads model from .env (must use "ollama_chat/" prefix for tool calling!)

root_agent = Agent(  # Defines the main ADK agent variable (must be named root_agent for adk web / adk run)
    name="pokedex_agent",  # Unique identifier for this agent inside Google ADK
    model=LiteLlm(model=MODEL_NAME),  # Connects the agent to your local Qwen model running in Ollama
    instruction=(  # System prompt that governs how the LLM behaves and uses tools
        "You are a Pokédex assistant. Answer strictly and only from tool results. "  # Forbids using pre-trained LLM memory
        "When asked where a Pokémon lives, call both get_pokemon_habitat and get_pokemon_locations "  # Ensures "Where does Pikachu live?" returns both habitat and game locations
        "to report both its species habitat and its catch locations per game version. "  # Satisfies Test #1 in the project rubric
        "If a tool returns available=False or nothing, explicitly state that the Pokémon is not in the loaded data. "  # Satisfies Test #5 (Mewtwo out-of-dataset check)
        "Never guess or use your own pre-trained memory."  # Prevents hallucination on unloaded Pokémon
    ),
    tools=[  # Registers the 5 synchronous Python functions so the LLM can call them automatically
        get_pokemon_profile,  # Registered tool for types, abilities, stats, height, weight
        get_pokemon_habitat,  # Registered tool for species habitat and legendary/mythical status
        get_pokemon_locations,  # Registered tool for encounter locations per game version
        get_evolution_chain,  # Registered tool for linear and branching evolution chains
        get_sprite_uri,  # Registered tool for static PNG and animated GIF GCS URIs
    ],
)

