# Database, Pinecone, and API configurations
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv(Path(__file__).resolve().parent / ".env")


class Settings(BaseSettings):
    PINECONE_API_KEY: str = ""
    PINECONE_INDEX_NAME: str = ""