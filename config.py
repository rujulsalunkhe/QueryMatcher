import os
from dataclasses import dataclass
from typing import Dict, List, Optional

@dataclass
class SystemConfig:
    # Model Configuration
    MODEL_NAME: str = 'all-mpnet-base-v2'
    INDEX_DIM: int = 768
    
    # Thresholds
    INTENT_THRESHOLD: float = 0.0
    DESC_SLOT_THRESHOLD: float = 0.0
    TOP_K_DESC_SLOTS: int = 3
    
    # Files and Database
    DB_FILE: str = 'data.db'
    TABLE_NAME: str = 'main_table'
    TEMPLATE_FILE: str = 'templates.json'
    SCHEMA_FILE: str = 'schema.json'
    
    # Redis Configuration
    REDIS_URL: str = 'redis://localhost:6379/0'
    MISS_LOG_LIST: str = 'match_misses'
    
    # Processing Configuration
    TEXT_COLUMNS_MIN_LENGTH: int = 10  # Minimum length to consider a column as text/description
    MAX_UNIQUE_VALUES_FOR_CATEGORICAL: int = 50  # Max unique values to treat as categorical

config = SystemConfig()