from pathlib import Path

# define absolute path of project root
CURRENT_FILE = Path(__file__).resolve()
PROJECT_ROOT = CURRENT_FILE.parent.parent

# path to db settings file
CONFIG_PATH = f"{PROJECT_ROOT}/config/db_config.yaml"

# path to log file
LOG_PATH = f"{PROJECT_ROOT}/logs/etl_execution.log"

# path to csv source files
CSV_PATH = f"{PROJECT_ROOT}/data/input"

# path to raw staged data
RAW_STAGING_DIR = f"{PROJECT_ROOT}/data/raw_staging"

# path to raw cleaned data
CLEAN_STAGING_DIR = f"{PROJECT_ROOT}/data/clean_staging"