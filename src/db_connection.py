import yaml
import os
import logging
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine
from src.settings import LOG_PATH

load_dotenv()

# setup logging (write on both log file and terminal)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_PATH), 
        logging.StreamHandler(sys.stdout)    
    ]
)

class DBConnector:
    """
    Manages connection to:
    - AIMS and AMOS databases (input data).
    - Target database (DW).
    """
    def __init__(self, config_path):
        logging.info("Initialized DB connector.")

        # open file
        with open(config_path, 'r') as file:
            raw_content = file.read()
        
        # replace ${var} in .yaml with its real content from .env
        expanded_content = os.path.expandvars(raw_content)

        # convert expanded text in a python dict
        self.config = yaml.safe_load(expanded_content)
        
        self.engines = {}
    
    def get_connection(self, db_name):
        """
        Creates SQLAlchemy engines.
        Args:
            db_name (str): 'aims', 'amos', or 'dw'
        """
        logging.info(f"Connecting with {db_name} database...")

        if db_name in self.engines:
            return self.engines[db_name]

        # fetch credentials based on the name
        if db_name in self.config['sources']:
            creds = self.config['sources'][db_name]
        elif db_name in self.config['target']:
            creds = self.config['target'][db_name] 
        else:
            raise ValueError(f"Database with name '{db_name}' not found in config.")

        # create URL
        # format: dialect+driver://username:password@host:port/database
        if creds['type'] == 'postgresql':
            url = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db']}"
        elif creds['type'] == 'oracle':
            url = f"oracle+oracledb://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db']}"
        else:
            raise ValueError(f"Unsupported DB type: {creds['type']}")

        # create the engine (connection pool)
        engine = create_engine(url, echo=False)
        self.engines[db_name] = engine
        
        logging.info(f"Connection with {db_name} database established.")
        return engine 