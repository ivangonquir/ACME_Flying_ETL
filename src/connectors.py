import yaml
from sqlalchemy import create_engine
import os


class DBConnector:
    """
    Manages connection to:
    - AIMS and AMOS databases (input data).
    - Target database (DW).
    """
    def __init__(self, config_path="config/db_config.yaml"):
        base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        full_path = os.path.join(base_path, config_path)
        
        with open(full_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.engines = {}
    
    def get_connection(self, db_alias):
        """
        Creates SQLAlchemy engines.
        Args:
            db_alias (str): 'aims', 'amos', or 'dw'
        """
        if db_alias in self.engines:
            return self.engines[db_alias]

        # Fetch credentials based on the alias
        if db_alias in self.config['sources']:
            creds = self.config['sources'][db_alias]
        elif db_alias in self.config['target']:
            creds = self.config['target']['dw'] 
        else:
            raise ValueError(f"Database alias '{db_alias}' not found in config.")

        # Create URL
        # Format: dialect+driver://username:password@host:port/database
        if creds['type'] == 'postgresql':
            url = f"postgresql+psycopg2://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db']}"
        elif creds['type'] == 'oracle':
            url = f"oracle+oracledb://{creds['user']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['db']}"
        else:
            raise ValueError(f"Unsupported DB type: {creds['type']}")

        # Create the engine (Connection Pool)
        engine = create_engine(url, echo=False)
        self.engines[db_alias] = engine
        
        return engine 