import pandas as pd

if __name__ == '__main__':
    me = pd.read_parquet('../data/raw_staging/maintenanceevents.parquet')
    print(me.kind.unique())