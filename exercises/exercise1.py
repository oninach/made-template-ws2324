from sqlalchemy import create_engine, Table, Column, INTEGER, TEXT, MetaData, FLOAT
import pandas as pd
import numpy as np

class CSVLoader:
    def __init__(self, database_url, table_name, csv_url):
        self.engine = create_engine(database_url, echo=True)
        self.metadata = MetaData()
        self.table_name = table_name
        self.csv_url = csv_url

    def create_table(self, columns):
        table = Table(self.table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)
        return table

    def load_data(self):
        connection = self.engine.connect()
        table = self.create_table([
            Column('column_1', INTEGER),
            Column('column_2', TEXT),
            Column('column_3', TEXT),
            Column('column_4', TEXT),
            Column('column_5', TEXT),
            Column('column_6', TEXT),
            Column('column_7', FLOAT),
            Column('column_8', FLOAT),
            Column('column_9', INTEGER),
            Column('column_10', FLOAT),
            Column('column_11', TEXT),
            Column('column_12', TEXT),
            Column('geo_punkt', TEXT)
        ])

        try:
            # Fetch data directly from the URL using pandas
            csv_data = pd.read_csv(self.csv_url, delimiter=';')

            # Use to_sql for bulk insert
            csv_data.to_sql(self.table_name, connection, if_exists='replace', index=False, method='multi')

            print("Data loaded successfully.")
        except Exception as e:
            print(f"Error loading data: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    loader = CSVLoader('sqlite:///airports.sqlite', 'airports', 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv')
    loader.load_data()
