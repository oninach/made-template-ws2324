from sqlalchemy import create_engine, Table, Column, INTEGER, TEXT, MetaData, FLOAT
import pandas as pd

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
            # Fetch data directly from the URL
            csv_data = pd.read_csv(self.csv_url, delimiter=';')
            for _, row in csv_data.iterrows():
                connection.execute(table.insert().values(
                    column_1=row['column_1'],
                    column_2=row['column_2'],
                    column_3=row['column_3'],
                    column_4=row['column_4'],
                    column_5=row['column_5'],
                    column_6=row['column_6'],
                    column_7=row['column_7'],
                    column_8=row['column_8'],
                    column_9=row['column_9'],
                    column_10=row['column_10'],
                    column_11=row['column_11'],
                    column_12=row['column_12'],
                    geo_punkt=row['geo_punkt']
                ))
            print("Data loaded successfully.")
        except Exception as e:
            print(f"Error loading data: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    loader = CSVLoader('sqlite:///airports.sqlite', 'airports', 'https://opendata.rhein-kreis-neuss.de/api/v2/catalog/datasets/rhein-kreis-neuss-flughafen-weltweit/exports/csv')
    loader.load_data()
