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
            Column('date', TEXT),
            Column('CIN', TEXT),
            Column('name', TEXT),
            Column('petrol', FLOAT),
            Column('diesel', FLOAT),
            Column('gas', FLOAT),
            Column('electro', FLOAT),
            Column('hybrid', FLOAT),
            Column('plugInHybrid', FLOAT),
            Column('others', FLOAT),
        ])

        try:
            # Fetch data directly from the URL using pandas
            data = pd.read_csv(self.csv_url, sep = ';',encoding='ISO-8859-15',engine = 'python',skiprows=7, nrows=476,header=None)
            data = data.iloc[:, [0, 1, 2, 12, 22, 32, 42, 52, 62, 72]]
            data.rename(columns={0: 'date', 1: 'CIN', 2: 'name', 12: 'petrol', 22: 'diesel', 32: 'gas', 42: 'electro', 52: 'hybrid', 62: 'plugInHybrid', 72: 'others'}, inplace=True)

            # Columns to filter
            columns_to_filter = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

            for column in columns_to_filter:
                filter_condition = (data[column] != "") & (data[column] != "-")
                data = data[filter_condition]

            data = data.astype(
                {'date': str, 'CIN': str, 'name': str, 'petrol': int, 'diesel': int, 'gas': int, 'electro': int, 'hybrid': int, 'plugInHybrid': int, 'others': int})

            data['CIN'] = data['CIN'].astype(str).str.zfill(5)

            # Clip lower bounds to 0
            data[columns_to_filter] = data[columns_to_filter].clip(lower=0)

            data.dropna(inplace=True)
            # Use to_sql for bulk insert
            data.to_sql(self.table_name, connection, if_exists='replace', index=False, method='multi')

            print("Data loaded successfully.")
        except Exception as e:
            print(f"Error loading data: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    loader = CSVLoader('sqlite:///cars.sqlite', 'cars', 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv')
    loader.load_data()
