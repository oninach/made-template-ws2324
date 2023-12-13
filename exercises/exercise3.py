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
            Column('date', TEXT),
            Column('CIN', INTEGER),
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
            data = pd.read_csv(self.csv_url, sep = ';',encoding='ISO-8859-15',engine = 'python',skiprows=7, nrows=475,header=None)
            mapping={0:'date', 1:'CIN',2:'name',12:'petrol', 22:'diesel', 32:'gas',42:'electro',52:'hybrid',62:'plugInHybrid',72:'others'}
            df = data[list(mapping.keys())].rename(columns=mapping)
            # Keep rows where 'CIN' is a string with 5 characters
            df = df[df['CIN'].astype(str).str.match(r'^\d{5}$')]
            
            
            columns_to_check = ['petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']

            # Convert specified columns to numeric, handle non-numeric values as NaN, and fill NaN with 0
            df[columns_to_check] = df[columns_to_check].applymap(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)
            # Filter rows where values are greater than 0 after converting to integers
            df = df[(df[columns_to_check].astype(int) > 0).all(axis=1)]
            

            # Use to_sql for bulk insert
            df.to_sql(self.table_name, connection, if_exists='replace', index=False, method='multi')

            print("Data loaded successfully.")
        except Exception as e:
            print(f"Error loading data: {e}")
        finally:
            connection.close()

if __name__ == "__main__":
    loader = CSVLoader('sqlite:///cars.sqlite', 'cars', 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv')
    loader.load_data()
