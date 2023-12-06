from sqlalchemy import create_engine, Table, Column, INTEGER, TEXT, MetaData, FLOAT
import pandas as pd
import numpy as np

class CSVLoader:
    def __init__(self, database_url, table_name, csv_url,columns):
        self.engine = create_engine(database_url, echo=True)
        self.metadata = MetaData()
        self.table_name = table_name
        self.csv_url = csv_url
        self.columns=columns

    def create_table(self, columns):
        table = Table(self.table_name, self.metadata, *columns)
        self.metadata.create_all(self.engine)
        return table

    def load_data(self):
        connection = self.engine.connect()
        table = self.create_table(self.columns)

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
  columns_crimes = [
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
]
  loader = CSVLoader('sqlite:///data/crimes.sqlite', 'crimes', 'https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD', columns_crimes)
  loader.load_data()
  
  
  
  columns_arrest = [
    Column('Report_ID', INTEGER),
    Column('Report_Type', TEXT),
    Column('Arrest_Date', TEXT),
    Column('Time', INTEGER),
    Column('Area_ID', INTEGER),
    Column('Area_Name', TEXT),
    Column('Reporting_District', INTEGER),
    Column('Age', INTEGER),
    Column('Sex_Code', TEXT),
    Column('Descent_Code', TEXT),
    Column('Charge_Group_Code', INTEGER),
    Column('Charge_Group_Description', TEXT),
    Column('Arrest_Type_Code', TEXT),
    Column('Charge', TEXT),
    Column('Charge_Description', TEXT),
    Column('Disposition_Description', TEXT),
    Column('Address', TEXT),
    Column('Cross_Street', TEXT),
    Column('LAT', FLOAT),
    Column('LON', FLOAT),
    Column('Location', TEXT),
    Column('Booking_Date', TEXT),
    Column('Booking_Time', TEXT),
    Column('Booking_Location', TEXT),
    Column('Booking_Location_Code', TEXT)
]


  loader = CSVLoader('sqlite:///data/arrest.sqlite', 'arrest', 'https://data.lacity.org/api/views/amvf-fr72/rows.csv?accessType=DOWNLOAD', columns_arrest)
  loader.load_data()