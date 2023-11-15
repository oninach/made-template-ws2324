from sqlalchemy import create_engine, Table, Column, INTEGER, TEXT, MetaData, FLOAT

class CSVLoader:
    def __init__(self, database_url, table_name, csv_file):
        self.engine = create_engine(database_url, echo=True)
        self.metadata = MetaData()
        self.table_name = table_name
        self.csv_file = csv_file

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

        with open(self.csv_file, 'r') as file:
            file.readline()
            for line in file:
                row = line.strip().split(';')
                connection.execute(table.insert().values(        
                    column_1=row[0],
                    column_2=row[1],
                    column_3=row[2],
                    column_4=row[3],
                    column_5=row[4],
                    column_6=row[5],
                    column_7=row[6],
                    column_8=row[7],
                    column_9=row[8],
                    column_10=row[9],
                    column_11=row[10],
                    column_12=row[11],
                    geo_punkt=row[12]
                    ))
        connection.close()

if __name__ == "__main__":
    loader = CSVLoader('sqlite:///airports.sqlite', 'airports', 'rhein-kreis-neuss-flughafen-weltweit.csv')
    loader.load_data()
