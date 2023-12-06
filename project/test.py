import pytest
from pipeline import CSVLoader 

@pytest.fixture
def crimes_loader(tmp_path):
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
    loader = CSVLoader('sqlite:///:memory:', 'crimes', 'https://data.lacity.org/api/views/2nrs-mtv8/rows.csv?accessType=DOWNLOAD', columns_crimes)
    loader.load_data()
    return loader

@pytest.fixture
def arrest_loader(tmp_path):
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
    loader = CSVLoader('sqlite:///:memory:', 'arrest', 'https://data.lacity.org/api/views/amvf-fr72/rows.csv?accessType=DOWNLOAD', columns_arrest)
    loader.load_data()
    return loader
  
  

def test_crimes_sqlite_file_exists(crimes_loader):
    # Check if the SQLite file exists
    sqlite_file_path = "data/crimes.sqlite"
    assert sqlite_file_path.exists()

def test_arrest_sqlite_file_exists(arrest_loader):
    # Check if the SQLite file exists
    sqlite_file_path = "data/arrest.sqlite"
    assert sqlite_file_path.exists()

def test_crimes_data_loading(crimes_loader):

    assert crimes_loader.engine.execute('SELECT COUNT(*) FROM crimes').scalar() == 24
    rows = crimes_loader.fetchall()
    expected_row = (10304468, '01/08/2020 0:00', '01/08/2020 0:00', 2230, 3, 'Southwest', 377,2, 624, 'BATTERY - SIMPLE ASSAULT', '0444 0913', 36, 'F', 'B', 501,'SINGLE FAMILY DWELLING', 400, 'STRONG-ARM (HANDS, FIST, FEET OR BODILY FORCE)','AO', 'Adult Other', 624, None, None, None, None, '1100 W  39TH PL',None, 34.0141, -118.2978)
    assert rows[0] == expected_row

def test_arrest_data_loading(arrest_loader):

    assert arrest_loader.engine.execute('SELECT COUNT(*) FROM arrest').scalar() == 25
    rows = crimes_loader.fetchall()
    expected_row = (231413977, 'RFC', '07/13/2023 12:00:00 AM', 2330, 14, 'Pacific', 1412, 29, 'M', 'H',None, 'I', 25620, 'MISDEMEANOR COMPLAINT FILED', 'BROOKS AV', 'OCEAN FRONT WK',33.9908, -118.4765, 'POINT (-118.4765 33.9908)', None)    
    assert rows[0] == expected_row





# import pytest

# def test_data_loading_success(sqlite_file_path,database_name,len_rows, las_row):
# # Connect to the SQLite database

#     # check if my sqlite databe file is generated
#     assert sqlite_file_path.exists()
    
#     connection = sqlite3.connect('sqlite:///data/crimes.sqlite')
#     cursor = connection.cursor()

#     try:
#         # Fetch data from the database
#         cursor.execute("SELECT * FROM test_table")
#         rows = cursor.fetchall()

#         assert len(rows) == 24        
#         expected_row = (10304468, '01/08/2020 0:00', '01/08/2020 0:00', 2230, 3, 'Southwest', 377,2, 624, 'BATTERY - SIMPLE ASSAULT', '0444 0913', 36, 'F', 'B', 501,'SINGLE FAMILY DWELLING', 400, 'STRONG-ARM (HANDS, FIST, FEET OR BODILY FORCE)','AO', 'Adult Other', 624, None, None, None, None, '1100 W  39TH PL',None, 34.0141, -118.2978)
#         assert rows[0] == expected_row
#     finally:
#         connection.close()
