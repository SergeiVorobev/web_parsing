from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import time
import datetime
import sqlite3


def parsing_web(url, numb):
    start_ts = datetime.datetime.now()
    driver = webdriver.Chrome()
    driver.get(url)
    time.sleep(5)
    search_box = driver.find_element(By.NAME, 'searcher').send_keys(numb + Keys.ENTER)
    time.sleep(5)
    soup = BeautifulSoup(driver.page_source)

    brak = soup.find_all("div", {"class":"ki-market-message-text-title"})
    result ={}
    if len(brak) == 1:
        parsing_ts = datetime.datetime.now()
        result['Nip'] = numb
        result['is_exist'] = False
        result['start_ts'] = start_ts
        result['parsing_ts'] = parsing_ts

    else:
        labels = [l.text for l in soup.findAll('span') if len(l) > 0][1:-5]

        values = []
        for val in [v for v in soup.find_all("div") if
                    v in soup.find_all("div", {"class":"ki-market-case-details-caseData-item"})
                    or v in soup.find_all("div", {"class":"ki-market-case-header-item"})]:

            if len(val.contents) > 1:
                values.append(val.contents[1].text)
        parsing_ts = datetime.datetime.now()

        result = dict(zip(labels, values))
        result['is_exist'] = True
        result['start_ts'] = start_ts
        result['parsing_ts'] = parsing_ts
        driver.close()

    print(f"Web \"{url}\" is parsed........\n")

    return result

def create_tables(data):
    try:
        # Connecting to sqlite
        conn = sqlite3.connect('SQLitePython.db', detect_types=sqlite3.PARSE_DECLTYPES |
                                                            sqlite3.PARSE_COLNAMES)

        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        print("Connected to SQLite.......\n")

        # Doping SOURCE table if already exists.
        cursor.execute("DROP TABLE IF EXISTS SOURCE")

        # Creating Source table
        sql_create_source_table = '''CREATE TABLE SOURCE(
            id integer PRIMARY KEY,
            tin INT,
            name CHAR(50),
            total_amount CHAR(30),
            address CHAR(50),
            document_type CHAR(30),
            number_id CHAR(30),
            sell_for CHAR(30),
            is_exist BOOL,
            start_ts timestamp ,
            parsed_ts timestamp
        );'''
        cursor = conn.cursor()
        cursor.execute(sql_create_source_table)

        # Doping TINS table if already exists.
        cursor.execute("DROP TABLE IF EXISTS TINS")
        # Creating Tins table
        sql_create_tins_table = '''CREATE TABLE TINS(
                id integer PRIMARY KEY,
                tin INT,
                updated_at CHAR(30)
                );'''
        cursor = conn.cursor()
        cursor.execute(sql_create_tins_table)

        print("Tables created successfully........\n")

        # Commit your changes in the database
        conn.commit()

        # insert values into tables
        if data['is_exist'] == False:

            insert_into_source = """INSERT INTO 'SOURCE'
                                         ('id','number_id', 'is_exist', 'start_ts', 'parsed_ts') 
                                         VALUES (?, ?, ?, ?, ?);"""
            data_source = (1, data['Nip'], data['is_exist'], data['start_ts'], data['parsing_ts'])

            cursor.execute(insert_into_source, data_source)
            conn.commit()
            print("Parsed data added into Source table \n")

            insert_into_tins = """INSERT INTO 'TINS'
                                                 ('id', 'tin') 
                                                 VALUES (?, ?);"""
            data_tins = (1, data['Nip'])

            cursor.execute(insert_into_tins, data_tins)
            conn.commit()
        else:
            insert_into_source = """INSERT INTO 'SOURCE'
                                                 ('id', 'tin', 'name', 'total_amount', 'address', 'document_type',
                                                 'number_id', 'sell_for', 'is_exist', 'start_ts', 'parsed_ts') 
                                                 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
            data_source = (1, data['Nip'], data['Dłużnik'], data['Kwota zadłużenia'], data['Adres'],
                           data[' Rodzaj/typ dokumentu stanowiący podstawę dla wierzytelności '], data['Numer'],
                           data['Kwota zadłużenia'], data['is_exist'], data['start_ts'], data['parsing_ts'])

            cursor.execute(insert_into_source, data_source)
            conn.commit()
            print("Parsed data added into Source table \n")

            insert_into_tins = """INSERT INTO 'TINS'('id', 'tin', 'updated_at') VALUES (?, ?, ?);"""
            data_tins = (1, data['Nip'], datetime.datetime.now().strftime("%m/%d/%Y, %H:%M:%S"))

            cursor.execute(insert_into_tins, data_tins)
            conn.commit()
            print("Data added into Tins table \n")

            # Closing the connection
            conn.close()

    except sqlite3.Error as error:
        print("Error while working with SQLite", error)
    finally:
        if conn:
            conn.close()
            print("sqlite connection is closed.....\n")



def get_tins(data):
    try:
        # Connecting to sqlite
        conn = sqlite3.connect('SQLitePython.db', detect_types=sqlite3.PARSE_DECLTYPES |
                                                               sqlite3.PARSE_COLNAMES)

        # Creating a cursor object using the cursor() method
        cursor = conn.cursor()
        print("Connected to SQLite.......\n")

        # Commit your changes in the database
        conn.commit()

        # get values from tables
        if data['is_exist'] == False:
            print("No NIP!\n")

        else:
            sqlite_select_query = """SELECT * FROM TINS ORDER BY updated_at DESC"""
            cursor.execute(sqlite_select_query)
            records = cursor.fetchall()
            print("Total rows are:  ", len(records))
            for row in records:
                print("Id: ", row[0])
                print("Nip: ", row[1])
                print("Updated at: ", row[2])
                print("\n")

            print("Data from Tins table is printed \n")

        # Closing the connection
        conn.close()

    except sqlite3.Error as error:
        print("Error while working with SQLite", error)
    finally:
        if conn:
            conn.close()
            print("sqlite connection is closed")

if __name__ == '__main__':
    data = parsing_web("https://kaczmarski.pl/gielda-wierzytelnosci", 'PL5270103827')
    create_tables(data)
    get_tins(data)

