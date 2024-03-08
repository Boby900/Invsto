import psycopg2
import unittest
from unittest import mock
import psycopg2.extras
import configparser
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

config = configparser.ConfigParser()
config.read('config.ini')
dbc = mock.MagicMock()
hostname = config['postgresql']['hostname']
database = config['postgresql']['database']
username = config['postgresql']['username']
port_id = config['postgresql']['port_id']
pwd = config['postgresql']['pwd']
conn = None
cur = None








try:
    with psycopg2.connect(
        host =  hostname,
        dbname = database, 
        user = username,
        port = port_id,
        password = pwd 
) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            create_script = """
                    SELECT * from zab;
            """
            insert_script = """
                    INSERT INTO zab (datetime,close,high,low,open,volume,instrument) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """
            update_script = """
                    UPDATE zab 
            """
            delete_script = "DELETE from zab WHERE datetime = '2024-03-06 12:00:00'"
            cur.execute(create_script)
            
            wholeData = cur.fetchall()
            data = ('2024-03-06 12:00:00', 100.0, 110.0, 90.0, 95.0, 10000, 'AAPL')
            # for data in wholeData:
            #     print(data['datetime'], data['low'])
          
except Exception as error:
    print(error)
    
finally:
    if conn is not None:
        conn.close()
        
        
data = pd.read_csv('./data_h.csv', parse_dates=['datetime'], index_col='datetime')


# print(data['2018-12-28'])

# Calculate moving averages
short_window = 50
long_window = 200
data['SM200'] = data['low'].rolling(window=short_window, min_periods=1).mean()
data['SM100'] = data['low'].rolling(window=long_window, min_periods=1).mean()

# # Generate buy and sell signals
# data['Signal'] = np.where(data['SMA50'] > data['SMA200'], 1, 0)
# data['Position'] = data['Signal'].diff()

plt.figure(figsize=(12, 6))
plt.plot(data['low'], label='Low Price')
plt.plot(data['SM200'], label='50-Day SMA')
plt.plot(data['SM100'], label='200-Day SMA')
plt.plot(data.loc[data['high'] == 1].index, data['SM200'][data['high'] == 1], '^', markersize=10, color='g', lw=0, label='Buy Signal')
plt.plot(data.loc[data['high'] == -1].index, data['SM100'][data['high'] == -1], 'v', markersize=10, color='r', lw=0, label='Sell Signal')
plt.title('Simple Moving Average Crossover Strategy')
plt.legend()
plt.show()


def insert_rows(rows, table_name, dbc):
    field_names = rows[0].keys()
    field_names_str = ', '.join(field_names)
    placeholder_str = ','.join('?'*len(field_names))
    insert_sql = f'INSERT INTO {table_name}({field_names_str}) VALUES ({placeholder_str})'
    saved_autocommit = dbc.autocommit
    with dbc.cursor() as cursor:
        try:
            dbc.autocommit = False
            tuples = [ tuple((row[field_name] for field_name in field_names)) for row in rows ]
            cursor.executemany(insert_sql, tuples)
            cursor.commit()
        except Exception as exc:
            cursor.rollback()
            raise exc
        finally:
            dbc.autocommit = saved_autocommit

            
class Test_insert_rows(unittest.TestCase):

    def fix_dbc(self):
        dbc = mock.MagicMock(spec=['cursor'])
        dbc.autocommit = True
        return dbc

    def fix_rows(self):
        rows = [{'id':1, 'name':'John'}, 
                {'id':2, 'name':'Jane'},]
        return rows

    def test_insert_rows_calls_cursor_method(self):
        dbc = self.fix_dbc()
        rows = self.fix_rows()
        insert_rows(rows, 'users', dbc)
        self.assertTrue(dbc.cursor.called)

if __name__ == '__main__':
    unittest.main(argv=['', '-v'])
