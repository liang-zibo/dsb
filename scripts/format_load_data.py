import os
import psycopg2


tables = ['call_center',
		'catalog_page', 'catalog_returns',
		'catalog_sales',
		'customer', 'customer_address', 'customer_demographics',
		'date_dim', 'household_demographics', 'income_band', 'inventory', 'item', 'promotion', 'reason', 'ship_mode',
		'store', 'store_returns', 'store_sales',
		'time_dim', 'warehouse',
		'web_page', 'web_returns', 'web_sales', 'web_site'
		]

data_path = '/data1/liangzibo/dsb/data' # directory of data files
target_path = '/data1/liangzibo/dsb/format_data' # directory of format_data files

# read data and strip the last '|'
for table in tables:
    print(table)
    file_path = os.path.join(data_path, table + '.dat')
    target_file_path = os.path.join(target_path, table + '.dat')
    with open(file_path, 'r') as fin:
        lines = fin.readlines()
        with open(target_file_path, 'w') as fout:
            for line in lines:
                fout.write(line[:-2] + '\n')

# Connct to the database
class Database:
    def __init__(self, database_name):
        self.database_name = database_name
        self.conn = psycopg2.connect(
            host="localhost",
            database=database_name,
            user="liangzibo",
            password="",
            port=5486,
        )
        self.cur = self.conn.cursor()

    def execute(self, query):
        self.cur.execute(query)
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


db = Database('tpcds_100')
# copy data to database from target_path
for table in tables:
    print(table)
    file_path = os.path.join(target_path, table + '.dat')
    db.execute('copy ' + table + " from '" + file_path + "' with (delimiter '|', null '');")