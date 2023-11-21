import psycopg2
import os
import json


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
        records = self.cur.fetchall()
        # self.conn.commit()
        return records

    def set_timeout(self, timeout):
        self.cur.execute(f"SET statement_timeout = {timeout};")
        self.conn.commit()

    def close(self):
        self.cur.close()
        self.conn.close()


workload_file_dir = "/data1/liangzibo/dsb/format_queries"
path_list = os.listdir(workload_file_dir)
assert len(path_list) == 52

for query_file in path_list:
    query_file_path = os.path.join(workload_file_dir, query_file)
    with open(query_file_path, 'r') as f:
        sqls = f.readlines()
    plans = []
    for sql in sqls:
        db = Database("tpcds_100")
        db.set_timeout(60000)
        try:
            plan = db.execute('EXPLAIN (ANALYZE true,FORMAT json) ' +sql)
            plans.append(plan)
            if len(plans) % 500 == 0:
                # write to json file
                with open('/data1/liangzibo/dsb/tpcds_plans/'+query_file[0:-4]+'.json', 'w') as outfile:
                    json.dump(plans, outfile)
            # print(plan)
        except Exception as e:
            print(e)
            continue
        db.close()
    with open('/data1/liangzibo/dsb/tpcds_plans/'+query_file[0:-4]+'.json', 'w') as outfile:
        json.dump(plans, outfile)

