"""'
./dsqgen -output_dir /data1/liangzibo/dsb/queries/ -scale 10 -directory /data1/liangzibo/dsb/query_templates_pg/spj_queries  -template query013_spj.tpl -DIALECT postgres
"""
import os


os.chdir("/data1/liangzibo/dsb/code/tools")

query_temp_dir = "/data1/liangzibo/dsb/query_templates_pg"
diff_temps = ["agg_queries", "multi_block_queries", "spj_queries"]
count = 0
for diff_temp in diff_temps:
    temp_dir = os.path.join(query_temp_dir, diff_temp)
    query_temps = os.listdir(temp_dir)
    query_temps.remove("postgres.tpl")
    # print(len(query_temps))
    for query_temp in query_temps:
        # print(query_temp)
        count += 1
        os.system(
            "./dsqgen -output_dir /data1/liangzibo/dsb/queries/ -scale 100 -directory "
            + temp_dir
            + " -template "
            + query_temp
            + " -DIALECT postgres -COUNT 10000"
        )
        # rename the query file "query_0.sql" to "query_(temp).sql"
        os.system(
            "mv /data1/liangzibo/dsb/queries/query_0.sql /data1/liangzibo/dsb/queries/query_"
            + query_temp[5:-4]
            + ".sql"
        )
print(count)
