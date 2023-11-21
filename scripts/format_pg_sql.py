import os

query_dir = '/data1/liangzibo/dsb/queries'
format_query_dir = '/data1/liangzibo/dsb/format_queries'
query_files = os.listdir(query_dir)

for query_file in query_files:
    query_path = os.path.join(query_dir, query_file)
    format_query_path = os.path.join(format_query_dir, 'format_' + query_file)
    with open(query_path, 'r') as fin:
        lines = fin.readlines()

        # Consolidate multiple lines of SQL into a single line
        query = ''
        for line in lines:
            if line=='\n':
                if query != '':
                    with open(format_query_path, 'a') as fout:
                        fout.write(query + '\n')
                query = ''
                continue
            query += line.strip('\n ') + ' '
        
