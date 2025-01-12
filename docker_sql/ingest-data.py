
import argparse ##Python 的一个命令行参数解析模块
import pandas as pd
import time
from sqlalchemy import create_engine
import os 

#user
#password
#host
#port
#db
#table_name
#url

def main(params):
    user=params.user
    password=params.password
    host=params.host
    port=params.port
    db=params.db
    table_name=params.table_name
    url=params.url  
    
    # csv_name='output.csv'
    # os.system(f'wget {url} -O {csv_name}')
      # 下载压缩文件
    csv_gz = 'output.csv.gz'
    os.system(f'wget {url} -O {csv_gz}')
    
    # 解压文件
    os.system(f'gunzip -f {csv_gz}')  # -f 参数表示如果文件已存在则强制覆盖
    csv_name = 'output.csv'

    engine=create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    df_iterator=pd.read_csv(csv_name,iterator=True,chunksize=100000)
    try:
        while True:
            start_time = time.time()
            df = next(df_iterator)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            end_time = time.time()
            print('inserted another chunk...', f'{end_time-start_time}')
    ##当数据被读取完时
    except StopIteration:
        print("Finished ingesting data into the postgres database")

if __name__=='__main__':
    parser=argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user',help='user name for postgres')
    parser.add_argument('--password',help='password for postgres')
    parser.add_argument('--host',help='host for postgres')
    parser.add_argument('--port',help='port for postgres')
    parser.add_argument('--db',help='database name for postgres')
    parser.add_argument('--table_name',help='name of the table where we will write the results to')
    parser.add_argument('--url',help='url of the csv file')
    args=parser.parse_args()
    main(args)



