import findspark
findspark.init('/mnt/nfs/spark')
findspark.find()
 
import pyspark
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, DecimalType, StringType, IntegerType, BooleanType
from pyspark.sql import Window, HiveContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan,when,count
from decimal import Decimal
import requests, os
import json

conf = pyspark.SparkConf().setAll([
('spark.ui.port', '3778'),
('spark.cores.max', '2'),
('spark.executor.memory', '1g'),
('spark.driver.extraClassPath', 'postgresql-42.2.20.jre7.jar'),
('spark.executor.extraClassPath', 'postgresql-42.2.20.jre7.jar'),
('spark.sql.catalogImplementation', 'hive'),
('spark.sql.parquet.writeLegacyFormat', 'True'),
('spark.sql.sources.partitionOverwriteMode', 'dynamic')
])
 
master = 'mesos://zk://dserver02:2181/mesos'
 
conf.setMaster(master)
sc = pyspark.SparkContext(appName='spark-postgresql', conf=conf)
spark = pyspark.sql.SparkSession.builder.master(master).config(conf=conf).enableHiveSupport().getOrCreate()
 
sc
spark

token = os.environ['my_token']
dependence = 'my_dep'
 
payload = requests.get(f'api with passwords/?token={token}&dependence={dependence}').json()
username, password = payload['username'], payload['password']

host = 'host'
port = 'port'
dbase = 'dbase'
dbschema = 'schema'
hdfs_path = 'hdfs path'

schema_query = f"(select * from information_schema.tables where table_schema = '{dbschema}')"
schema_tables = spark.read\
    .format('jdbc')\
    .option('url', f'jdbc:postgresql://{host}:{port}/{dbase}')\
    .option('dbtable', f'{schema_query}')\
    .option('user', f'{username}')\
    .option('password', f'{password}')\
    .option('driver', 'org.postgresql.Driver')\
    .load()

large_tables = ['']
schema_tables_list = [i.table_name for i in schema_tables.select(col('table_name')).collect() if i.table_name.lower() not in large_tables]

types_dict = {
    ',StringType' : "',StringType()",
    ',BooleanType' : "',BooleanType()",
    ',IntegerType' : "',IntegerType()",
    ',DateType' : "',StringType()",
    ',TimestampType' : "',StringType()",
    'StructField(' : "StructField('",
    ',DecimalType(' : "',DecimalType("
}

data_list = []

for table in schema_tables_list:
    temp_df = spark.read\
    .format('jdbc')\
    .option('url', f'jdbc:postgresql://{host}:{port}/{dbase}')\
    .option('dbtable', f'(select * from {dbschema}.{table})')\
    .option('user', f'{username}')\
    .option('password', f'{password}')\
    .option('driver', 'org.postgresql.Driver')\
    .load()
    data = [json.loads(part) for part in temp_df.toJSON().collect()]
    schema_string = str(temp_df.schema).replace('StructType(List(', 'StructType([')\
    .replace('true', 'True')[:-2] + '])'
    for key in types_dict:
        schema_string = schema_string.replace(key, types_dict[key])
    for key in data[0]:
        if schema_string.find(f"'{key}'"+',DecimalType') != -1:
            for part in data:
                try:
                    part[key] = Decimal(part[key])
                except:
                    part[key] = None
                    pass
    temp_dict = {'table_name' : table, 'struct_type' : schema_string, 'data': data}
    data_list.append(temp_dict)
    
struct_string = ""
for i in data_list:
    tmp_struct = f"StructField('{i['table_name']}',{i['struct_type']})"
    struct_string += f', {tmp_struct}'
struct_string = struct_string[2:].replace('true', 'True')

lens = [len(i['data']) for i in data_list]

for i in data_list:
    empty_dict = dict.fromkeys(i['data'][0])
    while len(i['data']) < max(lens):
        i['data'].append(empty_dict)
        
data_by_row = []
for row in range(max(lens)):
    temp_list = []
    for table in data_list:
        tup = table['data'][row]
        temp_list.append(tup)
    data_by_row.append(tuple(temp_list))
    
exec(f"schema = StructType([{struct_string}])")
df = spark.createDataFrame(data=data_by_row, schema = schema)
df.repartition(numPartitions=1).write.mode('overwrite').parquet(hdfs_path)


so = ''
for dtype in df.dtypes:
    s = f"`{dtype[0]}` {dtype[1]}"
    s = s.replace('<', '<`').replace(':', '`:').replace(',', ',`')
    for i in range(0,30):
        s = s.replace(f'`{str(i)})', f'{str(i)})')
    so += s + ',\n'
so = so[:-2]


#Create table in hive
create_table_query = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS HIVE_SCHEMA.{dbschema}(
{so}
)

STORED AS PARQUET
LOCATION '{hdfs_path}'
"""
spark.sql(create_table_query)
spark.stop()

#Dtypes for confluence
import pandas as pd
some_list = []
for column in df.columns:
    for i in df.select(col(f'{column}.*')).dtypes:
        some_list.append({'table': column, 'column':i[0], 'type':i[1]})
df_types = pd.DataFrame(some_list)
df_types.to_excel('types.xlsx')

#Selects for each table
for column in df.columns:
    where = 'WHERE '
    for where_clause in df.select(col(f'{column}.*')).columns:
        where += f't.{where_clause} IS NOT NULL OR '
    where = where[:-4]
    s = f'--select for {column} \nSELECT t.* from HIVE_SCHEMA.{dbschema} LATERAL VIEW INLINE (ARRAY({column})) t \n{where}\n'
    print(s)
