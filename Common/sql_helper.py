import json
import matplotlib.pyplot as plt
import csv
import os
import pandas
from matplotlib.widgets import TextBox
import time
from Common.helper import format_two_point_time, sql_to_string
from Common.file_helper import export_to_csv
from Common.file_helper import export_to_csv

def check_existing_table_and_drop(cursor,provider_prefix,table_name):
    print('Checking and dropping {provider_prefix}_{table_name} table if existing.'.format(provider_prefix=provider_prefix,table_name=table_name))
    cursor.execute('DROP TABLE IF EXISTS {provider_prefix}_{table_name}'.format(provider_prefix=provider_prefix,table_name=table_name))

def calculate_indicator(cursor,package,provider_prefix,table_name,cdr_data_table,timer):
    print('Calculating {provider_prefix}_{table_name} table'.format(provider_prefix=provider_prefix,table_name=table_name))
    raw_sql = sql_to_string('{package}/{table_name}.sql'.format(package=package,table_name=table_name))
    query = raw_sql.format(provider_prefix=provider_prefix, cdr_data_table=cdr_data_table)
    cursor.execute(query)
    print('Created {provider_prefix}_{table_name} table. Elapsed time: {time} seconds'
            .format(provider_prefix=provider_prefix,table_name=table_name, time=format_two_point_time(timer, time.time())))

def export_indicator(cursor,package,provider_prefix,table_name,output_data_path,output_table_name,timer):
    raw_sql = sql_to_string('{package}/{table_name}_export.sql'.format(package=package,table_name=table_name))
    query = raw_sql.format(provider_prefix=provider_prefix)
    cursor.execute(query)
    file_path = '{output_data_path}/{provider_prefix}_{output_table_name}.csv'.format(provider_prefix=provider_prefix,output_data_path=output_data_path,output_table_name=output_table_name)
    export_to_csv(file_path,cursor)

def execute_multiple(cursor,package,sql_filename,sql_params,timer):
    print('Execute multiple queries...')
    raw_sql = sql_to_string('{package}/{sql_filename}.sql'.format(package=package,sql_filename=sql_filename))
    # query = raw_sql.format(provider_prefix=provider_prefix, cdr_data_table=cdr_data_table)
    # params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
    query = raw_sql.format(**sql_params)
    # cursor.execute(query)
    qList = query.split(";")
    for q in qList:
        if len(q.strip()) > 0:
            print('Execute {q}'.format(q=q))
            cursor.execute(q)

    print('Finised Execute multiple queries. Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))
