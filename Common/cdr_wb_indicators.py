from Common.hive_connection import HiveConnection
import time
from Common.helper import format_two_point_time, sql_to_string
from Common.file_helper import export_to_csv
from Common.sql_helper import check_existing_table_and_drop,calculate_indicator,export_indicator,execute_multiple

class WBIndicators:
    def __init__(self, config):
        self.config = config
        self.hc = HiveConnection()

#     def calculate_indicator_01_02_03(self):
#         provider_prefix = self.config.provider_prefix
#         cdr_data_table= self.config.cdr_data_table
#         cursor = self.hc.cursor
#         package = 'wb_indicators'

#         print('########## calculate_indicator_1_2_3 ##########')
#         timer = time.time()
#         check_existing_table_and_drop(cursor,provider_prefix,'indicator01_admin3_hour')
#         check_existing_table_and_drop(cursor,provider_prefix,'indicator01_admin2_hour')
#         check_existing_table_and_drop(cursor,provider_prefix,'indicator03_admin3_date')
#         check_existing_table_and_drop(cursor,provider_prefix,'indicator03_admin2_date')
#         check_existing_table_and_drop(cursor,provider_prefix,'indicator03_admin1_date')
#         check_existing_table_and_drop(cursor,provider_prefix,'indicator03_admin0_date')

#         print('Checked and dropped indicator01_02_03 related tables if existing. ''Elapsed time: {time} seconds'
#               .format(provider_prefix=provider_prefix, time=format_two_point_time(timer, time.time())))

#         timer = time.time()

#         calculate_indicator(cursor,package,provider_prefix,'indicator01_admin3_hour',cdr_data_table, time.time())
#         calculate_indicator(cursor,package,provider_prefix,'indicator01_admin2_hour',cdr_data_table, time.time())
#         calculate_indicator(cursor,package,provider_prefix,'indicator03_admin3_date',cdr_data_table, time.time())
#         calculate_indicator(cursor,package,provider_prefix,'indicator03_admin2_date',cdr_data_table, time.time())
#         calculate_indicator(cursor,package,provider_prefix,'indicator03_admin1_date',cdr_data_table, time.time())
#         calculate_indicator(cursor,package,provider_prefix,'indicator03_admin0_date',cdr_data_table, time.time())

#         print('########## FINISHED calculate_indicator_1_2_3 ##########')


    def calculate_indicator_01_02_03(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_01_02_03 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
        execute_multiple(cursor,package,'indicator01_02_03',sql_params, time.time())

        print('########## FINISHED calculate_indicator_01_02_03 ##########')

    def export_indicator_01_02_03(self):
        provider_prefix = self.config.provider_prefix
      # cdr_data_table = self.config.cdr_data_table
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_1_2_3 ##########')

        timer = time.time()

        export_indicator(cursor,package,provider_prefix,'indicator03_admin0_date',output_data_path,'indicator01_02_admin2_hour', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator01_admin3_hour',output_data_path,'indicator01_02_admin3_hour', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator03_admin0_date',output_data_path,'indicator03_admin0_date', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator03_admin1_date',output_data_path,'indicator03_admin1_date', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator03_admin2_date',output_data_path,'indicator03_admin2_date', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator03_admin3_date',output_data_path,'indicator03_admin3_date', time.time())


      #   raw_sql = sql_to_string('wb_indicators/indicator01_admin2_hour_export.sql')
      #   query = raw_sql.format(provider_prefix=provider_prefix)
      #   cursor.execute(query)
      #   file_path = '{output_data_path}/{provider_prefix}_indicator01_02_admin2_hour.csv'.format(provider_prefix=provider_prefix,output_data_path=output_data_path)
      #   export_to_csv(file_path,cursor)

  
        print('########## FINISHED exprot indicator_1_2_3 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))


    def calculate_indicator_06_11(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_06_11 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
        execute_multiple(cursor,package,'indicator06_11',sql_params, time.time())

        print('########## FINISHED calculate_indicator_06_11 ##########')

    def export_indicator_06_11(self):
        provider_prefix = self.config.provider_prefix
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_06_11 ##########')

        timer = time.time()

        export_indicator(cursor,package,provider_prefix,'indicator06_admin3_week',output_data_path,'indicator06_admin3_week', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator06_admin2_week',output_data_path,'indicator06_admin2_week', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator11_admin3_month',output_data_path,'indicator11_admin3_month', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator11_admin2_month',output_data_path,'indicator11_admin2_month', time.time())
  
        print('########## FINISHED export indicator_06_11 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))

    def calculate_indicator_04(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        baseline_start_date= self.config.baseline_start_date
        baseline_end_date= self.config.baseline_end_date
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_04 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table,'baseline_start_date':baseline_start_date,'baseline_end_date':baseline_end_date}
        execute_multiple(cursor,package,'indicator04',sql_params, time.time())

        print('########## FINISHED calculate_indicator_04 ##########')

    def export_indicator_04(self):
        
        provider_prefix = self.config.provider_prefix
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_04 ##########')

        timer = time.time()

        export_indicator(cursor,package,provider_prefix,'indicator04_admin3_day',output_data_path,'indicator04_admin3_day', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator04_admin2_day',output_data_path,'indicator04_admin2_day', time.time())

  
        print('########## FINISHED export indicator_04 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))

    def calculate_indicator_09(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_09 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
        execute_multiple(cursor,package,'indicator09',sql_params, time.time())

        print('########## FINISHED calculate_indicator_09 ##########')

    def export_indicator_09(self):
        
        provider_prefix = self.config.provider_prefix
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_09 ##########')

        timer = time.time()

        export_indicator(cursor,package,provider_prefix,'indicator09_admin2_day',output_data_path,'indicator09_admin2_day', time.time())

  
        print('########## FINISHED export indicator_09 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))

    def calculate_indicator_10(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_10 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
        execute_multiple(cursor,package,'indicator10',sql_params, time.time())

        print('########## FINISHED calculate_indicator_07_08 ##########')

    def export_indicator_10(self):
        
        provider_prefix = self.config.provider_prefix
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_10 ##########')

        timer = time.time()

        export_indicator(cursor,package,provider_prefix,'indicator10_admin2_day',output_data_path,'indicator10_admin2_day', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator10_admin3_day',output_data_path,'indicator10_admin3_day', time.time())

  
        print('########## FINISHED export indicator_10 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))

    def calculate_indicator_05(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_05 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
        execute_multiple(cursor,package,'indicator05',sql_params, time.time())

        print('########## FINISHED calculate_indicator_05 ##########')

    def export_indicator_05(self):
        
        provider_prefix = self.config.provider_prefix
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_05 ##########')

        timer = time.time()

        export_indicator(cursor,package,provider_prefix,'indicator05_admin2_day',output_data_path,'indicator05_admin2_day', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator05_admin3_day',output_data_path,'indicator05_admin3_day', time.time())

  
        print('########## FINISHED export indicator_05 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))

    def calculate_indicator_07_08(self):
        provider_prefix = self.config.provider_prefix
        cdr_data_table= self.config.cdr_data_table
        cursor = self.hc.cursor
        package = 'wb_indicators'

        print('########## calculate_indicator_07_08 ##########')

        sql_params= {'provider_prefix':provider_prefix,'cdr_data_table':cdr_data_table}
        execute_multiple(cursor,package,'indicator07_08',sql_params, time.time())

        print('########## FINISHED calculate_indicator_07_08 ##########')

    def export_indicator_07_08(self):
        
        provider_prefix = self.config.provider_prefix
        output_data_path = self.config.output_data_path
        package = 'wb_indicators'
        cursor = self.hc.cursor
        

        print('########## export indicator_07_08 ##########')

        timer = time.time()
        export_indicator(cursor,package,provider_prefix,'indicator07_admin2_day',output_data_path,'indicator07_admin2_day', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator07_admin3_day',output_data_path,'indicator07_admin3_day', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator08_admin2_week',output_data_path,'indicator08_admin2_week', time.time())
        export_indicator(cursor,package,provider_prefix,'indicator08_admin3_week',output_data_path,'indicator08_admin3_week', time.time())

  
        print('########## FINISHED export indicator_07_08 ########## Elapsed time: {time} seconds'.format(time=format_two_point_time(timer, time.time())))


