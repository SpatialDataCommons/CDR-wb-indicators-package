from Common.config_object import Config
from Common.cdr_wb_indicators import WBIndicators
from Common.hive_create_tables import HiveTableCreator
from Common.hive_connection import HiveConnection
from Common.helper import format_two_point_time
import argparse
import time


def main():
    # argument parser
    start = time.time()
    parser = argparse.ArgumentParser(description='Argument indicating the configuration file')

    # add configuration argument
    parser.add_argument("-c", "--config", help="add a configuration file you would like to process the cdr data"
                                               " \n ex. py run_wb_indicators.py -c config.json",
                        action="store")

    # parse config to args.config
    args = parser.parse_args()

    config = Config(args.config)
    HiveConnection(host=config.__dict__["host"], port=config.__dict__["port"], user=config.__dict__["user"])

    table_creator = HiveTableCreator(config)
    table_creator.initialize('hive_init_commands/initial_hive_commands_wb_indicators.json')  # mandatory (init hive)

    # init indicators generators
    ig = WBIndicators(config)

    # user section here
    # run command

    ig.calculate_indicator_01_02_03()
    ig.calculate_indicator_06_11()
    ig.calculate_indicator_04()
    ig.calculate_indicator_09()      # need to run after "calculate_indicator_06_11"
    ig.calculate_indicator_10()      # need to run after "calculate_indicator_09"
    ig.calculate_indicator_05()      # need to run after "calculate_indicator_10"
    ig.calculate_indicator_07_08()     # need to run after "calculate_indicator_06_11,calculate_indicator_10"

    # export output
    ig.export_indicator_01_02_03()  
    ig.export_indicator_06_11()  
    ig.export_indicator_04() 
    ig.export_indicator_09()
    ig.export_indicator_10()
    ig.export_indicator_05()
    ig.export_indicator_07_08()   


    print('Overall time elapsed: {} seconds'.format(format_two_point_time(start, time.time())))
if __name__ == '__main__':
    main()
