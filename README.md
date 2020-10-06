# CDR-wb-indicators-package

This code is for generating a set of indicators defined by WorldBank under [worldbank/covid-mobile-data](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).
It specifically developed to run on Hadoop/Hive cluster and the data resisted on Hive table.

For the definition of the indicators, please refer to [indicator definition](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#indicators-computation)



### Table of contents

* [Required Software](#required-software)
* [Installation](#installation)
* [Data preparation](#data-preparation)
* [Configuration](#configuration)
* [Run program](#run-program)
* [Indicator results](#indicator-results)

## Required Software
* Hadoop cluster with Hive installed (Hortonwork Data Platform (HDP) 2.6.5)
* Hive 1.2 or higher
* Python 3 or above 
* Python pip3 (a Python package installer)

## Installation
clone the repository and then
install all requirement packages in requirements.txt using command 
  * pip install -r requirements.txt
  
## Data preparation
The software need to access CDR data in Hive table with predefined structure.
Make sure your table follow the predefined structure.

### Structure of CDR records in Hive table
The table needs to contain the following data items:

```
PDATE          : date only format, ex: 2020-10-29
CALL_DATETIME  : Activity Time (Start Time) in “YYYY-MM-DD HH:mm:ss” format 
IMEI           : International Mobile Equipment Identity (IMEI) of Caller
IMSI           : International Mobile Subscriber Identity (IMSI) of Caller
CELL_ID        : Unique Cell Tower ID (LAC+CellID)
LOGITUDE       : Real Number (decimal degree) in WGS84
LATITUDE       : Real Number (decimal degree) in WGS84
ADMIN1         : name of administrative level 1
ADMIN1_CODE    : code of administrative level 1
ADMIN2         : name of administrative level 2
ADMIN2_CODE    : code of administrative level 2
ADMIN3         : name of administrative level 3
ADMIN3_CODE    : code of administrative level 3
```

## Configuration
In `config_template.json` file, you need to assign the right path, prefix, location and so on. Here is an example of a `config_template.json` file with an explanation 

* **`provider_prefix`**:  any prefix you'd like to name (you may need in case that you want to use this tool to different data.

* **`db_name`** : spefify database name in hive.
* **`cdr_data_table`** : spefify table name that contain CDR data.
* **`host`** : IP Address of hadoop master node (for connecting to Thrift service).
* **`port`** : port that running Thrift service, default is 10000.
* **`user`** : user for connecting to Thrift service, default is hdfs.
* **`output_data_path`** : a path to store result files.

* **`baseline_start_date`** : a start period of baseline (indicator specific parameter).
* **`baseline_end_date`** : an end period of baseline (indicator specific parameter).

```
{
	"provider_prefix":"mno",
	"db_name" : "default",
	"cdr_data_table" : "calls_100k_test",	

	"host": "localhost",
	"port": 10000,
	"user": "rsstudent",

	"output_data_path":"/output_indicators",

	"cdr_data_table_format_comment_": "pdate,call_datetime,imei,imsi,cell_id,latitude,longitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code",

	"baseline_start_date": "2016-05-01",
	"baseline_end_date": "2016-05-02"

}
```

## Run Program

* python3 run_wb_indicators.py -c {config_file}
 
  ```
  python3 run_wb_indicators.py -c config_template.json
  ```

* It also allow to run specific indicators and export some indicators also.
  Open `run_wb_indicators.py` and look for user section. You can comment the code out to not calculate it.
  ```
    ...
    # user section here
    # run command

    ig.calculate_indicator_01_02_03()
    ig.calculate_indicator_06_11()
    ig.calculate_indicator_04()
    ig.calculate_indicator_09()      # need to run after "calculate_indicator_06_11"
    ig.calculate_indicator_10()      # need to run after "calculate_indicator_09"
    ig.calculate_indicator_05()      # need to run after "calculate_indicator_10"
    ig.calculate_indicator_07_08()   # need to run after "calculate_indicator_06_11,calculate_indicator_10"

    # export output
    ig.export_indicator_01_02_03()  
    ig.export_indicator_06_11()  
    ig.export_indicator_04() 
    ig.export_indicator_09()
    ig.export_indicator_10()
    ig.export_indicator_05()
    ig.export_indicator_07_08()   
    ...
  ```

## Indicator Results
* The sample of output files are in [sample_output_indicators](/sample_output_indicators)
* For indicator05, Origin Destination Matrix - trips between two regions:
  * admin1,admin1_code,admin2,admin2_code,admin3,admin3_code = Origin
  * N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code = Destination
  * OD = total OD exclude the first observation OD of each day
  * OD_firstObservation = total count of the first observation OD of each day
  * totalOD= OD + OD_firstObservation 
* For indicator07, Mean and Standard Deviation of distance traveled (by home location):
  * H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code = Home location
  * all distance in KM
* For indicator09, Daily locations based on Home Region with average stay time and SD of stay time:
  * admin1,admin1_code,admin2,admin2_code = daily location at admin 2 level
  * H_admin1,H_admin1_code,H_admin2,H_admin2_code = home location at admin 2 level
  * mean_duration = in minutes
  * stdev_duration = in minutes
* For indicator10, Simple Origin Destination Matrix - trips between consecutive in time regions with duration:
  * admin1,admin1_code,admin2,admin2_code,admin3,admin3_code = Origin
  * N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code = Destination
  * all duration in minutes


## License
Free to use and distribute with acknowledgement.
