# CDR-wb-indicators-package

This code is for generating a set of indicators defined by WorldBank under [worldbank/covid-mobile-data](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation).
It specifically developed to run on Hadoop/Hive cluster and the data resisted on Hive table.


### Table of contents
* [Indicator Definition](#indicator-definition)
* [Required Software](#required-software)
* [Installation](#installation)
* [Data preparation](#data-preparation)
* [Configuration](#configuration)
* [Run program](#run-program)
* [Indicator results](#indicator-results)

## Indicator Definition
| Indicator | Name | Unit | Description | 
| :---: | --- | --- | --- |
| 1 | Count of all CDR | Hour |Sum across all mobile usage in the given hour and lowest admin area |
| 2 | Count of unique handset | Hour | Sum all unique IMEI with an observation in the given admin area and time period |
| 3 | Count of unique handset | Day | Sum all unique IMEI with an observation in the given admin area and time period |
| 4 | Ratio of residents active that day based on those present during baseline | Day | Focusing just on the baseline period, calculate home location per individual and aggregate total IMEI per home location. On a daily basis, calculate the number of active users (defined as those that have made at least one observation on that day) but only focused on those IMEI that had at least one observation during the baseline period. Take the ratio of active residents to total residents |
| 5 | Origin Destination Matrix - trips between two regions | Day | 1. For each subscriber, list the unique regions that they visited within the time day, ordered by time. Create pairs of regions by pairing the nth region with all other regions that come after it. For example, the sequence [A, B, C, D] would result in the pairings [AB, AC, AD, BC, BD, CD]. For each pair, count the number of times that pair appears. 2. For each subscriber, look at the location of the first observation of the day. Look back and get the location of the last observation before this one (no matter how far back it goes) to form a pair, keep the date assigned to this pair as the date of the first observation of the day 3. Sum all pairs from 1 and from 2 for each day (also keep the sum of 1 and sum of 2 as variables). |
| 6 | Residents living in area | Week | Look at the location of the last observation on each day of the week and take the modal location to be the home location. If there is more than one modal region, the one that is the subscriber's most recent last location of the day is selected. Count number of subscribers assigned to each region for the specific week |
| 7 | Mean and Standard Deviation of distance traveled (by home location) | Day | Calculate distance traveled by each subscriber daily based on location of all calls (compute distance between tower cluster centroids). Group subscribers by their home location. Calculate mean distance traveled and SD for all subscribers from the same home region. |
| 8 | Mean and Standard Deviation of distance traveled (by home location) | Week | Same as Indicator 7 but at week level |
| 9 | Daily locations based on Home Region with average stay time and SD of stay time | Day | 1. For days with at least one observation, assign the district where the most time is spent (so each subscriber present on a day should have one location) 2. Use Weekly assigned home location (#6 above) for each person. 3. Create matrix of home location vs day location: for each home location sum the total number of people that from that home location that are in each district (including the home one) based on where they spent the most time and get mean and SD for time spent in the location (by home location) |
| 10 | Simple Origin Destination Matrix - trips between consecutive in time regions with duration | Day | 1. For each SIM, calculate movement between consecutive observations. 2. Calculate time spent in the origin and time spent in the destination for each trip. 3. For each day, sum all people going from X to Y area and the average time spent in X before going and spent in Y after arriving. |
| 11 | Residents living in area | Month | Same as Indicator 6 but at Month level |

For the detail definition of the indicators, please refer to [indicator definition](https://github.com/worldbank/covid-mobile-data/tree/master/cdr-aggregation#indicators-computation)
## Required Software
* Hadoop cluster with Hive installed (Hortonwork Data Platform (HDP) 2.6.5)
* Hive 1.2 or higher
* Python 3 or above 
* Python pip3 (a Python package installer)



## Installation
* Select machine to install software, better to **use one of slave machine** in the cluster (avoid use master node)
* Create target directory: /mobipack and go into that directory
* Install Python (3.7.9) and virtual environment
  ```
  wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz
  tar xvf Python-3.7.9.tgz
  cd Python-3.7*/
  ./configure --enable-loadable-sqlite-extensions --enable-optimizations
  make altinstall
  
  pip3.7 install virtualenv
  virtualenv ve379                  # create virtual environment
  source ve379/bin/activate         # enter virtual environment
  ```
* For CentOS, install additional software
  ```
  yum -y install cyrus-sasl cyrus-sasl-devel cyrus-sasl-lib 
  yum -y install git
  ```
* Clone the repository and then
  ```
  git clone https://github.com/SpatialDataCommons/CDR-wb-indicators-package.git
  ```
* Install all requirement packages in requirements.txt using command 
  * pip3.7 install -r requirements.txt
* Upload lib to hadoop master machine such as `/hive/lib`
  * /hive/lib/cdrlibindicator.jar
* Change lib path in `initial_hive_commands_wb_indicators.json` under `hive_init_commands`directory.
 
  ```
    {
      "hive_commands": [
      ...
      "ADD JAR /hive/lib/cdrlibindicator.jar",
      ...
      ]
    }
  ```

## Data preparation
The software need to access CDR data in Hive table with predefined structure.
Make sure your table follow the predefined structure.

### Structure of CDR records in Hive table
The table needs to contain the following data items:

```
IMEI           : International Mobile Equipment Identity (IMEI) of Caller
IMSI           : International Mobile Subscriber Identity (IMSI) of Caller
PDATE          : date only format, ex: 2020-10-29
CALL_DATETIME  : Activity Time (Start Time) in “YYYY-MM-DD HH:mm:ss” format 
LAC            : Location Area Code 
CELL_ID        : Unique Cell Tower ID 
LOGITUDE       : Real Number (decimal degree) in WGS84
LATITUDE       : Real Number (decimal degree) in WGS84
ADMIN1         : name of administrative level 1
ADMIN1_CODE    : code of administrative level 1
ADMIN2         : name of administrative level 2
ADMIN2_CODE    : code of administrative level 2
ADMIN3         : name of administrative level 3
ADMIN3_CODE    : code of administrative level 3
```

Example script for creating table on Hive.
```
create table cdr_data(imei string,imsi string,call_datetime string,lac string,cell_id string,longitude string,latitude string,
admin1 string, admin1_code string,admin2 string, admin2_code string,admin3 string, admin3_code string)
PARTITIONED BY (pdate string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS ORC;
```

If there is an existing table in different format, use following script to insert to the designed table.
```
set hive.exec.dynamic.partition.mode=nonstrict; 
set hive.exec.dynamic.partition=true; 

INSERT OVERWRITE  table cdr_data partition (pdate) 
select imei,imsi,call_datetime,lac,cell_id,longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code,to_date(call_datetime) as pdate
from <exiting table>;
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
	"cdr_data_table" : "cdr_data",	

	"host": "localhost",
	"port": 10000,
	"user": "hdfs",

	"output_data_path":"/output_indicators",

	"cdr_data_table_format_comment_": "imei,imsi,pdate,call_datetime,cell_id,longitude,latitude,admin1,admin1_code,admin2,admin2_code,admin3,admin3_code",

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
  Open `run_wb_indicators.py` and look for user section. You can comment the code out to not calculate it and similar for export part.
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
* **For indicator05**, Origin Destination Matrix - trips between two regions:
  * admin1,admin1_code,admin2,admin2_code,admin3,admin3_code = Origin
  * N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code = Destination
  * OD = total OD exclude the first observation OD of each day
  * OD_firstObservation = total count of the first observation OD of each day
  * totalOD= OD + OD_firstObservation 
* **For indicator07**, Mean and Standard Deviation of distance traveled (by home location):
  * H_admin1,H_admin1_code,H_admin2,H_admin2_code,H_admin3,H_admin3_code = Home location
  * all distance in KM
* **For indicator09**, Daily locations based on Home Region with average stay time and SD of stay time:
  * admin1,admin1_code,admin2,admin2_code = daily location at admin 2 level
  * H_admin1,H_admin1_code,H_admin2,H_admin2_code = home location at admin 2 level
  * mean_duration = in minutes
  * stdev_duration = in minutes
* **For indicator10**, Simple Origin Destination Matrix - trips between consecutive in time regions with duration:
  * admin1,admin1_code,admin2,admin2_code,admin3,admin3_code = Origin
  * N_admin1,N_admin1_code,N_admin2,N_admin2_code,N_admin3,N_admin3_code = Destination
  * all duration in minutes

