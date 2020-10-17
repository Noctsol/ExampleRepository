"""
Developed by Michael Perkins
Last Update: 2/28/2020
Create Date: 2/20/2020

Summary: This script pulls data from various fmPilot databases/tables and sends it to splunk to be used as lookup tables
"""
### Import dependancies
from library import method_helper
from library import sql_connection
from connection import exception_engine
from ref import splunk_data_ee_table_map as query_map

import datetime
import sys
import hashlib
from library import logger      # For creating logs
from dotenv import load_dotenv  # Custom package for environment variables
import os

# Gets parent directory of the script for Windows service account usage
directory = os.path.dirname(os.path.realpath(__file__))
env_path = os.path.join(directory, "Connection/EnvironmentVariables.env") # Environment variables
load_dotenv(dotenv_path=env_path)  # Loads environment variables

# Logger Related
log_path = os.getenv('splunk_log_path')   # Getting enrivonment variable
script_name = os.path.basename(__file__)  # Gets Script Name
script_type = "PROCESS"  # Set Script Type 'REPORT','DATA','UTILITY','PROCESS'
logger = logger.Logger(log_path, script_name, script_type) # Initializing logger

script_name = 'splunk_data_automation_ee_client' 

### Library objects
## Set method_helper and environment_variables
helper = method_helper.MethodHelper()

### Set ExceptionEngine database configuration and connection
ex_db_config = exception_engine.dbconfig
ex_sql_obj = sql_connection.SqlConnector(ex_db_config['host'], ex_db_config['user'], ex_db_config['password'], database=ex_db_config['dbname'])

### Create paths if they do not exist
helper.check_and_create_directory(log_path)
splunk_ee_report_path = os.getenv('splunk_ee_report_path')
helper.check_and_create_directory(splunk_ee_report_path)

### All queries mapped to their filenames
data_map = query_map.splunk_data_automation_ee_client_map

### Set email information and file extension
email_to = ['fsautomation@cbre.com']
email_subject = 'Script Notification - splunk_data_automation_ee_client.py'

### Declare file extension
internal_file_extension = '.psv'
field_term = '|'

### Set timestamps to be used in file name
timestamp_datetime = datetime.datetime.utcnow()
timestamp_str = timestamp_datetime.strftime('%Y%m%d')				# Used for filename

### Create log
try:
    logger.start_log()
except Exception as e:
	temp_msg = 'Unable to add_log for script. Exception: {0}'.format(str(e))
	helper.send_email(email_to, email_subject, message=temp_msg)

#############################################################
# Part 1: Pull Data
#############################################################
###
# Pull Table Data
###
try:
    # Loops through each json key to grab the json values contained inside, containing table and folder information
    for table_name in data_map:
        # Set table variable for SQL query, set folder variable for creating a folder path later
        table = data_map[table_name]['table']
        folder = data_map[table_name]['folder']

        # Declare query used to select data from table
        query = """SELECT * FROM [[table]]"""
        query = query.replace('[[table]]', table)

        ee_client_dataset = []
        # Run query, store information in ee_client_dataset
        try:
            logger.log_info(f'Executing query for {table}')

            # Pull data from ExceptionEngine database
            ee_client_dataset = ex_sql_obj.get_data(query)

            # Verify the returned dataset is not empty 
            assert(ee_client_dataset), 'Query returned empty dataset'
            assert(len(ee_client_dataset) > 1), 'Query returned empty dataset'
        except Exception as e:
            logger.log_error(f'Fatal error occured while querying {table}: {str(e)}')

#############################################################
# Part 2: Clean data
#############################################################    

        # NOTE: If this data does not show properly on splunk, consider removing quoteall (below), if that failes, consider cleaning extra quotes ""
        
        # Remove '|' from the dataset
        if (ee_client_dataset):
            helper.clean_lst_for_bulk_insert(ee_client_dataset, fieldterminator=field_term)
        else:
            logger.log_warning(f'Table is empty. Exiting.')
            sys.exit()

#############################################################
# Part 3: Write Data to file
#############################################################
    # Writed queried dataset to a .psv file
        try:
            # Create a folder path
            folder_path = splunk_ee_report_path + folder
            helper.check_and_create_directory(folder_path)
            # Write dataset to .psv file in created folder path
            output_file = folder_path + table_name + "_" + timestamp_str + internal_file_extension
            helper.write_to_csv(output_file, ee_client_dataset, _delimiter=field_term, _quoteall=True)
            # Verify file wrote successfully
            assert(helper.file_exists(output_file)), f'File is empty or does not exist: {output_file}'
            logger.log_info(f'Wrote {table_name} data to {output_file}')
        except Exception as e:
            logger.log_error(f'Fatal error occured while exporting query data. Exception: {str(e)}')
            
except Exception as e:
	logger.log_error(f'Fatal error: {str(e)}')

# Adding log for successful completion of script
logger.end_log()