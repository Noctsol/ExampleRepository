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
from datetime import timedelta
import sys
import hashlib
from library import logger      # For creating logs
from dotenv import load_dotenv  # Custom package for environment variables
import os

# RANDOM ADD CHGANGEWS

print(1+1)
