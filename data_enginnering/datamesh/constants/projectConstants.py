from ..constants import dbConstants


#Path constants
DEFAULT_JARS_PATH = 'resources/externalJars/'
DEFAULT_CONFIG_FILES_PATH = 'resources/jobConfigs/'
DEFAULT_S3_WRITE_PATH_PREFIX = 'mis/data_mesh/ingfw/'
DEFAULT_S3_CONTROL_PATH_PREFIX = 'controllers/'

# INV_PATH = "s3://datamesh-ingfw-qa-us-east-1/controllers/inventory_dir/inventory.csv"
# EXE_PATH = "s3://datamesh-ingfw-qa-us-east-1/controllers/exec_dir/exec.csv"
INV_PATH = "D:\\data\\inventory_dir\\inventory.csv"
EXE_PATH = "D:\\data\\exec_dir\\exec.csv"

#PROGRAME CONSTANTS
DATE_FORMAT = '%m%d%Y'
DATE_TIME_FORMAT = '%m%d%Y_%H%M%S'
PARQUET_TYPE = 'parquet'
FILE_TYPE = 'file'
CSV_TYPE = 'csv'
INIT_ID = '1'
EFF_CLOSE_DATE = '12-31-9999'
EFF_FLAG_Y='Y'
EFF_FLAG_N='N'

#spark constants
DEFAULT_SPARK_JDBC_FETCH_SIZE = '10000'
DEFAULT_SPARK_WRITE_CSV_HEADER_OPTN =True
DEFAULT_TARGET_NUM_PARTITIONS = 4
DEFAULT_SPARK_WRITE_FORMAT = PARQUET_TYPE
DEFAULT_SPARK_EXTRA_CLASS_PATH_CONFIG = DEFAULT_JARS_PATH+dbConstants.oracleJar+':'+DEFAULT_JARS_PATH+dbConstants.sybaseJar+':'+DEFAULT_JARS_PATH+dbConstants.db2Jar
SUPPORTED_TARGET_TYPES = ['file']
SUPPORTED_FILE_EXTENSION_TYPES = [PARQUET_TYPE, CSV_TYPE]
DEFAULT_CSV_DELIMITER = ','
DEFAULT_S3_BUCKET = 'datamesh-ingfw-qa-us-east-1'

# Flags
IN_PROGRESS_FLAG = 'i'
SUCCESS_FLAG = 's'
FAILURE_FLAG = 'f'