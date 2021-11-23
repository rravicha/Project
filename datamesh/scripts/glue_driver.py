import sys
import os
from mis_dm_ingfw_common.commons import *
from mis_dm_ingfw_common.utils import *
from mis_dm_ingfw_common.appCore import jobInitializer
# from awsglue.utils import getResolvedOptions
def scd(spark, logger, src, configParams, appname, load_type,curr_run_date,prv_run_date=None):
    print(f'starting scd2 logic for {src} load type : {load_type}')
    print(f'ending scd2 logic for {src} load type : {load_type}')
    status_code ='s'
    return status_code
def mis_dm_ingfw_appRunner():
    # runTimeArgs = getResolvedOptions(sys.argv, ['JOB_NAME', 'AppName'])
    # APPNAME=runTimeArgs['AppName']
    APPNAME='sbi'
    spark = jobInitializer.initializeSpark(APPNAME)
    logger = jobInitializer.getGlueLogger(spark)
    logger.info("current working directory {}".format(os.getcwd()))
    logger.info("Initialized Spark Session..")
    # configParams = configParser.getConfig(logger=logger, appName=APPNAME)
    xAPPNAME="D:\\data\\sbi";configParams = configParser.getConfig(logger=logger, appName=xAPPNAME)
    src_type_list={}
    src_type_list = load_dict(spark,logger,configParams,appName=APPNAME)
    logger.info("Dict src_type_list {}".format(src_type_list))
    logger.info('Loading all df')
    # dfs = sparkDataFrameReader.getDFsFromSource(spark=spark,
    #                                             logger=logger,
    #                                             config=configParams)
    # logger.info('loaded all dataframes from all the sources...')
    # logger.info('Writing all df')
    # sparkDataFrameWriter.writeDFsToDestination(logger=logger,
    #                                             appName=APPNAME,
    #                                             config=configParams,
    #                                             dfDict=dfs    )
    # logger.info('Wrote all the dataframes from all the sources to their respective destinations')
    for src, load_type in src_type_list.items():
        if load_type == 'History':
            inv_df = read_csv(spark,logger,projectConstants.INV_PATH)
            exec_df = read_csv(spark,logger,projectConstants.EXE_PATH)
            print('controllers loaded in the loop')
            srcConfigDict = configParams[yamlConfigConstants.sources][src]
            srcConfigObj = configParser.configDictToObjConverter(srcConfigDict)
            curr_exec_df = set_curr_exec_info(spark,logger,inv_df,exec_df,
            APPNAME,src,srcConfigObj.SourceFormat,srcConfigObj.SourceType,srcConfigObj.TableorFilename
            )
            scd_rc = scd(spark, logger, src, configParams, APPNAME, load_type,curr_run_date=curr_exec_df.collect()[0][0],prv_run_date=None)
            # logger.info('pre end before csv write')
            # final_exec_df = exec_df.union(curr_exec_df)
            # final_exec_df.coalesce(1).write.format("csv") \
            #     .option("header", "true") \
            #         .mode("append") \
            #             .save(projectConstants.EXE_PATH)
            print('csv write done')
    print('end of program')
if __name__ == '__main__':
    mis_dm_ingfw_appRunner()import sys
import os
from mis_dm_ingfw_common.commons import *
from mis_dm_ingfw_common.utils import *
from mis_dm_ingfw_common.appCore import jobInitializer
# from awsglue.utils import getResolvedOptions
def scd(spark, logger, src, configParams, appname, load_type,curr_run_date,prv_run_date=None):
    print(f'starting scd2 logic for {src} load type : {load_type}')
    print(f'ending scd2 logic for {src} load type : {load_type}')
    status_code ='s'
    return status_code
def mis_dm_ingfw_appRunner():
    # runTimeArgs = getResolvedOptions(sys.argv, ['JOB_NAME', 'AppName'])
    # APPNAME=runTimeArgs['AppName']
    APPNAME='sbi'
    spark = jobInitializer.initializeSpark(APPNAME)
    logger = jobInitializer.getGlueLogger(spark)
    logger.info("current working directory {}".format(os.getcwd()))
    logger.info("Initialized Spark Session..")
    # configParams = configParser.getConfig(logger=logger, appName=APPNAME)
    xAPPNAME="D:\\data\\sbi";configParams = configParser.getConfig(logger=logger, appName=xAPPNAME)
    src_type_list={}
    src_type_list = load_dict(spark,logger,configParams,appName=APPNAME)
    logger.info("Dict src_type_list {}".format(src_type_list))
    logger.info('Loading all df')
    # dfs = sparkDataFrameReader.getDFsFromSource(spark=spark,
    #                                             logger=logger,
    #                                             config=configParams)
    # logger.info('loaded all dataframes from all the sources...')
    # logger.info('Writing all df')
    # sparkDataFrameWriter.writeDFsToDestination(logger=logger,
    #                                             appName=APPNAME,
    #                                             config=configParams,
    #                                             dfDict=dfs    )
    # logger.info('Wrote all the dataframes from all the sources to their respective destinations')
    for src, load_type in src_type_list.items():
        if load_type == 'History':
            inv_df = read_csv(spark,logger,projectConstants.INV_PATH)
            exec_df = read_csv(spark,logger,projectConstants.EXE_PATH)
            print('controllers loaded in the loop')
            srcConfigDict = configParams[yamlConfigConstants.sources][src]
            srcConfigObj = configParser.configDictToObjConverter(srcConfigDict)
            curr_exec_df = set_curr_exec_info(spark,logger,inv_df,exec_df,
            APPNAME,src,srcConfigObj.SourceFormat,srcConfigObj.SourceType,srcConfigObj.TableorFilename
            )
            scd_rc = scd(spark, logger, src, configParams, APPNAME, load_type,curr_run_date=curr_exec_df.collect()[0][0],prv_run_date=None)
            # logger.info('pre end before csv write')
            # final_exec_df = exec_df.union(curr_exec_df)
            # final_exec_df.coalesce(1).write.format("csv") \
            #     .option("header", "true") \
            #         .mode("append") \
            #             .save(projectConstants.EXE_PATH)
            print('csv write done')
    print('end of program')
if __name__ == '__main__':
    mis_dm_ingfw_appRunner()