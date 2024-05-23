from datetime import datetime
from pyspark.sql.functions import lit



class ScdService:

    def __init__(self, spark, logger):
        self.spark = spark
        self.logger = logger


    def apply_scd(self, full_data, incremental_data, pipeline_metadata):
        pipeline_id = pipeline_metadata.id
        self.logger.info(f'{pipeline_id} - Started')
        
        if full_data:
            incremental_data_vw = f'{pipeline_id.replace("-","_")}_incremental_data_vw'
            incremental_data.createOrReplaceTempView(incremental_data_vw)

            full_data_vw = f'{pipeline_id.replace("-","_")}_full_data_vw'
            full_data.createOrReplaceTempView(full_data_vw)

            join_condition = self.get_join_condition(pipeline_id, pipeline_metadata.primary_key_list)
            
            if pipeline_metadata.change_indicator_key:
                self.logger.info(f'{pipeline_id} - ChangeIndicatorKey - {pipeline_metadata.change_indicator_key}')
                return self.cdc_with_change_indicator(pipeline_id, pipeline_metadata.change_indicator_key, full_data_vw, incremental_data_vw, join_condition)
            else:
                self.logger.info(f'{pipeline_id} - ChangeIndicatorKey - Not Given')
                return self.cdc_without_change_indicator(pipeline_id, full_data_vw, incremental_data_vw, join_condition, full_data, incremental_data)

        self.logger.info(f'{pipeline_id} - Target table/partition does not exist')
        insert_data = incremental_data
        self.logger.info(f'{pipeline_id} - Insert Data - Ready. Count - {insert_data.count()}')
        return insert_data, None, None, None


    def cdc_with_change_indicator(self, pipeline_id, change_indicator_key, full_data_vw, incremental_data_vw, join_condition):
        supported_inserts = self.get_supported_cdc_inserts()
        insert_data = self.spark.sql(f"select incremental_data_vw.* from {incremental_data_vw} incremental_data_vw where incremental_data_vw.{change_indicator_key} in {supported_inserts}")
        insert_data = insert_data.drop(change_indicator_key)
        if 'rno' in insert_data.columns:
            insert_data = insert_data.drop('rno')
        self.logger.info(f'{pipeline_id} - Insert Data - Ready. Count - {insert_data.count()}')
        
        supported_updates = self.get_supported_cdc_updates()
        update_data = self.spark.sql(f"select incremental_data_vw.* From {incremental_data_vw} incremental_data_vw left join {full_data_vw} full_data_vw on {join_condition} where incremental_data_vw.{change_indicator_key} in {supported_updates}")
        update_data = update_data.drop(change_indicator_key)
        if 'rno' in update_data.columns:
            update_data = update_data.drop('rno')
        self.logger.info(f'{pipeline_id} - Update Data - Ready. Count - {update_data.count()}')
        
        supported_deletes = self.get_supported_cdc_deletes()
        delete_data = self.spark.sql(f"select incremental_data_vw.* from {incremental_data_vw} incremental_data_vw where incremental_data_vw.{change_indicator_key} in {supported_deletes}")
        delete_data = delete_data.drop(change_indicator_key)
        self.logger.info(f'{pipeline_id} - Delete Data - Ready. Count - {delete_data.count()}')
        
        unchanged_data = self.spark.sql(f"select full_data_vw.* From {full_data_vw} full_data_vw Left join {incremental_data_vw} incremental_data_vw on {join_condition} where incremental_data_vw.{change_indicator_key} is null")
        unchanged_data = unchanged_data.drop(change_indicator_key)
        self.logger.info(f'{pipeline_id} - Unchanged Data - Ready. Count - {unchanged_data.count()}')

        return insert_data, update_data, delete_data, unchanged_data
    
    
    def cdc_without_change_indicator(self, pipeline_id, full_data_vw, incremental_data_vw, join_condition, full_data, incremental_data):
        #Primary key is given
        if join_condition != '':
            update_data = self.spark.sql(f"select incremental_data_vw.* From {incremental_data_vw} incremental_data_vw join {full_data_vw} full_data_vw on {join_condition}")
            self.logger.info(f'{pipeline_id} - Update Data - Ready. Count - {update_data.count()}')
            
            insert_data = incremental_data.subtract(update_data)
            self.logger.info(f'{pipeline_id} - Insert Data - Ready. Count - {insert_data.count()}')
            
            changed_data = self.spark.sql(f"select full_data_vw.* From {incremental_data_vw} incremental_data_vw join {full_data_vw} full_data_vw on {join_condition}")
            unchanged_data = full_data.subtract(changed_data)
            self.logger.info(f'{pipeline_id} - Unchanged Data - Ready. Count - {unchanged_data.count()}')
            
            return insert_data ,update_data, None, unchanged_data
        #Primary Key is not given
        else:
            insert_data = incremental_data
            self.logger.info(f'{pipeline_id} - Insert Data - Ready. Count - {insert_data.count()}')
            return insert_data, None, None, None


    def get_supported_cdc_updates(self):
        supported_updates = ''
        
        for supported_update in Scd.Constants.CDC_SUPPORTED_UPDATES.value:
            supported_updates += f"\"{supported_update}\","
        
        if supported_updates != '':
            supported_updates = "("+supported_updates[:-1]+")"
            
        return supported_updates
    
    
    def get_supported_cdc_inserts(self):
        supported_inserts = ''
        
        for supported_insert in Scd.Constants.CDC_SUPPORTED_INSERTS.value:
            supported_inserts += f"\"{supported_insert}\","
        
        if supported_inserts != '':
            supported_inserts = "("+supported_inserts[:-1]+")"
            
        return supported_inserts
    
    
    def get_supported_cdc_deletes(self):
        supported_deletes = ''
        
        for supported_delete in Scd.Constants.CDC_SUPPORTED_DELETES.value:
            supported_deletes += f"\"{supported_delete}\","
        
        if supported_deletes != '':
            supported_deletes = "("+supported_deletes[:-1]+")"
            
        return supported_deletes
    

    def get_join_condition(self, pipeline_id, primary_key_list):
        self.logger.info(f'{pipeline_id} - Started')
        
        join_condition = ''
        index = 0
        
        for col in primary_key_list:
            join_condition = join_condition + ' full_data_vw.' + col + ' = ' + 'incremental_data_vw.' + col + ' and'

        if join_condition != '':
            index = join_condition.rfind("and")
            join_condition = join_condition[:index]

        self.logger.info(f'{pipeline_id} - Join Condition: {join_condition}')
        self.logger.info(f'{pipeline_id} - Succeeded')
        return join_condition


    #Need more work on this
    def cdc_type4(self,primary_key,full_target_df, insert_df):
        if full_target_df== None:
            final_df = insert_df.withColumn(Scd.Constants.ETL_INSERT_TS.value, lit(datetime.now()))
            return final_df
        deltaTable = full_target_df.join(insert_df, on=primary_key, how='anti').union(insert_df)
        final_df = deltaTable.withColumn(Scd.Constants.ETL_INSERT_TS.value, lit(datetime.now()))
        return final_df

import yaml


class YamlService:
    def __init__(self, logger):
        self.logger = logger
        
    def read_pipeline_info_from_application_yaml(self,app_name, pipeline_id, pipeline_version):
        yaml_file_name = f'{app_name}.{Project.FileType.YAML.value}'
        with open(yaml_file_name, "r") as yaml_file:
            yaml_dict = yaml.load(yaml_file, Loader=yaml.SafeLoader)
        return next((pipeline for pipeline in yaml_dict[Project.Constants.PIPELINES.value] if pipeline["pipelineId"] == pipeline_id and pipeline["versionNumber"] == pipeline_version), None)

    def read_all_pipelines_info_from_application_yaml(self,app_name):
        yaml_file_name = f'{app_name}.{Project.FileType.YAML.value}'
        with open(yaml_file_name, "r") as yaml_file:
            yaml_content = yaml.load(yaml_file, Loader=yaml.SafeLoader)
        return yaml_content[Project.Constants.PIPELINES.value]

    def get_all_pipelineids_with_latest_versions(self, app_name):
        pipelines = self.read_all_pipelines_info_from_application_yaml(app_name)
        pipelines_with_versions = {}
        for pipeline in pipelines:
            if pipeline["pipelineId"] in pipelines_with_versions:
                if pipeline["status"] == '' or pipeline["status"] == None :
                    pipeline["status"]  = "active"
                if pipeline["status"].lower() == "active" and pipeline["versionNumber"] > pipelines_with_versions[pipeline["pipelineId"]]:
                    pipelines_with_versions.update({pipeline["pipelineId"] : pipeline["versionNumber"]})
            else:
                if pipeline["status"]  == '' or pipeline["status"]  == None :
                    pipeline["status"]  = "active"
                if pipeline["status"].lower() == 'active'  :
                    pipelines_with_versions[pipeline["pipelineId"]] = pipeline["versionNumber"]
        
        return pipelines_with_versions


import json


class JobStatusService:
    def __init__(self, logger):
        self.logger = logger


    def get_column_from_last_successful_record(self, aws_region, job_env, pipeline_id, spark, sort_column, extract_column):
        table_items = self.get_jobstatus_entries_for_pipeline(aws_region, job_env, pipeline_id)
        
        table_items_json = json.dumps(table_items, cls = PythonUtility.get_encoder_class_name())
        table_items_df = spark.read.json(spark.sparkContext.parallelize([table_items_json]))
        
        last_successful_run_value = None
        
        if table_items_df.count() > 0:
            successful_run_df = table_items_df.filter(f"{Dynamodb.JobStatus.JOB_STATUS.value} = '{Project.Status.SUCCESS_FLAG.value}'")
            if successful_run_df.count() > 0:
                last_successful_run_df = successful_run_df.orderBy(sort_column, ascending=[False]).limit(1)
                last_successful_run_df = last_successful_run_df.select(extract_column)

                last_successful_run_value = last_successful_run_df.collect()[0][0]

        return last_successful_run_value


    def get_last_consumed_kafka_offset(self, aws_region, job_env, pipeline_id, spark):
        table_items = self.get_jobstatus_entries_for_pipeline(aws_region, job_env, pipeline_id)
        
        table_items_json = json.dumps(table_items, cls = PythonUtility.get_encoder_class_name())
        table_items_df = spark.read.json(spark.sparkContext.parallelize([table_items_json]))
        
        last_consumed_kafka_offset = 0
        
        if table_items_df.count() > 0:
            successful_run_df = table_items_df.filter(f"{Dynamodb.JobStatus.JOB_STATUS.value} = '{Project.Status.SUCCESS_FLAG.value}'")
            if successful_run_df.count() > 0:
                last_successful_run_df = successful_run_df.orderBy(Dynamodb.JobStatus.EXECUTION_END_DTTM.value, ascending=[False]).limit(1)
                last_successful_run_df = last_successful_run_df.select(Dynamodb.JobStatus.LAST_CONSUMED_KAFKA_INFO.value)
                last_consumed_kafka_offset = last_successful_run_df.collect()[0][0]

        return last_consumed_kafka_offset


    def get_jobstatus_entries_for_pipeline(self, aws_region, job_env, pipeline_id):
        jobstatus_table_name = Dynamodb.TableName.JOB_STATUS.value.format(job_env)
        filter_key = Dynamodb.JobStatus.PIPELINE_ID.value
        
        return DynamoDbRepository.scan_with_filter_expression(aws_region, jobstatus_table_name, filter_key, pipeline_id, Dynamodb.Filters.EQUALS.value, self.logger)


    def update_jobstatus_entry(self, jobstatus_entry, pipeline_id, business_start_datetime, business_end_datetime, job_end_datetime, jobstatus_flag, landing_count, final_count, processed_count, sla_met, error_message, job_env, aws_region, last_consumed_kafka_offset = None):
        self.logger.info(f"{pipeline_id} - Started")
        jobstatus_table_name = Dynamodb.TableName.JOB_STATUS.value.format(job_env)
        updated_jobstatus_entry = JobStatusEntryMapper(self.logger).update_jobstatus_entry(jobstatus_entry, business_start_datetime, business_end_datetime, job_end_datetime, jobstatus_flag, landing_count, final_count, processed_count, sla_met, error_message, last_consumed_kafka_offset = last_consumed_kafka_offset)
        
        jobstatus_key_dictionary = {
                Dynamodb.JobStatus.PIPELINE_ID.value: pipeline_id,
                Dynamodb.JobStatus.EXECUTION_ID.value: updated_jobstatus_entry[Dynamodb.JobStatus.EXECUTION_ID.value]
            }
        jobstatus_update_expression = "set business_order_from_dttm=:business_start_datetime, \
                                business_order_to_dttm=:business_end_datetime, \
                                execution_end_dttm=:job_end_datetime, \
                                job_status=:jobstatus_flag, \
                                landing_count=:landing_count, \
                                final_count=:final_count, \
                                processed_count=:processed_count, \
                                sla_met=:sla_met, \
                                last_consumed_kafka_offset=:last_consumed_kafka_offset, \
                                error_message=:error_message"
        jobstatus_update_values_dictionary = {
                ':business_start_datetime': updated_jobstatus_entry[Dynamodb.JobStatus.BUSINESS_ORDER_FROM_DTTM.value],
                ':business_end_datetime': updated_jobstatus_entry[Dynamodb.JobStatus.BUSINESS_ORDER_TO_DTTM.value],
                ':job_end_datetime': updated_jobstatus_entry[Dynamodb.JobStatus.EXECUTION_END_DTTM.value],
                ':jobstatus_flag': updated_jobstatus_entry[Dynamodb.JobStatus.JOB_STATUS.value],
                ':landing_count': updated_jobstatus_entry[Dynamodb.JobStatus.LANDING_COUNT.value],
                ':final_count': updated_jobstatus_entry[Dynamodb.JobStatus.FINAL_COUNT.value],
                ':processed_count': updated_jobstatus_entry[Dynamodb.JobStatus.PROCESSED_COUNT.value],
                ':sla_met': updated_jobstatus_entry[Dynamodb.JobStatus.SLA_MET.value],
                ':last_consumed_kafka_offset': updated_jobstatus_entry[Dynamodb.JobStatus.LAST_CONSUMED_KAFKA_INFO.value],
                ':error_message': updated_jobstatus_entry[Dynamodb.JobStatus.ERROR_MESSAGE.value]
            }
        DynamoDbRepository.update_item(aws_region, jobstatus_table_name, jobstatus_key_dictionary, jobstatus_update_expression, jobstatus_update_values_dictionary, self.logger)
        self.logger.info(f"{pipeline_id} - Succeeded. Execution ID: {jobstatus_entry[Dynamodb.JobStatus.EXECUTION_ID.value]}")

        return updated_jobstatus_entry


    def put_jobstatus_entry_using_pipeline(self, aws_region, job_env, pipeline, job_start_datetime):
        pipeline_id = pipeline.metadata.id
        load_type = pipeline.metadata.load_type
        load_phase = pipeline.metadata.load_phase
        load_frequency = pipeline.metadata.load_frequency
        table_name = self.construct_target_table_name(pipeline, job_env)
        
        return self.put_jobstatus_entry(aws_region, job_env, pipeline_id, load_type, load_phase, load_frequency, table_name, job_start_datetime)

        
    def construct_target_table_name(self, pipeline, job_env):
        table_name = ""

        for resource in pipeline.resources:
            if resource.flag == "target":
                if resource.metadata.resource_type == "database":
                    table_name = resource.metadata.table_name
                elif resource.metadata.resource_type == "file":
                    domain_name = pipeline.domain.name
                    pipeline_phase = pipeline.metadata.load_phase
                    file_name = resource.metadata.file_name
                    table_name = f"{prefix}_{domain_name}_{pipeline_phase}_{job_env}.{file_name}"

        return table_name
    
    
    def put_jobstatus_entry(self, aws_region, job_env, pipeline_id, load_type, load_phase, load_frequency, table_name, job_start_datetime):
        self.logger.info(f"{pipeline_id} - Started")
        
        jobstatus_entry = JobStatusEntryMapper(self.logger).generate_initial_entry(pipeline_id, load_type, load_phase, load_frequency, table_name, job_start_datetime)

        jobstatus_table_name = Dynamodb.TableName.JOB_STATUS.value.format(job_env)

        DynamoDbRepository.put_item(aws_region, jobstatus_table_name, jobstatus_entry, self.logger)

        self.logger.info(f"{pipeline_id} - Succeeded. Execution ID: {jobstatus_entry[Dynamodb.JobStatus.EXECUTION_ID.value]}")

        return jobstatus_entry
        
        
        
