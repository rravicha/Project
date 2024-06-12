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
        
        
        

class ParserExecutionService:

    @staticmethod
    def log_parser_parameters(job_environment, aws_region, logger):
        logger.info('Input Parameters')

        logger.info(f'Job Environment : {job_environment}')
        logger.info(f'AWS Region : {aws_region}')

    @staticmethod
    def validate_parser_parameters(job_environment, aws_region, logger):
        if not job_environment:
            logger.error("Job Environment is needed")
            raise SystemExit('Job Environment is needed')

        if not aws_region:
            logger.error("Aws Region is needed")
            raise SystemExit('Aws Region is needed')

    @staticmethod
    def generate_yaml_files_from_dynamodb(aws_region, job_environment, logger):
        bucket_name = None
        file_path = None
        file_names = None
        error_message = None
        try:
            pipelines_df = ParserExecutionService.get_all_records_from_pipeline_table(aws_region, job_environment, logger)
            logger.info(f"Reading the data from dynamodb table - Done.")
            
            application_groups, pipeline_record_columns = ParserExecutionService.group_all_records_by_app_name(pipelines_df, logger)
            logger.info("Creating application groups - Done.")
            
            bucket_name, file_path, file_names = ParserExecutionService.create_yaml_files_from_application_groups(application_groups, pipeline_record_columns, job_environment, aws_region)
            logger.info("Writing yaml files to s3 - Done")
            
            job_status = Project.Status.SUCCESS_FLAG.value
        except Exception as error:
            job_status = Project.Status.FAILURE_FLAG.value
            error_message = f"{error}"
        finally:
            parser_response = ParserResponseMapper(logger).prepare_parser_response(job_status, bucket_name, file_path, file_names, error_message)
            return parser_response


    @staticmethod
    def get_all_records_from_pipeline_table(aws_region, job_environment, logger):
        pipeline_table_name = Dynamodb.TableName.INVENTORY_PIPELINE.value.format(job_environment)

        pipeline_table_items = DynamoDbRepository.scan(aws_region, pipeline_table_name, logger)
        logger.info(f"Number of records found : {len(pipeline_table_items)}")

        pipeline_table_items_json_str = json.dumps(pipeline_table_items, cls = PythonUtility.get_encoder_class_name())
        pipeline_table_items_json = pandas.DataFrame.from_dict(json.loads(pipeline_table_items_json_str))
        pipeline_table_items_df = pandas.DataFrame(pipeline_table_items_json)
        pipeline_table_items_df = pipeline_table_items_df.where(pandas.notnull(pipeline_table_items_df), None)
        return pipeline_table_items_df


    @staticmethod
    def group_all_records_by_app_name(pipelines_df, logger):
        logger.info(pipelines_df)
        pipeline_record_columns = pipelines_df.columns
        logger.info(pipeline_record_columns)
        app_info_index = pipeline_record_columns.get_loc("appInfo")
        logger.info(app_info_index)
        application_groups = defaultdict(lambda: [])

        for pipeline in pipelines_df.values:
            app_info_string = pipeline[app_info_index]
            app_info_dictionary = json.loads(app_info_string)
            app_name = app_info_dictionary["name"]
            application_groups[app_name].append(pipeline)
        return application_groups, pipeline_record_columns


    @staticmethod
    def create_yaml_files_from_application_groups(application_groups, pipeline_record_columns, job_environment,
                                                  aws_region):
        pipeline_record_columns_list = list(pipeline_record_columns)
        aws_account_id = AwsUtility.get_account_id()
        bucket_name = f"datamesh-ingfw-{aws_account_id}-{job_environment}-{aws_region}"
        file_path = Project.Paths.YAML_DEFAULT.value
        file_names = []

        for app_name, pipelines in application_groups.items():
            pipelines_data = [dict(zip(pipeline_record_columns_list, pipeline)) for pipeline in pipelines]
            applications_data = {Dynamodb.Constants.APP_NAME.value: app_name,
                                 Project.Constants.PIPELINES.value: pipelines_data}
            file_name = f"{app_name}.{Project.FileType.YAML.value}"
            S3Repository.write(bucket_name, file_path, file_name, yaml.dump(applications_data))
            file_names.append(file_name)

        return bucket_name, file_path, file_names


    @staticmethod
    def log_parser_response(parser_response, logger):
        logger.info('Parser Response')

        logger.info(f'Job Status : {parser_response.jobstatus}')
        logger.info(f'Bucket Name : {parser_response.bucket_name}')
        logger.info(f'File Path : {parser_response.file_path}')
        logger.info(f'File Names : {parser_response.file_names}')
        logger.info(f'Error Message : {parser_response.error_message}')
        
        

class DynamoDbRepository:
    aws_region = AwsUtility.get_current_region()
    dynamodb_resource = boto3.resource(Aws.Services.DYNAMO_DB.value, region_name = aws_region)

    @staticmethod
    def scan(aws_region, table_name, logger):
        try:
            table = DynamoDbRepository.dynamodb_resource.Table(table_name)
            table_scan = table.scan()
            
            table_items = table_scan[Dynamodb.Constants.TABLE_ITEMS.value]
            
            while Dynamodb.Constants.LAST_EVALUATED_KEY.value in table_scan:
                table_scan = table.scan(ExclusiveStartKey=table_scan[Dynamodb.Constants.LAST_EVALUATED_KEY.value])
                if Dynamodb.Constants.TABLE_ITEMS.value in table_scan:
                    table_items.extend(table_scan[Dynamodb.Constants.TABLE_ITEMS.value])
            return table_items
        except Exception as err:
            logger.error(f'Failed, Parameters : [table_name = {table_name}, aws_region = {aws_region}], Error: {err}')
            raise Exception(f'DynamoDB Scan Failure')


    @staticmethod
    def scan_with_filter_expression(aws_region, table_name, filter_key, filter_values, filter_type, logger):
        try:
            table = DynamoDbRepository.dynamodb_resource.Table(table_name)
            if isinstance(filter_values, str):
                values = filter_values.split(",")
            else:
                values = filter_values
            for filter_value in values:
                if filter_type.lower() == Dynamodb.Filters.EQUALS.value:
                    table_scan = table.scan(FilterExpression=Attr(filter_key).eq(filter_value))
                else:
                    table_scan = table.scan(FilterExpression=Attr(filter_key).contains(filter_value))
                table_items = table_scan[Dynamodb.Constants.TABLE_ITEMS.value]

                while Dynamodb.Constants.LAST_EVALUATED_KEY.value in table_scan:
                    if filter_type.lower() == Dynamodb.Filters.EQUALS.value:
                        table_scan = table.scan(FilterExpression=Attr(filter_key).eq(filter_value), ExclusiveStartKey = table_scan[Dynamodb.Constants.LAST_EVALUATED_KEY.value])
                    else:
                        table_scan = table.scan(FilterExpression=Attr(filter_key).contains(filter_value), ExclusiveStartKey = table_scan[Dynamodb.Constants.LAST_EVALUATED_KEY.value])
                    table_items.extend(table_scan[Dynamodb.Constants.TABLE_ITEMS.value])

            return table_items

        except Exception as err:
            logger.error(f'Failed, Parameters : [table_name = {table_name}, filter_key : {filter_key}, filter_values : {filter_values}], Error: {err}')
            raise Exception('DynamoDB Scan Failure')


    @staticmethod
    def put_item(aws_region, table_name, table_item, logger):
        try:
            table = DynamoDbRepository.dynamodb_resource.Table(table_name)

            response = table.put_item(Item = table_item)
            response_code = response[Dynamodb.Constants.METADATA.value][Dynamodb.Constants.HTTP_STATUS_CODE.value]
            
            if response_code != 200:
                logger.error(f'Failed, Parameters : [aws_region = {aws_region}, table_name = {table_name}, table_item = {table_item}]')
                raise Exception('DynamoDB Put Item Failure')
        
        except Exception as err:
            logger.error(f'Failed, Parameters : [aws_region = {aws_region}, table_name = {table_name}, table_item = {table_item}], Error : {err}')
            raise Exception('DynamoDB Put Item Failure')


    @staticmethod
    def update_item(aws_region, table_name, table_key_dictionary, update_expression, expression_attribute_values_dictionary, logger):
        try:
            table = DynamoDbRepository.dynamodb_resource.Table(table_name)

            response = table.update_item(Key = table_key_dictionary, UpdateExpression = update_expression, ExpressionAttributeValues = expression_attribute_values_dictionary)
            response_code = response[Dynamodb.Constants.METADATA.value][Dynamodb.Constants.HTTP_STATUS_CODE.value]
            
            if response_code != 200:
                logger.error(f'Failed, Parameters : [aws_region = {aws_region}, table_name = {table_name}, table_key_dictionary = {table_key_dictionary}, update_expression = {update_expression}, expression_attribute_values_dictionary = {expression_attribute_values_dictionary}]')
                raise Exception('DynamoDB Update Item Failure')
        
        except Exception as err:
            logger.error(f'Failed, Parameters : [aws_region = {aws_region}, table_name = {table_name}, table_key_dictionary = {table_key_dictionary}, , update_expression = {update_expression}, expression_attribute_values_dictionary = {expression_attribute_values_dictionary}], Error : {err}')
            raise Exception('DynamoDB Update Item Failure')    


    @staticmethod
    def delete_item(aws_region, table_name, partition_key, partition_value, sort_key, sort_key_value, logger):
        key_info = {}
        
        if partition_key and partition_value:
            key_info[partition_key] = partition_value
        else:
            logger.error(f'Failed, Parameters : [table_name = {table_name}, partition_key = {partition_key}, partition_value = {partition_value}, sort_key = {sort_key}, sort_value = {sort_value}, aws_region = {aws_region}], Error : Partition Key and Partition Value are mandatory')
            raise Exception('DynamoDB Delete Item Input Failure')
            
        if sort_key and sort_key_value:
            key_info[sort_key] = sort_key_value
        
        try:
            table = DynamoDbRepository.dynamodb_resource.Table(table_name)
            
            response = table.delete_item(Key = key_info)
            response_code = response[Dynamodb.Constants.METADATA.value][Dynamodb.Constants.HTTP_STATUS_CODE.value]
            
            if response_code != 200:
                logger.error(f'Failed, Parameters : [table_name = {table_name}, partition_key = {partition_key}, partition_value = {partition_value}, sort_key = {sort_key}, sort_key_value = {sort_key_value}, aws_region = {aws_region}]')
                raise Exception('DynamoDB Delete Item Failure')
        
        except Exception as err:
            logger.error(f'Failed, Parameters : [table_name = {table_name}, partition_key = {partition_key}, partition_value = {partition_value}, sort_key = {sort_key}, sort_key_value = {sort_key_value}, aws_region = {aws_region}], Error : {err}')
            raise Exception('DynamoDB Delete Item Failure')

#parallel processing
    @staticmethod
    def trigger_pipelines_in_parallel(pipelineids_with_versions, app_name, job_env, aws_region, business_start_datetime, business_end_datetime, run_in_daily_chunks_flag, parallel_pipelines_limit, spark, logger):
    
        with concurrent.futures.ThreadPoolExecutor(max_workers=parallel_pipelines_limit) as pipeline_executor:
            pipeline_executors = []
            for pipeline_id, pipeline_version in pipelineids_with_versions.items():
                pipeline_executors.append(pipeline_executor.submit(
                    PipelineHandler.run_pipeline, app_name, pipeline_id, pipeline_version, job_env, aws_region, business_start_datetime, business_end_datetime, run_in_daily_chunks_flag, spark, logger)
                )
    
        pipeline_response_list = []
        for pipeline_executor in concurrent.futures.as_completed(pipeline_executors):
            pipeline_response_list.extend(pipeline_executor.result())
     
        return pipeline_response_list
