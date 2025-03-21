# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
# -*- coding: utf-8 -*-

"""
sliding-windows.py
~~~~~~~~~~~~~~~~~~~
This module:
    1. Creates a table environment
    2. Creates a source table from a Kinesis Data Stream
    3. Creates a sink table writing to a Kinesis Data Stream
    4. Queries from the Source Table and
       creates a sliding window over 24 hours to calculate the minimum value over the window.
    5. These sliding window results are inserted into the Sink table.
"""

from pyflink.table import EnvironmentSettings, TableEnvironment, DataTypes
from pyflink.table.window import Slide
from pyflink.table.udf import udf
import os
import json

# 1. Creates a Table Environment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"

is_local = (
    True if os.environ.get("IS_LOCAL") else False
)

if is_local:
    # only for local, overwrite variable to properties and pass in your jars delimited by a semicolon (;)
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"  # local

    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))
    table_env.get_config().get_configuration().set_string(
        "pipeline.jars",
        "file:///" + CURRENT_DIR + "/lib/flink-sql-connector-kinesis-1.15.2.jar",
    )


def get_application_properties():
    if os.path.isfile(APPLICATION_PROPERTIES_FILE_PATH):
        with open(APPLICATION_PROPERTIES_FILE_PATH, "r") as file:
            contents = file.read()
            properties = json.loads(contents)
            return properties
    else:
        print('A file at "{}" was not found'.format(APPLICATION_PROPERTIES_FILE_PATH))


def property_map(props, property_group_id):
    for prop in props:
        if prop["PropertyGroupId"] == property_group_id:
            return prop["PropertyMap"]


def create_input_table(table_name, stream_name, region, stream_initpos):
    return """ CREATE TABLE {0} (
                prac_id VARCHAR(255),
                data_czas VARCHAR(255),
                wejscie VARCHAR(255),
                wyjscie VARCHAR(255),
                event_time TIMESTAMP(3),
                WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
              )
              PARTITIONED BY (data_czas)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'scan.stream.initpos' = '{3}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region, stream_initpos)


def create_output_table(table_name, stream_name, region):
    return """ CREATE TABLE {0} (
                prac_id VARCHAR(255),
                data_czas VARCHAR(255),
                event_time VARCHAR(64)
              )
              PARTITIONED BY (data_czas)
              WITH (
                'connector' = 'kinesis',
                'stream' = '{1}',
                'aws.region' = '{2}',
                'format' = 'json',
                'json.timestamp-format.standard' = 'ISO-8601'
              ) """.format(table_name, stream_name, region)


def perform_sliding_window_aggregation(input_table_name):
    # use SQL Table in the Table API
    input_table = table_env.from_path(input_table_name)

    sliding_window_table = (
        input_table
            .window(
                Slide.over("24.hours")
                .every("12.hours")
                .on("event_time")
                .alias("one_day_window")
            )
            .group_by("data_czas, one_day_window")
            .select("data_czas, prac_id.count as prac_id_count, to_string(one_day_window.end) as event_time")
            .where("prac_id_count > 1")
    )

    return sliding_window_table


@udf(input_types=[DataTypes.TIMESTAMP(3)], result_type=DataTypes.STRING())
def to_string(i):
    return str(i)


table_env.create_temporary_system_function("to_string", to_string)

def main():
    # Application Property Keys
    input_property_group_key = "consumer.config.0"
    producer_property_group_key = "producer.config.0"

    input_stream_key = "input.stream.name"
    input_region_key = "aws.region"
    input_starting_position_key = "scan.stream.initpos"

    output_stream_key = "output.stream.name"
    output_region_key = "aws.region"

    # tables
    input_table_name = "input_table"
    output_table_name = "output_table"

    # get application properties
    props = get_application_properties()

    input_property_map = property_map(props, input_property_group_key)
    output_property_map = property_map(props, producer_property_group_key)

    input_stream = input_property_map[input_stream_key]
    input_region = input_property_map[input_region_key]
    stream_initpos = input_property_map[input_starting_position_key]

    output_stream = output_property_map[output_stream_key]
    output_region = output_property_map[output_region_key]

    # 2. Creates a source table from a Kinesis Data Stream
    table_env.execute_sql(create_input_table(input_table_name, input_stream, input_region, stream_initpos))

    # 3. Creates a sink table writing to a Kinesis Data Stream
    table_env.execute_sql(create_output_table(output_table_name, output_stream, output_region))

    # 4. Queries from the Source Table and creates a sliding window over 24 hours to calculate the minimum value
    # over the window.
    sliding_window_table = perform_sliding_window_aggregation(input_table_name)
    table_env.create_temporary_view("sliding_window_table", sliding_window_table)

    # 5. These sliding windows are inserted into the sink table
    table_result1 = table_env.execute_sql("INSERT INTO {0} SELECT prac_id, data_czas, event_time FROM {1}"
                                          .format(output_table_name, "sliding_window_table"))


    if is_local:
        table_result1.wait()
    else:
        # get job status through TableResult
        print(table_result1.get_job_client().get_job_status())


if __name__ == "__main__":
    main()
