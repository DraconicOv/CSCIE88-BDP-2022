import json
import os
import time
from abc import ABC, abstractmethod
from typing import Iterable, Tuple

from pyflink.common import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.datastream.connectors import FlinkKafkaProducer

from pyflink.datastream import FlatMapFunction
from pyflink.common import Types, Time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (DataTypes, TableDescriptor, Schema, StreamTableEnvironment)
from pyflink.table.udf import udf
from pyflink.datastream.window import TimeWindow
from pyflink.datastream.window import Collection, Trigger, TimeWindowSerializer, TriggerResult, TypeSerializer, IN, OUT, W2,W, KEY, Generic
from pyflink.datastream.window import Window
#Datastream.window in the EC2 Instance was not the same as on local, so wrote down all dir of that
from pyflink.datastream.functions import KeyedStateStore, Function
#Constants doesnt exist there either, soo..
from pyflink.datastream import ProcessWindowFunction, AggregateFunction
from pyflink.datastream.window import WindowAssigner, 
from pyflink.datastream.window import T
from pyflink.common import Time
#Had to redefine a few things.
class MyProcessWindowFunction(ProcessWindowFunction):

    def process(self, key: str, context: ProcessWindowFunction.Context,
                elements: Iterable[Tuple[str, int]]) -> Iterable[str]:
        count = 0
        for _ in elements:
            count += 1
        yield "Window: {} count: {}".format(context.window(), count)
        
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(stream_execution_environment=env)

# see https://nightlies.apache.org/flink/flink-docs-release-1.12/dev/python/datastream-api-users-guide/intro_to_datastream_api.html
# https://blog.devgenius.io/playing-pyflink-in-a-nutshell-3abd16467677
# flink-sql-connector-kafka_2.12-1.14.0.jar
env.add_jars("file:///opt/flink/lib/flink-connector-kafka_2.12-1.14.0.jar")

deserialization_schema = SimpleStringSchema()

kafkaSource = FlinkKafkaConsumer(
    topics='p2_input',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'broker1:29092', 'group.id': 'test'}
)

serialization_schema = SimpleStringSchema()

kafkaSink = FlinkKafkaProducer(
    topic='p2_output',
    serialization_schema=serialization_schema,
    producer_config={'bootstrap.servers': 'broker1:29092', 'group.id': 'test'}
)
class AverageAggregate(AggregateFunction):
 
    def create_accumulator(self) -> Tuple[int, int]:
        return 0, 0

    def add(self, value: Tuple[str, int], accumulator: Tuple[int, int]) -> Tuple[int, int]:
        return accumulator[0] + value[1], accumulator[1] + 1

    def get_result(self, accumulator: Tuple[int, int]) -> float:
        return accumulator[0] / accumulator[1]

    def merge(self, a: Tuple[int, int], b: Tuple[int, int]) -> Tuple[int, int]:
        return a[0] + b[0], a[1] + b[1]
def split_line(line):
    return line.split(" ")
ds = env.add_source(kafkaSource)
ds \
    .key_by(lambda v: str(v).split[1]) \
    .window(TumblingEventTimeWindows.of(Time.minutes(5))) \
    .aggregate(AverageAggregate(),
               accumulator_type=Types.TUPLE([Types.LONG(), Types.LONG()]),
               output_type=Types.DOUBLE())
    
    

env.execute('kafkaread')
