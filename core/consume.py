# encoding: utf-8
# author TurboChang

import time
from confluent_kafka import Consumer, TopicPartition, KafkaException, KafkaError

class KafkaConsumer:

    def __init__(self, topic, begin_time, end_time):
        self.topic = topic
        self.begin_time = begin_time
        self.end_time = end_time
        self.consumer = Consumer({
            'bootstrap.servers': "59.110.219.31:9092",
            'group.id': "groupid",
            'auto.offset.reset': "earliest",
        })
        self.timeout = 10
        self.consumer_lists = []

    @staticmethod
    def __str_to_timestamp(str_time, format_type='%Y-%m-%d %H:%M:%S'):
        time_array = time.strptime(str_time, format_type)
        return int(time.mktime(time_array)) * 1000

    def __timestamp_to_offset(self):
        begin = self.begin_time if isinstance(self.begin_time, int) else self.__str_to_timestamp(self.begin_time)
        end = self.end_time if isinstance(self.end_time, int) else self.__str_to_timestamp(self.end_time)

        begin_partition_list = list(map(lambda p: TopicPartition(self.topic, 0, int(begin)), range(0, 1)))
        end_partition_list = list(map(lambda p: TopicPartition(self.topic, 0, int(end)), range(0, 1)))

        begin_offsets = self.consumer.offsets_for_times(begin_partition_list, timeout=self.timeout)[0].offset
        end_offsets = self.consumer.offsets_for_times(end_partition_list, timeout=self.timeout)[0].offset
        return begin_offsets, end_offsets

    def consume_kafka(self):
        start = self.__timestamp_to_offset()[0]
        end = self.__timestamp_to_offset()[1]
        self.consumer.assign([TopicPartition(topic=topic, partition=0, offset=start)])

        no_msg_counter = 0
        while True:
            msg = self.consumer.poll(self.timeout)
            if msg:
                if msg.offset() >= end:
                        break
                else:
                    dat = {
                        'msg_val': msg.value().decode("utf-8"),
                        'msg_partition': msg.partition(),
                        'msg_topic': msg.topic()
                    }
                    self.consumer_lists.append(dat)
                    no_msg_counter += 1
        self.consumer.close()
        return self.consumer_lists

    def run(self):
        print(self.__timestamp_to_offset())
        print(type(self.consume_kafka()[0]))
        # return self.consume_kafka()

if __name__ == '__main__':
    topic = "dp_agent_1_DP_TEST_T1"
    begin_time = "2021-08-18 17:00:00"
    end_time = "2021-08-19 10:30:00"
    f = KafkaConsumer(topic, begin_time, end_time)
    g = f.run()
    print(g)