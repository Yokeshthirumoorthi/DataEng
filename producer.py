#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Produce messages to Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import random

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Producer instance
    producer = Producer({
        'bootstrap.servers': conf['bootstrap.servers'],
        'sasl.mechanisms': conf['sasl.mechanisms'],
        'security.protocol': conf['security.protocol'],
        'sasl.username': conf['sasl.username'],
        'sasl.password': conf['sasl.password'],
        'transactional.id': 'python-tran-id-1'
    })

    producer.init_transactions()
    # Create topic if needed
    ccloud_lib.create_topic(conf, topic)

    delivered_records = 0

    # Optional per-message on_delivery handler (triggered by poll() or flush())
    # when a message has been successfully delivered or
    # permanently failed delivery (after retries).
    def acked(err, msg):
        global delivered_records
        """Delivery report handler called on
        successful or failed delivery of message
        """
        if err is not None:
            print("Failed to deliver message: {}".format(err))
        else:
            delivered_records += 1
            print("Produced record to topic {} partition [{}] @ offset {}"
                  .format(msg.topic(), msg.partition(), msg.offset()))

    # if given an input of, say, 0.7 will return True with a 70% probability and false with a 30% probability
    def decision(probability):
        return random.random() < probability

    # for n in range(100):
    #     record_key = "alice"
    #     record_value = json.dumps({'count': n})
    #     print("Producing record: {}\t{}".format(record_key, record_value))
    #     producer.produce(topic, key=record_key,
    #                      value=record_value, on_delivery=acked)
    #     # p.poll() serves delivery reports (on_delivery)
    #     # from previous produce() calls.
    #     producer.poll(0)

    with open('bcsample.json') as f:
        # return JSON object as a dictionary
        bcsample_data = json.load(f)

    for bc_data in bcsample_data:
        # record_key = "breadcrumb"
        # Choose a random number between 1 and 5 for each record’s key
        record_key = str(random.randint(1, 5))
        record_value = json.dumps(bc_data)
        print("Producing record key: {}".format(record_key))
        producer.begin_transaction()
        producer.produce(topic, key=record_key,
                         value=record_value, on_delivery=acked)
        # p.poll() serves delivery reports (on_delivery)
        # from previous produce() calls.
        producer.poll(2000)

        # CShoose True/False randomly with equal probability
        if decision(0.5):
            print("Commiting record key: {}".format(record_key))
            producer.commit_transaction()
        else:
            producer.abort_transaction()

    producer.flush()

    print("{} messages were produced to topic {}!".format(delivered_records, topic))
