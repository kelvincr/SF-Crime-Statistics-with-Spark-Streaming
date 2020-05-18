from kafka import KafkaProducer
from confluent_kafka.admin import AdminClient, NewTopic
import logging
import json
import time
import datetime

logger = logging.getLogger(__name__)
logging.getLogger("kafka.conn").setLevel('ERROR')


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, num_partitions, num_replicas, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        client = AdminClient({"bootstrap.servers": kwargs.get('bootstrap_servers')})
        exists = self.topic_exists(client)
        if exists is False:
            self.create_topic(client, self.topic)

    def read_file(self):
        """
        Read json file of calls.
        """
        with open(self.input_file, 'r') as f:
            data = json.load(f)
        return data

    def generate_data(self):
        records = self.read_file()
        logger.info(f"started producing data for kafka topic {self.topic}")
        for i in records:
            message = self.dict_to_binary(i)
            self.send(self.topic, message)
            curr_time = datetime.datetime.utcnow().replace(microsecond=0)
            logger.info(f"{len(message)} bytes sent at {curr_time.isoformat()}")
            time.sleep(1)

    @staticmethod
    def dict_to_binary(json_dict):
        return json.dumps(json_dict).encode("utf-8")

    def topic_exists(self, client):
        """Checks if the given topic exists"""
        topic_metadata = client.list_topics(timeout=5)
        return topic_metadata.topics.get(self.topic) is not None

    def create_topic(self, client, topic_name):
        """Creates the producer topic"""
        futures = client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=self.num_partitions, replication_factor=self.num_replicas)]
        )
        for _, future in futures.items():
            try:
                future.result()
                logger.info(f"kafka topic {self.topic} created")
            except Exception as e:
                logger.info(f"topic creation kafka failed {self.topic} with exception: {e}")