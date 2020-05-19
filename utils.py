from confluent_kafka.admin import AdminClient, NewTopic
import logging

logger = logging.getLogger(__name__)


class KakfaClient:
    def __init__(self, bootstrap_servers):
        self.client = AdminClient({"bootstrap.servers": bootstrap_servers})

    def topic_exists(self, topic_name):
        """Checks if the given topic exists"""
        topic_metadata = self.client.list_topics(timeout=5)
        return topic_metadata.topics.get(topic_name) is not None

    def create_topic(self, topic_name, num_partitions, num_replicas):
        """Creates the producer topic"""
        futures = self.client.create_topics(
            [NewTopic(topic=topic_name, num_partitions=num_partitions, replication_factor=num_replicas)]
        )
        for _, future in futures.items():
            try:
                future.result()
                logger.info(f"kafka topic {topic_name} created")
            except Exception as e:
                logger.info(f"topic creation kafka failed {topic_name} with exception: {e}")