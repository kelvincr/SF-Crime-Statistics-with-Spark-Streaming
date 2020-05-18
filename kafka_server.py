import producer_server

BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"


def run_kafka_server():
    input_file = "police-department-calls-for-service.json"
    topic_name = "org.sanfranciscopolice.stats.calls"
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=topic_name,
        bootstrap_servers=f"{BROKER_URL}",
        client_id=f"producer.{topic_name}"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
