import producer_server

KAFKA_BROKERS = "localhost:9092"


def run_kafka_server():
	# TODO get the json file path
    input_file = "police-department-calls-for-service.json"

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic="udacity.sf.police.crime.v2",
        bootstrap_servers=KAFKA_BROKERS,
        client_id="sf-police-department"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
