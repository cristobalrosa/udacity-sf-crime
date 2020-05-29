from kafka import KafkaConsumer

KAFKA_BROKERS = "localhost:9092"


class SFPoliceDepartmentCrimeConsumer:
    """Simple kafka consumer. We could have used the kafka consumer directly but this example aims to
    provide a way to customize your consumer."""

    def __init__(self, topic, **kwargs):
        self.consumer = KafkaConsumer(topic, **kwargs)
        self.topic = topic

    def run(self):
        """This method will run forever"""
        for msg in self.consumer:
            self.process_message(msg)

    def process_message(self, message):
        """Process the given message"""
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                             message.offset, message.key,
                                             message.value))


if __name__ == "__main__":
    consumer = SFPoliceDepartmentCrimeConsumer(
        "com.sf.police.crime.v1",
        bootstrap_servers=KAFKA_BROKERS,
        client_id="sf-police-department-consumer",
        auto_offset_reset='earliest'
    )

    consumer.run()
