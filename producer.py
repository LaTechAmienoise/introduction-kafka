from confluent_kafka import Producer
from common import read_config, gen_kafka_config, get_producer_parser
import json
import logging
import time


logger = logging.getLogger(__name__)


def main(args):
    conf = read_config()

    kafka_config = gen_kafka_config(conf)

    if args.debug:
        print("Kafka configuration:")
        print(json.dumps(kafka_config, indent=4))

    producer = Producer(kafka_config)

    i = 0
    while i < args.number:
        producer.produce(args.topic, f"{i}")
        logger.info("Message %s sent", i)

        i += 1
        time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = get_producer_parser()
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    main(args)
