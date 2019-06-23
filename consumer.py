from confluent_kafka import Consumer
from common import read_config, gen_kafka_config, get_consumer_parser
import json
import logging
import time


logger = logging.getLogger(__name__)


def main(args):
    conf = read_config()

    kafka_config = gen_kafka_config(conf)
    kafka_config['auto.offset.reset'] = args.offset
    kafka_config['group.id'] = args.consumer_id

    if args.debug:
        print("Kafka configuration:")
        print(json.dumps(kafka_config, indent=4))

    consumer = Consumer(kafka_config)
    consumer.subscribe([args.topic])

    while True:
        message = consumer.poll(1)
        print(message)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = get_consumer_parser()
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    main(args)
