from confluent_kafka import (
    Consumer,
    OFFSET_BEGINNING
)
from common import read_config, gen_kafka_config, get_consumer_parser
import json
import logging


logger = logging.getLogger(__name__)


def main(args):
    def _on_assign(consumer, partitions):
        """
        If force-beginning is True, force Kafka to read all stored messages
        :param consumer:
        :param partitions:
        :return:
        """
        if args.force_beginning:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
        consumer.assign(partitions)

    conf = read_config()

    kafka_config = gen_kafka_config(conf)
    kafka_config['auto.offset.reset'] = args.offset
    kafka_config['group.id'] = args.consumer_id

    if args.debug:
        print("Kafka configuration:")
        print(json.dumps(kafka_config, indent=4))

    consumer = Consumer(kafka_config)
    consumer.subscribe([args.topic], on_assign=_on_assign)

    while True:
        message = consumer.poll(1)
        if message is not None:
            print(message.value().decode('UTF-8'))
            consumer.commit()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    parser = get_consumer_parser()
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)

    main(args)
