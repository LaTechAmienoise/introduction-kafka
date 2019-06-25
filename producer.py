from confluent_kafka import Producer
from common import read_config, gen_kafka_config, get_producer_parser, get_random_user
from schema.marshmallow import UserSchema
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

    schema = None
    if args.schema in ['marshmallow', 'marshmallow-extended']:
        schema = UserSchema()

    i = 0
    while i < args.number:
        if args.schema == 'marshmallow':
            user = get_random_user()
            # dumps a json
            producer.produce(args.topic, schema.dumps(user).data)
        elif args.schema == 'marshmallow-extended':
            data = dict({
                'schema': {
                    'name': schema.name,
                    'version': schema.version,
                },
                'data': schema.dump(get_random_user()).data,
            })
            data = json.dumps(data)
            print(data)
            producer.produce(args.topic, data)
        else:
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
