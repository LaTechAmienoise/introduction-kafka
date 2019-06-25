import yaml
import os
import argparse
import random
import string
from schema.marshmallow import User


def read_config(filename=None):
    if filename is None:
        current_dir = os.path.dirname(__file__)
        filename = os.path.join(current_dir, 'config.yaml')

    with open(filename, 'r') as fh:
        config = yaml.safe_load(fh)
    fh.close()
    return config


def gen_kafka_config(config):
    kafka_conf = dict()
    kafka_conf['bootstrap.servers'] = ",".join([f"{x}:{config['kafka']['port']}" for x in config['kafka']['brokers']])

    for k, v in config['kafka']['extra_config'].items():
        kafka_conf[k] = v

    return kafka_conf


def _get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', default='bruno-lta-demo', help='Topic to read or write')
    parser.add_argument('--debug', default=False, action='store_true')
    parser.add_argument('--schema', default=None)

    return parser


def get_producer_parser():
    parser = _get_parser()
    parser.add_argument('--number', default=10, help='Number of message to send')
    return parser


def get_consumer_parser():
    parser = _get_parser()
    parser.add_argument('--consumer-id', default='bruno-test')
    parser.add_argument('--offset', default='latest', help='Offset to begin earliest/latest')
    parser.add_argument('--force-beginning', default=False, action='store_true')
    return parser


def _random_string(length=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def get_random_user():
    user = User(
        id=_random_string(8),
        firstname=_random_string(12),
        lastname=_random_string(16),
    )
    return user
