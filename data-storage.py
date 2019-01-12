import logging
import argparse
import json
import atexit

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from cassandra.cluster import Cluster

# - config logger
logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-producer')

# - TRACE, DEBUG, INFO, WARNING, ERROR
logger.setLevel(logging.DEBUG)

topic_name = 'stock_analyzer'
kafka_broker = '127.0.0.1:9092'
keyspace = 'stock'
data_table = ''
cassandra_broker = '127.0.0.1:9042'

def persist_data(stock_data, canssandra_session):
  # logger.debug('Start to persist data to cassandra %s', stock_data)
  # TODO: parse data and complete code.
  statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', '%f') % (data_table, symbol, )"
  canssandra_session.execute(statement)
  logger.info('Persisted data into cassandra for symbol: %s, price %f, trade time %s' % (symbol, price, tradetime))

def shutdown_hook(consumer, session):
  consumer.close()
  logger.info('Kafka consumer closed')
  session.shutdown()
  logger.info('Casandra')

if __name__ == '__main__':
  parser = argparse.ArgumentParser()
  parser.add_argument('topic_name', help = 'the kafka topic')
  parser.add_argument('kafka_broker', help = 'the location of the kafka broker')
  parser.add_argument('keyspace', help = 'the keyspace to be used')
  parser.add_argument('data_table', help = 'data table to be used')
  parser.add_argument('cassandra_broker', help = 'the location of the cassandra cluster')

  args = parser.parse_args()
  topic_name = args.topic_name
  kafka_broker = args.kafka_broker
  keyspace = args.keyspace
  data_table = args.data_table
  cassandra_broker = args.cassandra_broker

  # - set up a Kafka Consumer.
  consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_broker)

  # - set up a Cassandra session.
  canssandra_cluster = Cluster(contact_points=cassandra_broker.split(','))
  session = canssandra_cluster.connect(keyspace)
  atexit.register(shutdown_hook, consumer, session)
  for msg in consumer:
    persist_data(msg.value, session)