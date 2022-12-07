from kafka import KafkaProducer
from kafka.errors import KafkaError
import json
import uuid

servers = ['my-broker:9092']
topic = 'my-topic'
message = {
    'identifier': str(uuid.uuid4()),
    'apiId':'6141e1c70a8ddc0013300bcd',
    'date':'2022-07-12T00:00:00Z',
    'enableAutoForward':True,
    'requestedBy':'me@email.com.br'
}

print(message)
# produce json messages
producer = KafkaProducer(bootstrap_servers=servers, value_serializer=lambda m: json.dumps(m).encode('ascii'))

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    log.error('I am an errback', exc_info=excp)
    # handle exception

# produce asynchronously with callbacks
producer.send(topic, message).add_callback(on_send_success).add_errback(on_send_error)

# block until all async messages are sent
producer.flush()

# configure multiple retries
producer = KafkaProducer(retries=0)