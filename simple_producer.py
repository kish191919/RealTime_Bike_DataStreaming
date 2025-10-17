from confluent_kafka import Producer
import sys
import time

BROKER_LIST = 'localhost:9092'

class SimpleProducer:
    def __init__(self, topic, duration=None):
        self.topic = topic
        self.duration = duration if duration is not None else 60
        self.conf = {'bootstrap.servers':BROKER_LIST}

        self.producer = Producer(self.conf)

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' % (msg.topic(), msg.partition(), msg.offset()))

    def produce(self):
        cnt = 0
        while cnt < self.duration:
            try:
                self.producer.produce(
                    topic=self.topic,
                    key = str(cnt),
                    value = f'hello world: {cnt}',
                    on_delivery = self.delivery_callback
                )
            except BufferError:
                sys.stderr.write('%% Local producer queue if full (%d messages awaiting delivery): try again \n' % len(self.producer))

            self.producer.poll(0)
            cnt += 1
            time.sleep(1)

        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush()

if __name__== '__main__':
    simple_producer = SimpleProducer(topic='simpleproduce', duration=60)
    simple_producer.produce()


