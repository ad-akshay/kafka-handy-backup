#! /bin/python3
from confluent_kafka import Producer, Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED
from confluent_kafka.admin import AdminClient
from time import sleep

BOOTSTRAP_SERVERS = 'localhost:29092'
CLIENT_ID = 'kafka-backup.py'

producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': CLIENT_ID
})

consumer = Consumer({
    'group.id': 'kafka-backup',
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    # 'auto.offset.reset': 'smallest'
})

admin = AdminClient({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
})


def produceMessages(count = 100):
    for i in range(0, count):
        producer.produce('test-topic', key=f'key{i}', value=f'value{i}')
    producer.flush()

def offsetToStr(offset):
    if offset> 0:
        return str(offset)
    elif offset == OFFSET_STORED:
        return 'STORED'
    elif offset == OFFSET_BEGINNING:
        return 'BEGINNING'
    elif offset == OFFSET_INVALID:
        return 'INVALID'
    elif offset == OFFSET_END:
        return 'END'
    else:
        return 'unknown'

def consumeMessages(count = 5):

    def print_assignment(consumer, partitions):
        print('Assignment:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])
        partitions = consumer.committed([TopicPartition('test-topic', 0)])
        print('Comitted:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])
        
    consumer.subscribe(['test-topic'], on_assign=print_assignment)

    for i in range(0, count):
        msg = consumer.poll(30.0)

        if msg is None:
            print(f'{i} >> Consumer: Timeout polling for message')
        elif msg.error():
            print(f'Error reading message')
        else:
            print(f'{i} >> offset={msg.offset()} length={len(msg)} partition={msg.partition()} ts={msg.timestamp()[1]} value={msg.value()}')

    

def printInfo():
    # meta = consumer.consumer_group_metadata()
    # print(meta)

    md = admin.list_topics(timeout=10)

    print(f'{len(md.topics)} topics:')
    for t in iter(md.topics.values()):
        info = consumer.get_watermark_offsets(TopicPartition(t.topic, 0))
        print(f'- {t} watermark={info}')


if __name__ == "__main__":
    
    # consumeMessages()
    produceMessages(200)
    printInfo()


    print('Closing consumer')
    consumer.close()