#! /bin/python3
from typing import Dict, List
from confluent_kafka import Producer, Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED
from confluent_kafka.admin import AdminClient
from time import sleep

from dataclasses import dataclass

@dataclass
class PartitionDetails:
    id: int
    minOffset: int
    maxOffset: int

@dataclass
class TopicDetails:
    name: str
    paritions: List[PartitionDetails]


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


def list_consumer_groups():
    """Return the id of all active consumer groups"""
    group_meta = admin.list_groups(timeout=10) # List all active consumer groups
    return [d.id for d in group_meta]
    # print(f'- id={data.id} broker={data.broker} state={data.state} members={data.members}')


def topics_details():
    """Retreive topics details (paritions, etc.)"""

    topics_details = []
    cluster_meta = admin.list_topics(timeout=5) # Get info for this topic
    for t in cluster_meta.topics.values():
        if t.topic == "__consumer_offsets":
            continue

        partitions = []
        for id in t.partitions:
            # print(p.id, p.leader)
            wm = consumer.get_watermark_offsets(TopicPartition(t.topic, id))
            # print(f'{id} : {wm}')
            partitions.append(PartitionDetails(id, wm[0], wm[1]))
        
        topics_details.append(TopicDetails(t.topic, partitions))

    print(topics_details)


def consumer_group_details():

    consumer_goups = list_consumer_groups()

    print('Consumer group details:')
    for id in ['kafka-backup']: # Consumer group IDs
        c = Consumer({
            'group.id': id,
            'bootstrap.servers': BOOTSTRAP_SERVERS,
        })
        p = c.committed([TopicPartition('test-topic', 0)])
        # print('Comitted:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])

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
    # produceMessages(123)
    # printInfo()
    topics_details()


    print('Closing consumer')
    consumer.close()