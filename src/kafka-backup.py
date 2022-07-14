#! ../venv/Scripts/python
import re
from confluent_kafka import Producer, Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED
from confluent_kafka.admin import AdminClient

from utils import ConsumerDetails, ConsumerOffset, PartitionDetails, TopicDetails



TOPIC_NAME = 'test-topic-abc'

BOOTSTRAP_SERVERS = 'localhost:29092'
CLIENT_ID = 'kafka-backup.py'

producer = Producer({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'client.id': CLIENT_ID
})

consumer = Consumer({
    'group.id': 'kafka-backup',
    'bootstrap.servers': BOOTSTRAP_SERVERS,
    'auto.offset.reset': 'smallest'
})

admin = AdminClient({
    'bootstrap.servers': BOOTSTRAP_SERVERS,
})


def list_consumer_groups():
    """Return the id of all active consumer groups"""
    group_meta = admin.list_groups(timeout=10) # List all active consumer groups
    return [d.id for d in group_meta]


def topics_details():
    """Retreive topics details (paritions, offsets, etc.)"""

    topics_details = {}
    cluster_meta = admin.list_topics(timeout=5) # Get info for this topic
    for t in cluster_meta.topics.values():
        if t.topic == "__consumer_offsets":
            continue

        partitions = []
        for id in t.partitions:
            wm = consumer.get_watermark_offsets(TopicPartition(t.topic, id))
            # print(f'{id} : {wm}')
            partitions.append(PartitionDetails(id, wm[0], wm[1]))
        
        topics_details[t.topic] = TopicDetails(t.topic, partitions)

    return topics_details


def consumer_group_details():
    """Retreive consumer details"""

    # consumer_goups = list_consumer_groups()
    topics = topics_details()

    details = {}
    for id in ['kafka-backup']: # Consumer group IDs
        c = Consumer({
            'group.id': id,
            'bootstrap.servers': BOOTSTRAP_SERVERS,
        })

        # For each topic, get the committed offset
        for tn in topics.values():
            partition_offsets = c.committed([TopicPartition(tn.name, x.id) for x in tn.partitions])
        
        details[id] = ConsumerDetails(id, [ConsumerOffset(tn.name, p.partition, p.offset) for p in partition_offsets])

    return details


def produceMessages(count = 100):
    print(f'Producing {count} messages in {TOPIC_NAME}')
    for i in range(0, count):
        producer.produce(TOPIC_NAME, key=f'key{i}', value=f'value{i}')
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

def consumeOffsetTopic(count = 5):
    c = Consumer({
        'group.id': 'kafka-backup',
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'auto.offset.reset': 'smallest'
    })

    c.subscribe(['__consumer_offsets'])

    # c.seek(TopicPartition('__consumer_offsets')
    for i in range(0, count):
        msg = c.poll(10)

        if msg is None:
            print(f'{i} >> Consumer: Timeout polling for message')
        elif msg.error():
            print(f'Error reading message')
        else:
            print(f'{i} ({msg.partition()}:{msg.offset()}) >> ts={msg.timestamp()[1]} key={msg.key()} value={msg.value()[0:50]}')

    c.close()


def consumeMessages(count = 1):

    def print_assignment(consumer, partitions):
        print('Assignment:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])
        partitions = consumer.committed([TopicPartition(TOPIC_NAME, 0)])
        print('Comitted:', [f'topic={p.topic} offset={offsetToStr(p.offset)} partition={p.partition}' for p in partitions])
        
    consumer.subscribe([TOPIC_NAME], on_assign=print_assignment)

    for i in range(0, count):
        msg = consumer.poll(10.0)

        if msg is None:
            print(f'{i} >> Consumer: Timeout polling for message')
        elif msg.error():
            print(f'Error reading message {msg.error()}')
        else:
            print(f'{i} ({msg.partition()}:{msg.offset()}) >> offset={msg.offset()} length={len(msg)} partition={msg.partition()} ts={msg.timestamp()[1]} value={msg.value()}')

    consumer.commit()


import argparse

parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command')

p1 = subparsers.add_parser('backup')
p1.add_argument('--topic', '-t', dest='topics', action='append', help='Topics to backup')
p1.add_argument('--topics-regex', type=str, help='Topics to backup')
# p1.add_argument('--bootstrap-servers', type=str, required=True)

p2 = subparsers.add_parser('list-topics')

args = parser.parse_args()

if __name__ == "__main__":

    print(args)
    if args.command == 'backup':
        if not args.topics and not args.topics_regex:
            print('ERROR: at least one of --topic or --topics-regex must be specified')
            exit()
        
        existing_topics = topics_details()

        # Build the list of topics to backup
        topics_to_backup = []
        for topic in existing_topics:
            if args.topics is not None and topic in args.topics:
                topics_to_backup.append(topic)
            elif args.topics_regex is not None and re.match(args.topics_regex, topic):
                topics_to_backup.append(topic)

        print(f"Found {len(topics_to_backup)} topics to backup:", topics_to_backup)
        
        if len(topics_to_backup) == 0:
            print('No topic to backup')
            exit()

        # Create a

    elif args.command == 'list-topics':
        for t in topics_details().values():
            print(f'- {t.name} ({len(t.partitions)} partitions)')
    else:
        parser.print_help()
    
    # consumeMessages()
    # consumeOffsetTopic()
    # produceMessages(123)
    # printInfo()
    # topics = topics_details()
    # consumers = consumer_group_details()

    # print(topics)
    # print(consumers)

    # print('Closing consumer')
    # consumer.close()