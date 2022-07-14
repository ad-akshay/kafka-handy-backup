#! ../venv/Scripts/python
import argparse, re, signal
from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from TopicBackupConsumer import TopicBackupConsumer
from utils import ConsumerDetails, ConsumerOffset, PartitionDetails, TopicDetails

BOOTSTRAP_SERVERS = 'localhost:29092'

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


def list_topics():
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

    consumer_goups = list_consumer_groups()
    topics = list_topics()

    details = {}
    for id in consumer_goups: # Consumer group IDs
        c = Consumer({
            'group.id': id,
            'bootstrap.servers': BOOTSTRAP_SERVERS,
        })

        # For each topic, get the committed offset
        for tn in topics.values():
            partition_offsets = c.committed([TopicPartition(tn.name, x.id) for x in tn.partitions])
        
        details[id] = ConsumerDetails(id, [ConsumerOffset(tn.name, p.partition, p.offset) for p in partition_offsets])

    return details



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
        
        existing_topics = list_topics()

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

        # Create the task that backups the topics data
        topic_backup_consumer = TopicBackupConsumer()

        def signal_handler(sig, frame):
            topic_backup_consumer.stop()
        signal.signal(signal.SIGINT, signal_handler)

        topic_backup_consumer.start(topics_to_backup) # Blocks


    elif args.command == 'list-topics':
        for t in list_topics().values():
            print(f'- {t.name} ({len(t.partitions)} partitions)')
    else:
        parser.print_help()