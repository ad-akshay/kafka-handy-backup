"""
Class in charge of backing up the metadata (consumer offset, topics details) at points in time
"""

from dataclasses import asdict
import time
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from utils import ConsumerDetails, ConsumerOffset, Metadata, PartitionDetails, TopicDetails


def list_consumer_groups(bootstrap_servers):
    """Return the id of all active consumer groups"""
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    group_meta = admin.list_groups(timeout=10) # List all active consumer groups
    return [d.id for d in group_meta]


def topics_details(bootstrap_servers):
    """Retreive topics details (paritions, offsets, etc.)"""
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    consumer = Consumer({
        'group.id': 'kafka-backup',
        'bootstrap.servers': bootstrap_servers,
        'auto.offset.reset': 'smallest'
    })

    topics_details = {}
    cluster_meta = admin.list_topics(timeout=5) # Get info for this topic
    for t in cluster_meta.topics.values():
        if t.topic == "__consumer_offsets":
            continue

        partitions = []
        for p in t.partitions.values():
            wm = consumer.get_watermark_offsets(TopicPartition(t.topic, p.id))
            # print(f'{id} : {wm}')
            partitions.append(PartitionDetails(p.id, wm[0], wm[1], len(p.replicas)))
        
        topics_details[t.topic] = TopicDetails(t.topic, partitions)

    consumer.close()

    return topics_details

def consumer_details(bootstrap_servers):
    """Retreive consumer details"""

    consumer_goups = list_consumer_groups(bootstrap_servers)
    topics = topics_details(bootstrap_servers)

    details = {}
    for id in consumer_goups: # Consumer group IDs
        c = Consumer({
            'group.id': id,
            'bootstrap.servers': bootstrap_servers,
        })

        # For each topic, get the committed offset
        for tn in topics.values():
            partition_offsets = c.committed([TopicPartition(tn.name, x.id) for x in tn.partitions])

        c.close()
        
        details[id] = ConsumerDetails(id, [ConsumerOffset(tn.name, p.partition, p.offset) for p in partition_offsets])

    return details

def read_metadata(bootstrap_servers):
    return Metadata(
        timestamp=time.time(),
        consumers=consumer_details(),
        topics=topics_details()
    )

def backup_metadata(bootstrap_servers):
    """Make a capture of the metadata and store it to a file"""

    meta = read_metadata(bootstrap_servers)
    print(asdict(meta))

    return meta

