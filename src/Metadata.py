"""
Class in charge of reading the metadata (consumer offset, topics details) from a cluster
"""

from dataclasses import asdict
from math import floor
import time
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from utils import ConsumerDetails, ConsumerOffset, MetaData, PartitionDetails, TopicDetails, OFFSET_INVALID


def list_consumer_groups(bootstrap_servers: str):
    """Return the id of all active consumer groups"""
    admin = AdminClient({'bootstrap.servers': bootstrap_servers})
    group_meta = admin.list_groups(timeout=10) # List all active consumer groups
    return [d.id for d in group_meta]


def topics_details(bootstrap_servers: str) -> dict[str, TopicDetails]:
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

        partitions = {}
        for p in t.partitions.values():
            wm = consumer.get_watermark_offsets(TopicPartition(t.topic, p.id))
            partitions[p.id] = PartitionDetails(p.id, wm[0], wm[1], len(p.replicas))
        
        topics_details[t.topic] = TopicDetails(t.topic, partitions)

    consumer.close()

    return topics_details

def consumer_details(bootstrap_servers: str) -> dict[str, ConsumerDetails]:
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
        partition_offsets = []
        for tn in topics.values():
            pa : list[TopicPartition] = c.committed([TopicPartition(tn.name, x.id) for x in tn.partitions.values()])
            partition_offsets.extend(pa)
            # print([(p.topic, p.partition, p.offset) for p in pa])

        c.close()
        
        details[id] = ConsumerDetails(id, [ConsumerOffset(p.topic, p.partition, p.offset) for p in partition_offsets if p.offset != OFFSET_INVALID])

    return details

def read_metadata(bootstrap_servers: str):
    return MetaData(
        timestamp=floor(time.time()),
        consumers=consumer_details(bootstrap_servers),
        topics=topics_details(bootstrap_servers)
    )


