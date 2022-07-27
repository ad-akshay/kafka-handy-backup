import hashlib
import os
import subprocess
from typing import Dict, List
from dataclasses import dataclass
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED

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

@dataclass
class PartitionDetails:
    id: int
    minOffset: int
    maxOffset: int
    replicas: int

    def __repr__(self):
        return f'Partition {self.id}: minOffset={self.minOffset} maxOffset={self.maxOffset} replicas={self.replicas} ({self.maxOffset - self.minOffset} messages)'

@dataclass
class TopicDetails:
    name: str
    partitions: List[PartitionDetails]

    def friendly(self):
        text = 'Topic: ' + self.name
        for p in self.partitions:
            text += f'\n  - {str(p)}'

        return text

@dataclass
class ConsumerOffset:
    topic: str
    partition: int
    committedOffset: int

@dataclass
class ConsumerDetails:
    group_id: str
    offsets: List[ConsumerOffset] # [topic][partition]

@dataclass
class Metadata:
    timestamp: int
    consumers: Dict
    topics: Dict


def key_id(key: bytes) -> str:
    """Generate a unique ID for a given key"""
    return hashlib.md5(key).hexdigest()[0:10]


@dataclass
class KafkaMessage:
    topic: str
    value: bytes
    key: bytes
    partition: int
    timestamp: int
    headers: List|Dict
    offset: int


def reset_group_offset(topic, partition, offset, bootstrap_servers, consumer_group):
    """Reset committed offset"""

    # executable = f"/usr/bin/kafka-consumer-groups"
    executable = f"bash -c C:/Users/Karadoc/bin/kafka/bin/kafka-consumer-groups.sh"
    cmd = f"{executable} --help"
    # cmd = f"{executable}\
    #                     --bootstrap-server {bootstrap_servers}\
    #                     --group {consumer_group}\
    #                     --topic {topic}\
    #                     --partition {partition} \
    #                     --reset-offsets --to-earliest\
    #                     --dry-run"

    print(f"Executing {cmd}")

    res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)

    print(res.stdout)
