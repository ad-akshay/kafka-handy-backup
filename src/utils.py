import hashlib
from dataclasses import dataclass
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, OFFSET_END, OFFSET_INVALID, OFFSET_STORED

def offsetToStr(offset):
    if offset >= 0:
        return str(offset)
    elif offset == OFFSET_STORED:
        return f'STORED({OFFSET_STORED})'
    elif offset == OFFSET_BEGINNING:
        return f'BEGINNING({OFFSET_BEGINNING})'
    elif offset == OFFSET_INVALID:
        return f'INVALID({OFFSET_INVALID})'
    elif offset == OFFSET_END:
        return f'END({OFFSET_END})'
    else:
        return f'unknown({offset})'

@dataclass
class PartitionDetails:
    id: int
    minOffset: int
    maxOffset: int
    replicas: int

    # def __repr__(self):
    #     return f'Partition {self.id}: minOffset={self.minOffset} maxOffset={self.maxOffset} replicas={self.replicas} ({self.maxOffset - self.minOffset} messages)'

@dataclass
class TopicDetails:
    name: str
    partitions: dict[int, PartitionDetails]

    def fromObj(obj):
        return TopicDetails(
            obj['name'],
            { p['id'] : PartitionDetails(**p) for p in obj['partitions'].values() }
        )

    def friendly(self):
        text = 'Topic: ' + self.name
        for p in self.partitions.values():
            text += f'\n  - {str(p)}'

        return text

@dataclass
class ConsumerOffset:
    topic: str
    partition: int
    committedOffset: int

    # def __repr__(self):
    #     return f'{self.topic}/{self.partition} committedOffset={self.committedOffset}'

@dataclass
class ConsumerDetails:
    group_id: str
    offsets: list[ConsumerOffset]

    def friendly(self):
        text = f'- {self.group_id}'
        for offset in self.offsets:
            text += f'\n  - {offset}'
        return text

@dataclass
class MetaData:
    timestamp: int
    consumers: dict[str, ConsumerOffset]    # key = <group-id>
    topics: dict[str, TopicDetails]         # key = <topic-name>

    def fromObj(obj):
        # print({ x['group_id']: [ConsumerOffset(**o) for o in x['offsets']] for x in obj['consumers'].values() })
        return MetaData(
            obj['timestamp'],
            { x['group_id']: [ConsumerOffset(**o) for o in x['offsets']] for x in obj['consumers'].values() },
            { x['name']: TopicDetails.fromObj(x) for x in obj['topics'].values() }
        )


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
    headers: list|dict
    offset: int


# def reset_group_offset(topic, partition, offset, bootstrap_servers, consumer_group):
#     """Reset committed offset"""

#     # executable = f"/usr/bin/kafka-consumer-groups"
#     executable = f"bash -c C:/Users/Karadoc/bin/kafka/bin/kafka-consumer-groups.sh"
#     cmd = f"{executable} --help"
#     # cmd = f"{executable}\
#     #                     --bootstrap-server {bootstrap_servers}\
#     #                     --group {consumer_group}\
#     #                     --topic {topic}\
#     #                     --partition {partition} \
#     #                     --reset-offsets --to-earliest\
#     #                     --dry-run"

#     print(f"Executing {cmd}")

#     res = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)

#     print(res.stdout)


def setCommittedOffset(group: str, topic: str, partition: int, offset: int, bootstrap_server: str):
    """Set the committed offset of a consumer group for a topic-partition"""
    c = Consumer({
        'group.id': group,
        'bootstrap.servers': bootstrap_server,
        'enable.auto.commit': False
    })
    c.commit(offsets=[TopicPartition(topic=topic, partition=partition, offset=offset)])
    c.close()

def setCommittedOffsets(group: str, bootstrap_server: str, offsets: list[TopicPartition]):
    """Set the committed offset of a consumer group for a topic-partition"""
    c = Consumer({
        'group.id': group,
        'bootstrap.servers': bootstrap_server,
        'enable.auto.commit': False
    })
    c.commit(offsets=offsets)
    c.close()


