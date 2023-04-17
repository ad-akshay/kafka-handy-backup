

# Few observations about `__consumer_offsets` topic

Kafka uses the `__consumer_offsets` topic to store information about the consumers.

There are two types of messages writtent in this topic:

- offset commit: contains the committed offset of a consumer group for a topic partition
- group metadata: contains info about consumer groups (ex: members and other stuff like that)


## How is the data encoded

- The messages have a key and a value that are binary encoded
- The messages have empty headers
- The two first bytes of the **key** indicate the *version* of the key, which gives the type of message and the version of the key data.
- The two first bytes of the **value** indicate the *version* of the value (data in the value vary depending on the version).
- Numbers are encoded in big endian
- Strings are encoded with their length first: | <string length> (uint16) | <ascii characters> (<string length> bytes) |

The schemas that define the data encoding and versions are defined as json [here](https://github.com/apache/kafka/blob/3.3/core/src/main/resources/common/message).


```py
# Example of key+value of a offset commit message:
 
key = b'\x00\x01\x00\x12kafka-backup-topic\x00\x0ctest-topic-1\x00\x00\x00\x00'

    Key fields are:
        version (uint16) = \x00\x01
        group (string) = \x00\x12 (size) + 'kafka-backup-topic'
        topic (string) = \x00\x0c (size) + 'test-topic-1'
        partition = \x00\x00\x00\x00
 
value = b'\x00\x03\x00\x00\x00\x00\x00\x00\x15\x89\xff\xff\xff\xff\x00\x00\x00\x00\x01\x82\x1c\x1aT9'

    Value fields are:
        version: \x00\x03
        offset (int64): \x00\x00\x00\x00\x00\x00\x15\x89
        leaderEpoch (int32): \xff\xff\xff\xff (-1)
        metadata (string): \x00\x00 (empty string)
        commitTimestamp: \x00\x00\x01\x82\x1c\x1a\x54\x39
```


## Helpful links

- [What key version correspond to which type of message](https://github.com/apache/kafka/blob/77230b567ab51726302466058ca5f5e734e81664/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1139)
- [Encoding of the offset commit value in kafka 3.3](https://github.com/apache/kafka/blob/77230b567ab51726302466058ca5f5e734e81664/core/src/main/scala/kafka/coordinator/group/GroupMetadataManager.scala#L1076)
- [How the data is encoded in offset commit message value](https://github.com/apache/kafka/blob/77230b567ab51726302466058ca5f5e734e81664/core/src/main/resources/common/message/OffsetCommitValue.json#L21)


## How to set the committed offset manually

The [`commit()`](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit) function of the confluent kafka client library can be used to commit arbitrary offsets:

```py
# Using the offsets option, you can commit arbitrary offsets
consumer.commit(offsets=[Topic(topic='test', partition=0, offset=10)])
```

Any positive offset will be committed (negative values are ignored). There do not seem to be any sort of validation on the value appart from being positive (it can even be higher than the max topic partition offset).