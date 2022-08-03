
# Handy Backup and Restore for Apache Kafka

Once upon a time, I was looking for a tool to backup my kafka data and didn't find one that had all the features I wanted, so I wrote my own. The end.

Handy Kafka Backup is a CLI tool for easily backing up selected kafka topics to the file system or an object storage backend.

## Features

- Select which topics to backup
- Incremental backup
- Run as a job or in continuous backup mode
- Multiple storage backends: local file system and object storage (OpenStack Swift)
- Automatically backup and optionally restore consumer offsets
- Optional data encryption (AES256)
- Optionnal data compression
- Designed to be horizontally scalable (not tested)

## Installation

Various ways to go:

- Clone this repo and run `python -u src/kafka-backup.py` script directly.
- Use the docker image : `docker pull totalus/kafka-handy-backup` and `docker run -it --entrypoint //bin/bash totalus/kafka-handy-backup` (to get a shell in the container, then `python3 kafka-backup.py ...`).

## Usage

`python kafka-backup.py <command> [options]`

Available commands:

- `version`     : Print the tool version
- `list-topics` : List the topics available in the Kafka cluster
- `backup-info` : Print information about the backed up topics and restoration points
- `backup`      : Backup the selected topics
- `restore`     : Restore the selected topics
- `reset-cursor`: Reset the backup cursor to the beginning (will allow to start the next backup of topics from the lower offset of the topic)


### `list-topics` command

You can list the topics available on your cluster:

```bash
# List topics present in the cluster (my-kafka-cluster:9092)
./kafka-backup.py list-topics --bootstrap-servers my-kafka-cluster:9092

# You can also specify the bootstrap servers through the environment
export KAFKA_BOOTSTRAP_SERVERS=my-kafka-cluster:9092
./kafka-backup.py list-topics # Same result
```

### `backup` command

Specify the topics to backup by their name:

```bash
# Backup my-topic-1 and my-topic-2
./kafka-backup.py backup --topic my-topic-1 --topic my-topic-2
```

Specify the topics to backup with a regex pattern:

```bash
# Backup all topics that start with `abc`
./kafka-backup.py backup --topics-regex "abc.*"

# Backup all topics
./kafka-backup.py backup --topics-regex ".*"
```

To encrypt the backed up data, specify an encryption key (must be 32 bytes long):

```bash
# Backup topic my-topic-1 with encryption
./kafka-backup.py backup --topic my-topic-1 --encryption-key 0123456789abcdefghijklmnopqrstuv
```

By default, the backup data is saved on the local file system in the `kafka-backup-data` folder. The target directory can be changed with the `--directory` option.


**Continuous mode**

By default, the backup command will first capture the current topics max offsets and then backup the messages up to this offset (even if new messages came in during the backup process). This ensures that the backup will not run indefinitely.

In continuous mode however, the backup process will run indefinitely, backing up the messages as they come in and saving some restoration points at periodic intervals (configurable with `--point-in-time-interval`).


**Using object storage**

Currently only OpenStack Swift storage backend is supported (throught this [object storage client](https://github.com/Totalus/object-storage-client)).

To use OpenStack Swift as storage backend :
- Set the storage backend with the `--swift-url` option
- Provide the [required](https://github.com/Totalus/object-storage-client#openstack-swift) credentials through environment variables.

Note that the local file system will still be used to save data chunks before uploading them to the objects storage and removed from the file system. The name of the container where data will be stored is the same as the target directory on the local file system, that can be specified with `--directory`.

### `backup-info` command

When backing up, restoration points are created. The restoration points contain information about the state of the topics and consumer groups in the cluster at specific moments in time. That allows to restore the topics exactly as they were at that point in time. The `backup-info` command shows the available restoration points and the backed up topics.

Note that the `--directory` and `--swift-url` options must be the same as the ones used when running the `backup` command as it indicates where the backup resides.

```bash
# Example for a backup on the file system
./kafka-backup.py --directory my-backup-directory

# Will output:
#
#   Available restoration points:
#   0) 1659481509 : 2022-08-02 19:05:09 (2.5 hours ago)
#   1) 1659371338 : 2022-08-01 16:28:58 (24 hours ago)
#
#   Backed up topics:
#   - my-first-topic
#   - my-second-topic
#
```

### `restore` command

The restore command restores selected topics (that were backed up) to a target cluster.

```sh
# Don't forget to set your target cluster
export KAFKA_BOOTSTRAP_SERVERS=my-kafka-cluster:9092

# Restore topic-1 to the cluster
./kafka-backup.py --topic topic-1 --directory my-backup-directory

# You can also restore a topic to a different topic name with the --topic option.
# Ex: Restore the messages of topic-1 into topic-1-restored
./kafka-backup.py --topic topic-1/topic-1-restored
```

By default, the latest restoration point is used, but you pass the timestamp of the restoration point to use to  the `--restoration-point` option to use an older restoration point.

```sh
# Restore all topics from a specified restoration point
./kafka-backup.py restore --topics-regex ".*" --restoration-point 1659371338
```

If the backup was encrypted, you need to specify the encryption key used.

```sh
# Restore all topics from an encrypted backup, also restore the consumer offsets
./kafka-backup.py restore --topics-regex ".*" --encryption-key 0123456789abcdefghijklmnopqrstuv --restore-offsets
```

If you changed the encryption key of the backup along the way, you will have older chunks that are encrypted with one key and newer chunks encrypted with a different key. You can specify the `--encryption-key` multiple times to specify more than one key to use for decryption. The tool will automatically select the right key for decrypting a chunk.

Requirements for restoring:
- Destination topics must exist on the cluster.
- Destination topics must have a number of partition that is equal or higher than the backed up source topic.
- Destination topics must be empty.
- No producer should be publishing to the destination topic during restoration.
- No consumer should be consuming from the destination topic during restoration if the `--restore-offsets` option is used.

### Command line options

| Option                   | Applies to (command)         | Description                                                                                                                      |
|--------------------------|------------------------------|----------------------------------------------------------------------------------------------------------------------------------|
| --verbose                | (all)                        | Increase log level to print debug information                                                                                    |
| --bootstrap-servers      | backup, restore, list-topics | Kafka bootstrap servers (can also be set through KAFKA_BOOTSTRAP_SERVERS environement variable)                                  |
| --continuous             | backup                       | Continuous backup mode                                                                                                           |
| --topic, -t              | backup, restore              | Name of the topic to backup or restore                                                                                           |
| --topics-regex           | backup, restore              | Regex pattern to select the topic(s) to backup or restore                                                                        |
| --max-chunk-size         | backup                       | Maximum size of backup data chunks (files) in bytes (default = 1Gb)                                                              |
| --point-in-time-interval | backup                       | Use with `--continuous`. Defines the interval of time (in seconds) between two restoration point (default: 24h)                  |
| --compression            | backup                       | Specify compression algorithm for compressing messages (run the `backup` command with `--help` for available options)            |
| --directory              | backup, restore, backup-info | Output directory/container name (default="kafka-backup-data")                                                                    |
| --encryption-key         | backup, restore              | 256 bits encryption key                                                                                                          |
| --swift-url              | backup, restore, backup-info | OpenStack Swift URL                                                                                                              |
| --ignore-partitions      | restore                      | Ignore the original message partitions when restoring the messages                                                               |
| --ignore-errors          | restore                      | Ignore topics with errors                                                                                                        |
| --dry-run                | restore                      | Do not actually perform the restoration. Only print the actions that would be performed.                                         |
| --restoration-point      | restore                      | Manually select a restoration point (use the `backup-info` command to list available options                                     |
| --restore-offsets        | restore                      | Restore the consumer offsets of the restored topics                                                                              |
| --limit                  | backup-info                  | Max number of lines to print                                                                                                     |
| --confirm                | reset-cursor                 | Reset the committed consumer offset of the kafka backup consumer so that new backups will start from the beginning of each topic |
| --details                | list-topics                  | Also print partition details for each topic                                                                                      |

For more details, use `./kafka-backup.py --help` or `./kafka-backup.py <command> --help`.

<!-- ## Under the hood

Once a chunk reaches the `--max-chunk-size`, a new chunk is created.
To backup the topics, Kafka Handy Backup creates consumers that subscribe to the topics to backup and reads the messages. The messages read are encoded in CBOR and stored in files (chunks) on the file system, in the specified `--directory`. Chunks are stored in `<backup-directory>/topics/<topic-name>/<partition>/<first-msg-offset>_<first-msg-timestamp>` and the restoration points are stored in `<backup-directory>/metadata/<epoch-seconds>`.

When object storage is used, the chunks are first written to disk, then when the chunk is closed, it is uploaded to the object storage and deleted from the file system. The amount of disk space required with object storage can be calculated by : `disk space = <max chunk size> x <number of topic-partitions to backup>`.

Each chunk has an unencrypted header that stores a few information such as: the compression algorithme, an encryption key id, the type of message encoding, the encryption initialization vector. This allows to properly decrypt and decode the chunks.

Since kafka is an append-only system, it is easy and logical to make incremental backups. By using consumers to read the topics, we automatically implement an incremental backup: only the new messages that have not been read by the backup tool are read and appended to the backup data. -->

## Security (encryption)

AES265 in CTR mode is used for encryption. The initialization vector is different for each chunk, randomly generated and stored in the chunk's header. This provides a strong encryption (as far as I know).

There is currently no data integrity mechanism implemented.

Even though encryption is implemented, not all of the data is or can be encrypted for practical reasons.

What is encrypted:

- The messages (content, length, keys and headers)

What is NOT encrypted:

- Topic names and partitions
- Topic offsets
- Consumer groups data (group name, committed offsets, etc.)

## Known issues and limitations

There are a few behavior that have not been optimized, mostly because they apply in special case scenarios that are not so often encountered, but I thought good to list them here, in case it applies to you.

- New topics created after backup is started that match a topic to backup are not detected. This is an issue when working in continuous mode and new topics that match the given `--topics-regex` pattern are created after the backup process is running.
- The total disk space required for the process to run, when running with object storage backend, is around `max-chunk-size x number-of-topics-partitions`. If you have a lot of topics, that could be considerably high. Adding a `--task-limit` option to define the max number of backup tasks running at the same time would allow to limit the disk space. At the moment, only the `--max-chunk-size` option can be set to lower value to reduce required disk space.
- If multiple instances are ran at the same time, multiple restoration points will be created with similar timestamps (each instance creates a restoration point regardless of if one already exists).
- The tool is not currently optimized performance wise. Here are a a few things that could be improved:
    - The file uploading to object storage is done synchronously (the backup for a topic stops until the chunk is uploaded).
    - Only one thread is used per topic (regardless of the number of partitions) so the partitions are not processed in parallel.
- There is not retention period on the backed up messages. All the messages backed up are kept indefinitely (chunks are not deleted), even if the messages are deleted on kafka.
- **Horizontal scalability** : Since the partitions use distinct output streams to write to the storage backend, it should be possible to run multiple instances (`backup` command) in parallel without conflicting with each other. This will create multiple consumers subscribing to the same topic, thus distributing the load accross instances. This has not been tested however.


