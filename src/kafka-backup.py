#! ../venv/Scripts/python

# Configure logger module before importing anything else, so that the config applies in other imported modules
import logging

import argparse, re, signal, threading, time, os
from datetime import datetime
from Storage import Storage
from TopicBackupConsumer import KAFKA_BACKUP_CONSUMER_GROUP, TopicBackupConsumer
import Metadata
from Encoder import AVAILABLE_COMPRESSORS, Encoder
from TopicRestorationProducer import TopicRestorationProducer
from utils import TopicDetails, setCommittedOffsets
from confluent_kafka import TopicPartition


# Define command line arguments
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command')

# "backup" command parser
p1 = subparsers.add_parser('backup', help='Backup selected topics from the specified cluster')
p1.add_argument('--topic', '-t', dest='topics', action='append', help='Topics to backup')
p1.add_argument('--topics-regex', type=str, help='Topics to backup')
p1.add_argument('--bootstrap-servers', type=str)
p1.add_argument('--max-chunk-size', type=int, default=1000000, help='Maximum size of chunk (files) in bytes (default = 1Gb)')
p1.add_argument('--directory', type=str, default='kafka-backup-data', help='Output directory/container (default="kafka-backup-data")')
p1.add_argument('--continuous', action='store_true', help='Continuous backup mode')
p1.add_argument('--point-in-time-interval', type=int, default=86400, help='Point in time interval (default: 24h)')
p1.add_argument('--compression', type=str, choices=AVAILABLE_COMPRESSORS, help='Specify compression algorithm for compressing messages')
p1.add_argument('--encryption-key', type=str, help='256 bits encryption key')
p1.add_argument('--swift-url', type=str, help='OpenStack Swift URL. When set, uploads the chunks to OpenStack swift storage.')

# "list-topics" command parser
p2 = subparsers.add_parser('list-topics', help='List topics in the cluster')
p2.add_argument('--bootstrap-servers', type=str)
p2.add_argument('--details', action='store_true', help='Show partition details')

# "restore" command parser
p3 = subparsers.add_parser('restore', help='Restore backed up topics to the cluster')
p3.add_argument('--bootstrap-servers', type=str)
p3.add_argument('--directory', type=str, default='kafka-backup-data', help='Backup directory/container (default="kafka-backup-data")')
p3.add_argument('--topic', '-t', dest='topics', action='append', help='Topics to backup')
p3.add_argument('--topics-regex', type=str, help='Topics to restore')
p3.add_argument('--ignore-partitions', dest='original_partitions', action='store_false', help='Ignore the original message partitions when publishing')
p3.add_argument('--ignore-errors', action='store_true', help='Ignore topics with errors')
p3.add_argument('--ignore-offsets', action='store_true', help='Do not restore the consumer group offsets (default when --ignore-partition is used)')
p3.add_argument('--dry-run', action='store_true', help='Do not actually perform the restoration. Only print the actions that would be performed.')
p3.add_argument('--point-in-time', type=str, help="Manually select a restoration point (use the `backup-info` command to list available options")
p3.add_argument('--encryption-key', dest='encryption_keys', action='append', type=str, help="Key used for decrypting the data. This option can be used multiple times to specify more than one key if multiple keys were used for encryption.")
p3.add_argument('--restore-offsets', action='store_true', help="Restore the consumer offsets")


# "backup-info" command parser
p4 = subparsers.add_parser('backup-info', help='Print information on the backed up info')
p4.add_argument('--limit', type=int, default=10, help='Max number of lines to print')
p4.add_argument('--directory', type=str, default='backup', help='Backup directory/container (default="backup")')

# "reset-cursor"
p5 = subparsers.add_parser('reset-cursor', help='Reset the committed consumer offset of the kafka backup consumer so that new backups will start from the beginning of each topic')
p5.add_argument('--bootstrap-servers', type=str)
p5.add_argument('--confirm', action='store_true', help='Set to cctually execute the command')
p5.add_argument('--topics-regex', type=str, help='Topics to restore')

# Parse the input arguments
args = parser.parse_args()

if __name__ == "__main__":


    log_level = 'debug'

    if log_level == 'debug':
        logging.basicConfig(format='%(levelname)s [%(module)s] %(message)s', level=logging.DEBUG)
    else: # info
        logging.basicConfig(format='%(message)s', level=logging.INFO)

    # Capture system signals to implement graceful exit
    exit_signal = False
    def signal_handler(sig, frame):
        print('received', 'SIGINT' if sig == signal.SIGINT else 'SIGTERM')
        global exit_signal
        exit_signal = True
    signal.signal(signal.SIGINT, signal_handler)    # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)   # Termination

    # print(args)

    if args.command in ['list-topics', 'backup', 'restore', 'reset-cursor']: # Commands that require --bootstrap-servers option
        BOOTSTRAP_SERVERS = args.bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS') or 'localhost:29092'

    if args.command == 'backup':

        if not args.topics and not args.topics_regex:
            print('ERROR: at least one of --topic or --topics-regex must be specified')
            exit()

        encryption_key = None
        if args.encryption_key:
            encryption_key = args.encryption_key.encode()
            
            if len(encryption_key) != 32:
                print('ERROR: Encryption key must be 256 bits (32 bytes)')
                exit()

        restoration_point_metadata = Metadata.read_metadata(BOOTSTRAP_SERVERS)
        existing_topics = restoration_point_metadata.topics

        # Build the list of topics to backup
        topics_to_backup = []
        for topic_name in existing_topics:
            if args.topics is not None and topic_name in args.topics:
                topics_to_backup.append(topic_name)
            elif args.topics_regex is not None and re.match(args.topics_regex, topic_name):
                topics_to_backup.append(topic_name)

        if args.continuous:
            print(f'Starting backup in continuous mode')

        print(f"Found {len(topics_to_backup)} topics to backup:", topics_to_backup)

        if len(topics_to_backup) == 0:
            print('No topic to backup')
            exit()

        # Configure the encoder
        encoder = Encoder(
            compression=args.compression,
        )

        # Configure the storage backend
        storage = Storage(
            base_path=args.directory,
            max_chunk_size=args.max_chunk_size,
            encoder=encoder,
            encryption_key=encryption_key,
            swift_url=args.swift_url
        )

        consumers = []
        for topic in topics_to_backup:
            # Create a TopicBackupConsumer per topic to backup
            consumers.append(TopicBackupConsumer(
                storage,
                BOOTSTRAP_SERVERS,
                topic,
                { p.id: p.maxOffset for p in existing_topics[topic].partitions.values() },
                args.continuous
            ))

        threads : list[threading.Thread] = []
        for c in consumers:
            x = threading.Thread(target=c.start, args=())
            threads.append(x)
            x.start()


        # The topic backup task is started, now we loop to wait until it finisheds or an interrupt signal is received
        elapsed_seconds = 0
        while not exit_signal and x.is_alive():
            time.sleep(1) # We need to stay in the main thread for the SIGINT signal to be caught

            if all([not x.is_alive() for x in threads]):
                break # All backup completed
            
            # In continuous mode, we want to backup the metadata at periodic intervals
            # The metadata backup are the different point-in-time at which we can restore our data
            if args.continuous:
                elapsed_seconds = elapsed_seconds + 1
                if elapsed_seconds >= args.point_in_time_interval:
                    restoration_point_metadata = Metadata.read_metadata(BOOTSTRAP_SERVERS)
                    storage.backup_metadata(restoration_point_metadata)
                    elapsed_seconds = 0
        
        # If we get here, an exit signal was caught or the topic backup task is done

        if exit_signal:
            # An exit signal was caught, we need to stop the remaining active tasks
            for c in consumers:
                c.stop()

        # Only backup the metadata at the end so that the restoration point is available once topics are properly backed up
        storage.backup_metadata(restoration_point_metadata)
        print('Backup complete')

    elif args.command == 'restore':
        if not args.topics and not args.topics_regex:
            print('ERROR: at least one of --topic or --topics-regex must be specified')
            exit()

        # Validate encryption keys
        encryption_keys = []
        if args.encryption_keys is not None:
            for key in args.encryption_keys:
                if len(key) != 32:
                    print(f'ERROR: Encryption key "{key}" must be 256 bits (32 bytes)')
                    exit()
                encryption_keys.append(key.encode())

        # Configure the storage backend
        storage = Storage(
            base_path=args.directory,
            max_chunk_size=10,
            encoder=Encoder(),
            encryption_key=None,
            decryption_keys=encryption_keys
        )

        available_topics = storage.available_topics()

        # Build the list of topics to backup
        topics_to_restore = []
        for topic_name in available_topics:
            if args.topics_regex is not None and re.match(args.topics_regex, topic_name):
                topics_to_restore.append({'source':topic_name})

        if args.topics is not None:
            for t in args.topics:
                if '/' in t:
                    (source_topic, destination_topic) = t.split('/')
                    if source_topic in available_topics:
                        topics_to_restore.append({
                            'source': source_topic,
                            'destination': destination_topic
                        })
                elif t in available_topics:
                    topics_to_restore.append({ 'source': t })
                else:
                    topics_to_restore.append({
                        'source': t,
                        'error': 'Topic not found in backup'
                    })

        restoration_point_metadata = storage.get_metadata(args.point_in_time)
        cluster_topic_details = Metadata.topics_details(BOOTSTRAP_SERVERS)

        if restoration_point_metadata is None:
            print(f'ERROR: Could not find metadata for restoration point {args.point_in_time}')
            exit()

        print(f'Restoration point: {restoration_point_metadata.timestamp}')
        # print(f'<source-topic>:<partition> -> <destination-topic>:<partition>')

        # Get more info on the topic
        for t in topics_to_restore:
            t['destination'] = t.get('destination', t['source']) # Set destination to source if not specified

            d: TopicDetails = cluster_topic_details.get(t['destination']) # Info on the destination topic

            # Check if destination topic exist
            if d is None:
                t['error'] = f'Destination topic "{t["destination"]}" does not exist on the cluster'
                continue

            # Check if topics is empty
            if not all([p.maxOffset == p.minOffset for p in d.partitions.values()]):
                t['error'] = f'Destination topic {t["destination"]} contains messages. Must be empty to restore.'
                continue

            if t['source'] not in restoration_point_metadata.topics:
                t['error'] = 'Topic not found in backup metadata'
                continue

            # Cluster partitions
            t['partitions'] = restoration_point_metadata.topics[t['source']].partitions

            if args.original_partitions and len(d.partitions) < len(t['partitions']):
                t['error'] = f'Topic in cluster has a lower number of partitions ({len(d.partitions)}) than the backup topic ({len(t["partitions"])}): cannot restore original partitions for this topic.'
        
        # Print restoration summary
        topics_to_restore.sort(key=lambda x: x['source'])
        errors = [x for x in filter(lambda x: 'error' in x, topics_to_restore)]
        topics_to_restore = [x for x in filter(lambda x: 'error' not in x, topics_to_restore)] # Keep topics without errors

        if len(errors) > 0:
            print('ERRORS:')
        for t in errors:
            print(f"- {t['source']} : {t['error']}")

        if len(topics_to_restore) > 0:
            print(f'Topics/partitions to restore:')
        for t in topics_to_restore:
            for p in t['partitions']:
                print(f"- {t['source']}/{p.id} ({p.minOffset}, {p.maxOffset}) -> {t['destination']}/{ f'{p.id} ({d.partitions[p.id].maxOffset}, {d.partitions[p.id].maxOffset + p.maxOffset - p.minOffset})' if args.original_partitions else 'any'}")

        if len(errors) > 0 and not args.ignore_errors:
            print('Aborted: there are some errors. Use --ignore-errors if you wish to continue ignoring the topics with errors.')
            exit()

        if len(errors) > 0 and args.ignore_errors:
            print('WARNING: Topics with errors will be ignored')

        if len(topics_to_restore) == 0:
            print('WARNING: There are not topics to restore')
            exit()

        if args.dry_run:
            print('Dry run completed. Remove --dry-run to actually restore the topics.')
            exit()

        # Create producers that will restore the topic-partitions
        producers = []
        for t in topics_to_restore:
            for p in t['partitions']:
                producers.append(TopicRestorationProducer(
                    src_topic=t['source'],
                    partition=p.id,
                    dst_topic=t['destination'],
                    original_partitions=args.original_partitions,
                    minOffset=p.minOffset,
                    maxOffset=p.maxOffset,
                    storage=storage,
                    bootstrap_server=BOOTSTRAP_SERVERS,
                    restore_consumer_offset=args.restore_offsets,
                    consumer_offsets=restoration_point_metadata.consumers
                ))

        # Start all the producers in different threads, then wait for them to finish
        threads = []
        for k in producers:
            x = threading.Thread(target=k.start)
            x.start()
            threads.append(x)
        
        # Wait for all threads to be finished or interrupt signal to be received
        while not exit_signal:
            time.sleep(1) # We need to stay in the main thread for the SIGINT signal to be caught

            if all([ not x.is_alive() for x in threads ]):
                break # All threads are finished
        
        if exit_signal:
            for p in producers:
                p.cancel()


    elif args.command == 'list-topics':
        print('\nTopic list:')
        for t in Metadata.topics_details(BOOTSTRAP_SERVERS).values():
            if args.details:
                print(t.friendly())
            else:
                print(f'- {t.name} ({len(t.partitions)} partitions, {max([x.replicas for x in t.partitions.values()])} replicas)')

        # print('\nConsumers info:')
        # for c in Metadata.consumer_details(BOOTSTRAP_SERVERS).values():
        #     print(c.friendly())

    elif args.command == 'backup-info':
        # Configure the storage backend
        storage = Storage(
            base_path=args.directory,
            max_chunk_size=100, # Whatever value, we're not writing anyway
            encoder=Encoder(),
            encryption_key=None
        )

        print('\nAvailable restoration points:')
        points = storage.available_restoration_points(args.limit)
        for i in range(0, len(points)):
            epoch = points[i]
            dt = datetime.fromtimestamp(epoch)
            diff = datetime.now() - dt
            if diff.days == 0:
                diff_string = str(round(diff.seconds/60/60, 1)) + ' hours'
            else:
                diff_string = str(diff.days) + ' days'

            print(f' {i}) {epoch} : {dt.strftime("%Y-%m-%d %H:%M:%S")} ({diff_string} ago)')

        print('\nBacked up topics:')
        partitions_to_restore = storage.available_topics()
        partitions_to_restore.sort()
        for t in partitions_to_restore:
            print(f'- {t}')

    elif args.command == 'reset-cursor':

        # topic_names = storage.list_available_topics()
        group = KAFKA_BACKUP_CONSUMER_GROUP # The group used by the TopicBackupConsumer
        meta = Metadata.consumer_details(BOOTSTRAP_SERVERS)
        cd = meta.get(group)

        if cd is None:
            print(f'No offsets committed on the cluster (never backed up)')
            exit()
        
        print(f"Will reset the cursor for {len(cd.offsets)} topic/partitions")
        # for c in cd.offsets:
        #     print(f"- {c.topic}/{c.partition}")
        
        # Set committed offset to 0 for all topics that have a committed offset for the backup group id
        setCommittedOffsets(group, BOOTSTRAP_SERVERS, [TopicPartition(topic=c.topic, partition=c.partition, offset=0) for c in cd.offsets])

        if not args.confirm:
            print('The --confirm option must be set to actually execute the command')
            exit()

        print('Done. Your next backup will start from the beginning of the topics.')

    else:
        parser.print_help()