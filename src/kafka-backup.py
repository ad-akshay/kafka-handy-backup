#! ../venv/Scripts/python
import argparse, re, signal, threading, time
from dataclasses import asdict
import os
from Storage import Storage
from TopicBackupConsumer import TopicBackupConsumer
import Metadata
from Encoder import AVAILABLE_COMPRESSORS


# Define command line arguments
parser = argparse.ArgumentParser()
subparsers = parser.add_subparsers(dest='command')

# "backup" command parser
p1 = subparsers.add_parser('backup')
p1.add_argument('--topic', '-t', dest='topics', action='append', help='Topics to backup')
p1.add_argument('--topics-regex', type=str, help='Topics to backup')
p1.add_argument('--bootstrap-servers', type=str)
p1.add_argument('--max-chunk-size', type=int, default=1000000, help='Maximum size of chunk (files) in bytes (default = 1Gb)')
p1.add_argument('--directory', type=str, default='backup', help='Output directory/container (default="backup")')
p1.add_argument('--continuous', action='store_true', help='Continuous backup mode')
p1.add_argument('--point-in-time-interval', type=int, default=86400, help='Point in time interval (default: 24h)')
p1.add_argument('--compression', type=str, choices=AVAILABLE_COMPRESSORS, help='Specify compression algorithm for compressing messages')

# "list-topics" command parser
p2 = subparsers.add_parser('list-topics')
p2.add_argument('--bootstrap-servers', type=str)
p2.add_argument('--details', action='store_true', help='Show partition details')

# Parse the input arguments
args = parser.parse_args()

if __name__ == "__main__":

    # Capture system signals to implement graceful exit
    exit_signal = False
    def signal_handler(sig, frame):
        print('received', 'SIGINT' if sig == signal.SIGINT else 'SIGTERM')
        global exit_signal
        exit_signal = True
    signal.signal(signal.SIGINT, signal_handler)    # Ctrl+C
    signal.signal(signal.SIGTERM, signal_handler)   # Termination

    # print(args)

    if args.command in ['list-topics', 'backup']: # Commands that require --bootstrap-servers option
        BOOTSTRAP_SERVERS = args.bootstrap_servers or os.getenv('KAFKA_BOOTSTRAP_SERVERS') or 'localhost:29092'

    if args.command == 'backup':

        if not args.topics and not args.topics_regex:
            print('ERROR: at least one of --topic or --topics-regex must be specified')
            exit()

        metadata = Metadata.read_metadata(BOOTSTRAP_SERVERS)
        existing_topics = metadata.topics

        # Build the list of topics to backup
        topics_to_backup = []
        for topic_name in existing_topics:
            if args.topics is not None and topic_name in args.topics:
                topics_to_backup.append(topic_name)
            elif args.topics_regex is not None and re.match(args.topics_regex, topic_name):
                topics_to_backup.append(topic_name)

        print(f"Found {len(topics_to_backup)} topics to backup:", topics_to_backup)

        if len(topics_to_backup) == 0:
            print('No topic to backup')
            exit()

        # Configure the storage backend and encoding options
        storage = Storage(
            base_path=args.directory,
            max_chunk_size=args.max_chunk_size,
            compression=args.compression
        )

        storage.backup_metadata(metadata)

        # Create the task that backups the topics data
        topic_backup_consumer = TopicBackupConsumer(
            storage=storage,
            bootstrap_servers=BOOTSTRAP_SERVERS
        )

        offsets = {}
        for topic in topics_to_backup:
            offsets[topic] = { p.id: p.maxOffset for p in existing_topics[topic].partitions }
        x = threading.Thread(target=topic_backup_consumer.start, args=(topics_to_backup, offsets, args.continuous))
        x.start()


        # The topic backup task is started, now we loop to wait until it finisheds or an interrupt signal is received
        elapsed_seconds = 0
        while not exit_signal and x.is_alive():
            time.sleep(1) # We need to stay in the main thread for the SIGINT signal to be caught
            
            # In continuous mode, we want to backup the metadata at periodic intervals
            # The metadata backup are the different point-in-time at which we can restore our data
            if args.continuous:
                elapsed_seconds = elapsed_seconds + 1
                if elapsed_seconds >= args.point_in_time_interval:
                    metadata = Metadata.read_metadata(BOOTSTRAP_SERVERS)
                    storage.backup_metadata(metadata)
                    elapsed_seconds = 0
        
        # If we get here, an exit signal was caught or the topic backup task is done

        topic_backup_consumer.stop()


    elif args.command == 'list-topics':
        for t in Metadata.topics_details(BOOTSTRAP_SERVERS).values():
            if args.details:
                print(t.friendly())
            else:
                print(f'- {t.name} ({len(t.partitions)} partitions, {max([x.replicas for x in t.partitions])} replicas)')
    else:
        parser.print_help()