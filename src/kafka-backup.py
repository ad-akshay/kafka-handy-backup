#! ../venv/Scripts/python
import argparse, re, signal, threading, time
from dataclasses import asdict
import os
from Storage import Storage
from TopicBackupConsumer import TopicBackupConsumer
import Metadata


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

# "list-topics" command parser
p2 = subparsers.add_parser('list-topics')
p2.add_argument('--bootstrap-servers', type=str)
p2.add_argument('--details', action='store_true')

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
        for topic in existing_topics:
            if args.topics is not None and topic in args.topics:
                topics_to_backup.append(topic)
            elif args.topics_regex is not None and re.match(args.topics_regex, topic):
                topics_to_backup.append(topic)

        print(f"Found {len(topics_to_backup)} topics to backup:", topics_to_backup)

        if len(topics_to_backup) == 0:
            print('No topic to backup')
            exit()

        storage = Storage(args.directory, args.max_chunk_size)
        storage.backup_metadata(metadata)

        # Create the task that backups the topics data
        topic_backup_consumer = TopicBackupConsumer(
            storage=storage,
            bootstrap_servers=BOOTSTRAP_SERVERS
            )

        x = threading.Thread(target=topic_backup_consumer.start, args=(topics_to_backup,))
        x.start()

        while not exit_signal:
            time.sleep(1) # We need to stay in the main thread for the SIGINT signal to be caught
            # print('is_alive():', x.is_alive())
        
        # If we get here, an exit signal was caught

        topic_backup_consumer.stop()


    elif args.command == 'list-topics':
        for t in Metadata.topics_details(BOOTSTRAP_SERVERS).values():
            if args.details:
                print(t.friendly())
            else:
                print(f'- {t.name} ({len(t.partitions)} partitions, {max([x.replicas for x in t.partitions])} replicas)')
    else:
        parser.print_help()