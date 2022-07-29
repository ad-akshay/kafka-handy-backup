#
#   Functions for encoding/decoding messages in the __consumer_offsets topic
#


from struct import *
import re
from typing import Dict, List

def includes_version(version: int, pattern: str):
    """Return true if the given `version` is included in the given version pattern"""
    if re.match(r'\d+\-\d+', pattern):
        min, max = pattern.split('-')
        return version >= int(min) and version <= int(max)
    elif re.match(r'\d+\+', pattern):
        min = int(pattern.replace('+',''))
        return version >= min
    elif re.match(r'\d+', pattern):
        return version == int(pattern)
    else:
        print(f'EXCEPTION: Unknown pattern for versions string "{pattern}"')
        return False


OFFSET_COMMIT_KEY_SCHEMA = [
    # https://github.com/apache/kafka/blob/3.3/core/src/main/resources/common/message/OffsetCommitKey.json
    { "name": "group", "type": "string", "versions": "0-1" },
    { "name": "topic", "type": "string", "versions": "0-1" },
    { "name": "partition", "type": "int32", "versions": "0-1" }
]

OFFSET_COMMIT_VALUE_SCHEMA = [
    # https://github.com/apache/kafka/blob/3.3/core/src/main/resources/common/message/OffsetCommitValue.json
    { "name": "offset", "type": "int64", "versions": "0+" },
    { "name": "leaderEpoch", "type": "int32", "versions": "3+", "default": -1, "ignorable": True},
    { "name": "metadata", "type": "string", "versions": "0+" },
    { "name": "commitTimestamp", "type": "int64", "versions": "0+" },
    { "name": "expireTimestamp", "type": "int64", "versions": "1", "default": -1, "ignorable": True}
]

GROUP_METADATA_KEY_SCHEMA = [
    # https://github.com/apache/kafka/blob/3.3/core/src/main/resources/common/message/GroupMetadataKey.json
    { "name": "group", "type": "string", "versions": "2" }
  ]

# Map which schema to use for decoding the key of a __consumer_offsets message
MESSAGE_TYPE_SCHEMAS = {
    # There are two message types (offset commit and group metadata)
    0 : OFFSET_COMMIT_KEY_SCHEMA,
    1 : OFFSET_COMMIT_KEY_SCHEMA,
    2 : GROUP_METADATA_KEY_SCHEMA
}

# How to unpack the different values
# Numbers are stored in big endian
UNPACK_MAP = {
    'int16': { 'format':'>h', 'size': 2 },
    'uint16': { 'format':'>H', 'size': 2 },
    'int32': { 'format':'>i', 'size': 4 },
    'uint32': { 'format':'>I', 'size': 4 },
    'int64': { 'format':'>q', 'size': 8 },
    'uint64': { 'format':'>Q', 'size': 8 },
}

def decodeFromSchema(version: int, buffer: bytes, fields: List[Dict]) -> Dict:
    """Decode a buffer from the given schema (fields) following the specified version"""
    obj = { "version": version }
    cursor = 0

    for field in fields:
        
        if not includes_version(version, field['versions']):
            continue

        if field['type'] == 'string': # Special case for string
            # String is encoded with the size first : | <string_size> (uint16) | <string_chars> (<string_size> bytes) |
            str_size = unpack('>H', buffer[cursor:cursor+2])[0]; cursor += 2
            obj[field['name']] = buffer[cursor:cursor+str_size].decode(); cursor += str_size
        else: # Other types can be handled by unpack()
            l = UNPACK_MAP.get(field['type'])
            if not l:
                print(f'EXCEPTION: {field["type"]} is not defined in the UNPACK_MAP')
                return None

            if len(buffer) < (cursor + l['size']):
                print(f'ERROR: Not enough bytes to decode type {field["type"]}')
                return None

            obj[field['name']] = unpack(l['format'], buffer[cursor: cursor+l['size']])[0]
            cursor += l['size']

    return obj


def encodeFromSchema(obj: Dict, fields: List[Dict]) -> bytes:
    """Encode a buffer from the given schema (fields) following the specified version"""
    version = obj['version']
    buffer = b''

    for field in fields:
        if not includes_version(version, field['versions']):
            continue

        if field['name'] not in obj:
            if field.get('default') is not None:
                obj[field['name']] = field.get('default')
            else:
                print(f"ERROR: missing {field['name']} in provided object")
                return None

        if field['type'] == 'string': # Special case for string
            # String is encoded with the size first : | <string_size> (uint16) | <string_chars> (<string_size> bytes) |
            buffer += pack('>H', len(obj[field['name']]))
            buffer += obj[field['name']].encode()
        else: # Other types can be handled by unpack()
            l = UNPACK_MAP.get(field['type'])
            if not l:
                print(f'EXCEPTION: {field["type"]} is not defined in the UNPACK_MAP')
                return None

            buffer += pack(l['format'], obj[field['name']])

    return buffer

def decodeKey(key: bytes) -> Dict:
    """Decode the key of a message from the __consumer_offsets topic"""
    version = unpack('>H', key[0:2])[0]
    schema = MESSAGE_TYPE_SCHEMAS[version]
    # print('keyVersion:', version)
    # print('schema:', schema)
    return decodeFromSchema(version, key[2:], schema)

def encodeKey(key: Dict) -> bytes:
    """Encode the key to bytes"""
    schema = MESSAGE_TYPE_SCHEMAS[key['version']]
    buffer = pack('>H', key['version'])
    buffer += encodeFromSchema(key, schema)
    return buffer

def decodeOffsetValue(buffer: bytes):
    """Decode the value of a offset commit message from the __consumer_offsets topic"""
    version = unpack('>H', buffer[0:2])[0]
    return decodeFromSchema(version, buffer[2:], OFFSET_COMMIT_VALUE_SCHEMA)

def encodeOffsetValue(value: Dict) -> bytes:
    """Encode the value of a offset commit message"""
    buffer = pack('>H', value['version'])
    buffer += encodeFromSchema(value, OFFSET_COMMIT_VALUE_SCHEMA)
    return buffer

def decodeMetadataValue(value: bytes):
    return None # Not implemented (not useful for this project, plus not sure how to decode this part)

def decodeMessage(key: bytes, value: bytes):
    """Decode the key and value of a message in the __consumer_offsets topic"""
    obj = {}
    obj['key'] = decodeKey(key)

    if obj['key']['version'] in [0,1]:
        obj['type'] = 'OFFSET_COMMIT'
        obj['value'] = decodeOffsetValue(value)
    elif obj['key']['version'] in [2]:
        obj['type'] = 'GROUP_METADATA'
        obj['value'] = decodeMetadataValue(value)

    return obj

