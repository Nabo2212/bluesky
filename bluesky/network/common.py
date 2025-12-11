import socket
import binascii
import string
# from os import urandom
import random
from enum import Enum, auto
from click import group
import msgpack


# Message headers (first byte): (un)subscribe
MSG_SUBSCRIBE = 1
MSG_UNSUBSCRIBE = 0
# Message headers (second byte): group identifiers
GROUPID_DEFAULT = 0
GROUPID_CLIENT = ord('C')
GROUPID_SIM = ord('S')
GROUPID_NOGROUP = ord('N')
# Connection identifier string length
IDLEN = 6
IDXLEN = 2


class ActionType(Enum):
    ''' Shared state action types. 
    
        An incoming shared state update can be of the following types:
        Append: An item is appended to the state
        Extend: Two or more items are appended to the state
        Delete: One or more items are deleted from the state
        Update: One or more items within the state are updated
        Replace: The full state object is replaced
        Reset: The entire object is reset to its (empty) default
        ActChange: A new active remote is selected
    '''
    Append = b'A'
    Extend = b'E'
    Delete = b'D'
    Update = b'U'
    Replace = b'R'
    Reset = b'X'
    ActChange = b'C'
    NoAction = b''

    @classmethod
    def isaction(cls, data):
        ''' Returns True if passed data is an ActionType '''
        return any([data == a.value for a in cls])


class MessageType(Enum):
    ''' BlueSky network message type indicator. '''
    Unknown = auto()
    Regular = auto()
    SharedState = auto()


def genid(groupid: str|bytes|int='', idlen=IDLEN, idxlen=IDXLEN, seqidx=None):
    ''' Generate a binary identifier string 
    
        The identifier string consists of a group identifier of idlen-1 bytes,
        and ends with a sequence number that is indicated with curidx.

        Arguments:
        - groupid: The group identifier of the generated id can be part of a
          larger group. A group id passed to the function is extended with random
          bytes up to a length of idlen-1. Valid values in each position are all
          possible byte values except the wildcard character '*', which is reserved
          as wildcard padding.

        - idlen: The length in bytes of the generated identifier string

        - idxlen: The length in bytes of the sequence index part of the identifier

        - seqidx: The value of the sequence index part of this identifier.
    '''
    groupid = asbytestr(groupid)
    if len(groupid) >= idlen and seqidx is None:
        # If no sequence index is requested, just return the groupid truncated to idlen
        return groupid[:idlen]
    groupid = groupid[:idlen - idxlen]

    if len(groupid) < idlen - 1:
        # groupid += urandom(idlen - 1 - len(groupid)).replace(b'*', b'_')
        groupid += ''.join(random.choices(_allowed_id_chars, k=idlen - IDXLEN - len(groupid))).encode('charmap')
    return groupid + seqidx2id(seqidx or 0)


def ws_msgid(topic: str|bytes, from_group: int|str|bytes='', to_group: int|str|bytes=''):
    ''' Generate a message identifier for publications to websocket connections.
    
        The message identifier is a msgpack-packed list consisting of the following parts:
        - to_group (destination mask padded to IDLEN with '*' bytes)
        - the publication topic
        - from_group (sender mask for subscriptions, sender id for the actual publications)
        - a trailing None value (to be replaced with the message payload)
    '''
    # TODO: bytes -> str or hex?
    packed = msgpack.packb([from_group, topic, to_group, None])
    if packed is not None:
        return packed[:-1]

def unpack_zmq_msgid(msgid: bytes):
    ''' Unpack a zmq message identifier into its components.
    
        The message identifier is the concatenation of the following parts:
        - to_group (destination mask padded to IDLEN with '*' bytes)
        - the publication topic
        - from_group (sender mask for subscriptions, sender id for the actual publications)

        Returns a tuple of (topic, from_group, to_group).
    '''
    btopic = msgid[IDLEN:-IDLEN].decode()
    bfrom_group = msgid[-IDLEN:]
    bto_group = msgid[:IDLEN]
    return (btopic, bfrom_group, bto_group)

def zmq_msgid(topic: str|bytes, from_group: int|str|bytes='', to_group: int|str|bytes=''):
    ''' Generate a message identifier for publications to ZMQ connections.
    
        The message identifier is the concatenation of the following parts:
        - to_group (destination mask padded to IDLEN with '*' bytes)
        - the publication topic
        - from_group (sender mask for subscriptions, sender id for the actual publications)
    '''
    btopic = asbytestr(topic)
    bto_group = asbytestr(to_group)
    bfrom_group = asbytestr(from_group)
    return bto_group.ljust(IDLEN, b'*') + btopic + bfrom_group


def getseqidxfromid(nodeid: bytes):
    ''' Get the sequence index from a node identifier string. '''
    return int(nodeid[-2:], 16)


def seqid2idx(seqid):
    ''' Transform a hexadecimal bytestring sequence id to a numeric sequence index.
    '''
    return int(seqid, 16)


def seqidx2id(seqidx):
    ''' Transform a numeric sequence index to a hexadecimal bytestring sequence id '''
    return f'{seqidx:02x}'.encode('charmap')


def asbytestr(val:int|str|bytes) -> bytes:
    return chr(val).encode('charmap') if isinstance(val, int) else \
              val.encode('charmap') if isinstance(val, str) else \
              val

def bin2hex(bstr):
    return binascii.hexlify(bstr).decode()


def hex2bin(hstr):
    return binascii.unhexlify(hstr)


def get_ownip():
    ''' Try to determine the IP address of this machine. '''
    try:
        local_addrs = socket.gethostbyname_ex(socket.gethostname())[-1]

        for addr in local_addrs:
            if not addr.startswith('127'):
                return addr
    except:
        pass
    return '127.0.0.1'

_allowed_id_chars = string.ascii_letters + string.digits
