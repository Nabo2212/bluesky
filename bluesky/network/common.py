import socket
import string
import random
from enum import Enum, auto
import msgpack


# Message headers (first byte): (un)subscribe
MSG_SUBSCRIBE = 1
MSG_UNSUBSCRIBE = 0
# Message headers (second byte): group identifiers
GROUPID_DEFAULT = '_'
GROUPID_CLIENT = 'C'
GROUPID_SIM = 'S'
GROUPID_NOGROUP = 'N'
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
    Append = 'A'
    Extend = 'E'
    Delete = 'D'
    Update = 'U'
    Replace = 'R'
    Reset = 'X'
    ActChange = 'C'
    NoAction = ''

    @classmethod
    def isaction(cls, data):
        ''' Returns True if passed data is an ActionType '''
        return any([data == a.value for a in cls])


class MessageType(Enum):
    ''' BlueSky network message type indicator. '''
    Unknown = auto()
    Regular = auto()
    SharedState = auto()


def genid(groupid: str='', idlen=IDLEN, idxlen=IDXLEN, seqidx=None) -> str:
    ''' Generate an identifier string 
    
        The identifier string consists of a group identifier of idlen-idxlen characters,
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
    if len(groupid) >= idlen and seqidx is None:
        # If no sequence index is requested, just return the groupid truncated to idlen
        return groupid[:idlen]
    groupid = groupid[:idlen - idxlen]

    # If there is room, add random characters
    if len(groupid) < idlen - 1:
        groupid += ''.join(random.choices(_allowed_id_chars, k=idlen - IDXLEN - len(groupid)))

    # Finally add the hex-encoded sequence number and return
    return groupid + f'{seqidx or 0:0{idxlen}x}'


def ws_msgid(topic: str, from_group: str='', to_group: str='') -> bytes:
    ''' Generate a message identifier for publications to websocket connections.
    
        The message identifier is a msgpack-packed list consisting of the following parts:
        - to_group (destination mask padded to IDLEN with '*')
        - the publication topic
        - from_group (sender mask for subscriptions, sender id for the actual publications)
        - a trailing None value (to be replaced with the message payload)
    '''
    packed = msgpack.packb([to_group, topic, from_group, None])
    if packed is not None:
        return packed[:-1]
    return b''


def unpack_zmq_msgid(msgid: bytes) -> tuple[str, str, str]:
    ''' Unpack a zmq message identifier into its components.
    
        The message identifier is the concatenation of the following parts:
        - to_group (destination mask padded to IDLEN with '*' bytes)
        - the publication topic
        - from_group (sender mask for subscriptions, sender id for the actual publications)

        Returns a tuple of (topic, from_group, to_group).
    '''
    smsgid = msgid.decode()
    topic = smsgid[IDLEN:-IDLEN]
    from_group = smsgid[-IDLEN:]
    to_group = smsgid[:IDLEN]
    return (topic, from_group, to_group)


def zmq_msgid(topic: str, from_group: str='', to_group: str='') -> bytes:
    ''' Generate a binary message identifier for publications to ZMQ connections.
    
        The message identifier is the concatenation of the following parts:
        - to_group (destination mask padded to IDLEN with '*')
        - the publication topic
        - from_group (sender mask for subscriptions, sender id for the actual publications)
    '''
    return (to_group.ljust(IDLEN, '*') + topic + from_group).encode('charmap')


def getseqidxfromid(nodeid: str) -> int:
    ''' Get the sequence index from a node identifier string. '''
    return int(nodeid[-2:], 16)


def seqid2idx(seqid: str) -> int:
    ''' Transform a hexadecimal sequence id string to a numeric sequence index.
    '''
    return int(seqid, 16)


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
