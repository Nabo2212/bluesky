''' Network context namespace. 

    Gives access to original message context for connected functions.
'''
from typing import Any
from bluesky.network.common import ActionType


# For shared state messages: the action type
action: ActionType = ActionType.NoAction

# For shared state messages: the action content
action_content: Any = None

# The node id of the sender of the current message
sender_id = ''

# The topic of the current message
topic = ''

# The raw message
msg = b''
