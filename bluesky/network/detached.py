""" Node encapsulates the sim process, and manages process I/O. """
import os
from collections.abc import Collection
import bluesky as bs
from bluesky import stack
from bluesky.core.base import Base


class Node(Base):
    def __init__(self, *args):
        super().__init__()
        self.node_id = ''
        self.server_id = ''
        self.act_id = None

    def connect(self, hostname=None, recv_port=None, send_port=None, protocol='tcp'):
        ''' Connect node to the BlueSky server. This does nothing in detached mode. '''
        pass
    
    def close(self):
        pass

    def update(self):
        pass

    def receive(self, timeout=0):
        pass

    def send(self, topic: str, data: str|Collection='', to_group: str=''):
        pass

    def subscribe(self, topic: str, from_group: str='', to_group: str='', actonly=False):
        pass

    def unsubscribe(self, topic: str, from_group: str='', to_group: str='') -> None:
        pass

    def _subscribe(self, topic: str, from_group: str='', to_group: str='', actonly=False) -> None:
        pass

    def _unsubscribe(self, topic: str, from_group: str='', to_group: str='') -> None:
        pass

    def addnodes(self, count: int=1, *node_ids):
        pass

    @stack.command(name='QUIT', annotations='', aliases=('CLOSE', 'END', 'EXIT', 'Q', 'STOP'))
    def quit(self):
        ''' Quit the simulation process. '''
        bs.sim.quit()
