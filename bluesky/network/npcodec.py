import numpy as np

def encode_ndarray(o):
    '''Msgpack encoder for numpy arrays.'''
    if isinstance(o, np.ndarray):
        return {'np_pckd': True,
                'type': o.dtype.str,
                'shape': o.shape,
                'data': o.tobytes()}
    return o

def decode_ndarray(o):
    '''Msgpack decoder for numpy arrays.'''
    if o.get('np_pckd'):
        return np.frombuffer(o['data'], dtype=np.dtype(o['type'])).reshape(o['shape'])
    return o
