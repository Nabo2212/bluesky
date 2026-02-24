import numpy as np
import msgpack

# def encode_ndarray(o):
#     '''Msgpack encoder for numpy arrays.'''
#     if isinstance(o, np.ndarray):
#         return {'np_pckd': True,
#                 'type': o.dtype.str,
#                 'shape': o.shape,
#                 'data': o.tobytes()}
#     return o

# def decode_ndarray(o):
#     '''Msgpack decoder for numpy arrays.'''
#     if o.get('np_pckd'):
#         return np.frombuffer(o['data'], dtype=np.dtype(o['type'])).reshape(o['shape'])
#     return o

def encode_json(obj) -> str:
    if isinstance(obj, np.ndarray):
        return str(obj)
    raise TypeError


def encode_ext(obj):
    if isinstance(obj, np.ndarray):
        return msgpack.ExtType(42, msgpack.packb([obj.dtype.str, obj.shape, obj.tobytes()]))
    raise TypeError("Unknown type: %r" % (obj,))


def ext_hook(code, data):
    if code == 42:
        dtype, shape, ndbytes = msgpack.unpackb(data)
        return np.frombuffer(ndbytes, dtype=np.dtype(dtype)).reshape(shape)

    return msgpack.ExtType(code, data)
