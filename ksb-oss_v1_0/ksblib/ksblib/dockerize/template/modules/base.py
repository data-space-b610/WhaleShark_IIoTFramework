# NOTE: this file is only for test and development purpose.
# This file WILL NOT be copied to a custom docker image.
# User must provide this function independently and separately.


# After load the built docker image, you can query it something like this:
# curl -d '{"input": "Hello"}' http://localhost:8080
import numpy as np


def main_func(x):
    data = x['input']

    return {'output': np.array([data, data]), 'message': True}
