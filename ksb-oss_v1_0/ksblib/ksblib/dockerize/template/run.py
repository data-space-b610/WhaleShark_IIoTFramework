import json
import tornado.web
import tornado.ioloop

import tornado.web
import tornado.ioloop

import numpy as np

# To enable to run this script either in terminal (so in Docker too), or
# in PyCharm (development purpose).
try:
    from modules.base import main_func
except ImportError:
    from .modules.base import main_func
else:
    from .modules.base import main_func


class DockerizeServer(tornado.web.RequestHandler):
    def post(self):
        """Get request and call pre-defined functions."""

        # Read input data.
        inputs = json.loads(self.request.body.decode('utf-8'))

        # Get output data by running main_func.
        outputs = main_func(inputs)
        if type(outputs) is not dict:
            self.write({'Error': 'Returned data type is not "dict"'})

        # Write output data.
        # Convert numpy array to plain list.
        plain_outputs = {}

        for key, val in outputs.items():
            if type(outputs[key]) is np.ndarray:
                # plain_outputs[key] = str(outputs[key].tolist())
                plain_outputs[key] = outputs[key].tolist()
            else:
                # plain_outputs[key] = str(outputs[key])
                plain_outputs[key] = outputs[key]
        # self.write(str(plain_outputs))
        self.write(plain_outputs)


def main(port: int=8080):
    """
    Run Tornado web application.

    Args:
        script (str): Python script to run.
        port (int): Tornado web server port.
            If used, a next port in sequence will be used.
    """

    app = tornado.web.Application([(r'/', DockerizeServer)])
    app.listen(port)

    # Start application.
    tornado.ioloop.IOLoop.instance().start()


if __name__ == '__main__':
    # Run main function.
    main()
