"""
    run_curator
    ~~~~~~~~~
    A convenience script for running the Curator as a service.
    :copyright: (c) 2014 by Konstantin Bokarius.
    :license: MIT, see LICENSE for more details.
"""
from oiot import Curator, OiotClient
import sys, time, traceback

if __name__ == '__main__':
    """
    Instantiate a Curator class instance and run it. If an exception
    is caught then log it, sleep a few seconds, and continue running.
    """
    api_key = sys.argv[1]
    if api_key is None:
        print('Error: specify the API key to use.')
    while (True):
        try:
            client = OiotClient(api_key)
            client.ping().raise_for_status()
            curator = Curator(client)
            curator.run()
        except Exception as e:
            # TODO: Log exception.
            print('Caught: ' + traceback.format_exc())
            time.sleep(3)
