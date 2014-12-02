from oiot import Curator, OiotClient
import sys, time, traceback

if __name__ == '__main__':
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
			print('Caught: ' + str(e) + ': ' + traceback.format_exc(sys.exc_info()))
			time.sleep(3)
