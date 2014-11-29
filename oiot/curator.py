from porc import Client
from . import _locks_collection, _get_lock_collection_key, CollectionKeyIsLocked

class Curator(Client):
	def __init__(self, client):
		self._client = client

	def run(self):
		pass

