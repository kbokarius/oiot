from porc import Client
from . import _locks_collection, _get_lock_collection_key, CollectionKeyIsLocked

# TODO: Address race condition between checking for a lock and executing the 
# operation. Consider checking for a lock before and after the operation.

class OiotClient(Client):
	def __init__(self, api_key, custom_url = None, 
				 use_async = False, **kwargs):
		super(self.__class__, self).__init__(api_key, custom_url = None, 
											 use_async = False, **kwargs)

	def _raise_if_locked(self, collection, key):
		response = super(self.__class__, self).get(_locks_collection, _get_lock_collection_key(collection, key))
		if response.status_code != 404:
			raise CollectionKeyIsLocked

	def put(self, collection, key, value, ref = None, raise_if_locked = True):
		if raise_if_locked:	
			self._raise_if_locked(collection, key)
		return super(self.__class__, self).put(collection, key, value, ref)	

	def get(self, collection, key, ref = None, raise_if_locked = True):
		if raise_if_locked:	
			self._raise_if_locked(collection, key)
		return super(self.__class__, self).get(collection, key, ref)	

	def delete(self, collection, key = None, ref = None, raise_if_locked = True):
		if key and raise_if_locked:
			self._raise_if_locked(collection, key)
		return super(self.__class__, self).delete(collection, key, ref)
