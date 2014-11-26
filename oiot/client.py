from porc import Client

# Inherit from the porc.Client class and override methods that should check for an existing lock prior to executing.
class OiotClient(Client):
    def __init__(self, api_key, custom_url=None, use_async=False, **kwargs):
        super(OiotClient, self).__init__(api_key, custom_url=None, use_async=False, **kwargs)

	def _raise_if_locked(collection, key):
		response = super(OiotClient, self).get(_locks_collection, _get_lock_collection_key(collection, key))
		if response.status_code != 404:
			raise CollectionKeyIsLocked

	def put(self, collection, key, value, ref = None):	
		_raise_if_locked(collection, key)
		return super(OiotClient, self).put(collection, key, value, ref)	

	def get(self, collection, key, ref=None):
		_raise_if_locked(collection, key)
		return super(OiotClient, self).get(collection, key, ref)	

	def delete(self, collection, key=None, ref=None):
		if key:
			_raise_if_locked(collection, key)
		return super().delete(collection, key, ref)
