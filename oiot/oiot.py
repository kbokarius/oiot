import os, sys

import json, random, string
from datetime import datetime
from porc import Client
from enum import Enum

_locks_collection = 'oiot-locks'
_jobs_collection = 'oiot-jobs'

def _generate_key():
	return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))	

def _get_lock_collection_key(collection_to_lock, key_to_lock):
	return collection_to_lock + "-" + key_to_lock

class Encoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, datetime):
			return obj.isoformat()
		return json.JSONEncoder.default(self, obj)

class CollectionKeyLocked(Exception):
	pass

class _Lock:
	job_id = None
	timestamp = None
	collection = None
	key = None
	ref = None
	lock_ref = None

class JobItem:
	job_item_type = None
	collection = None
	key = None
	value = None
	original_ref = None
	response = None
	is_completed = False

# Inherit from the porc.Client class and override methods that should check for an existing lock prior to executing.
class OiotClient(Client):
    def __init__(self, api_key, custom_url=None, use_async=False, **kwargs):
        super(OiotClient, self).__init__(api_key, custom_url=None, use_async=False, **kwargs)

	def _raise_if_locked(collection, key):
		response = super(OiotClient, self).get(_locks_collection, _get_lock_collection_key(collection, key))
		if response.status_code != 404:
			raise CollectionKeyLocked

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

# TODO: Try to inherit from porc.Client and refactor accordingly.
class Job:
	def __init__(self, client):
		self._job_id = _generate_key()
		self._client = client
		self._locks = []
		self._items = []

	def _lock(self, collection, key, ref = None):
		for lock in self._locks:
			if lock.collection == collection and lock.key == key:
				return
		lock = _Lock()
		lock.job_id = self._job_id
		lock.timestamp = datetime.utcnow()
		lock.collection = collection
		lock.key = key
		self._locks.append(lock)
		lock_response = self._client.put(_locks_collection, _get_lock_collection_key(collection, key), json.loads(json.dumps(vars(lock), cls=Encoder)), False)
		lock_response.raise_for_status()
		lock.lock_ref = lock_response.ref
		return lock

	def post(self, collection, value):
		key = _generate_key()
		return self.put(collection, key, value)

	# TODO: Catch all exceptions - if one is caught, roll-back, and raise a wrapped exception.
	# TODO: Provide a way to pass in the original-ref?
	def put(self, collection, key, value, ref = None):	
		lock = self._lock(collection, key, ref)
		response = self._client.put(collection, key, value, ref)
		response.raise_for_status()
		# If ref was passed, ensure that the value has not changed - if ref was not passed, retrieve the current ref and store it.
		response = self._client.get(collection, key, ref)
		response.raise_for_status()
		lock.ref = response.ref
		return response

	def complete(self):
		for lock in self._locks:
			response = self._client.delete(_locks_collection, _get_lock_collection_key(lock.collection, lock.key), lock.lock_ref)
			response.raise_for_status()
		self._locks = []
