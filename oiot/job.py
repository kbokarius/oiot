from . import _locks_collection, _jobs_collection, _get_lock_collection_key, _generate_key, _Lock, _Encoder, JobIsCompleted
from datetime import datetime
import json

# TODO: Try to inherit from porc.Client and refactor accordingly.
class Job:
	def __init__(self, client):
		self._job_id = _generate_key()
		self._client = client
		self._locks = []
		self._items = []
		self.is_completed = False
		self.is_rolled_back = False

	def _verify_job_is_active(self):
		if self.is_completed:
			raise JobIsCompleted
		if self.is_rolled_back:
			raise JobIsRolledBack

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
		lock_response = self._client.put(_locks_collection, _get_lock_collection_key(collection, key), json.loads(json.dumps(vars(lock), cls=_Encoder)), False)
		lock_response.raise_for_status()
		lock.lock_ref = lock_response.ref
		return lock

	def post(self, collection, value):
		self._verify_job_is_active()
		key = _generate_key()
		return self.put(collection, key, value)

	# TODO: Catch all exceptions - if one is caught, roll-back, and raise a wrapped exception.
	# TODO: Provide a way to pass in the original-ref?
	def put(self, collection, key, value, ref = None):	
		self._verify_job_is_active()
		lock = self._lock(collection, key, ref)
		response = self._client.put(collection, key, value, ref, False)
		response.raise_for_status()
		# If ref was passed, ensure that the value has not changed - if ref was not passed, retrieve the current ref and store it.
		response = self._client.get(collection, key, ref, False)
		response.raise_for_status()
		lock.ref = response.ref
		return response

	def complete(self):
		for lock in self._locks:
			response = self._client.delete(_locks_collection, _get_lock_collection_key(lock.collection, lock.key), lock.lock_ref, False)
			response.raise_for_status()
		# TODO: Delete journal.
		self._locks = []
		self.is_completed = True
