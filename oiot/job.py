from . import _locks_collection, _jobs_collection, _get_lock_collection_key, _generate_key, _Lock, _JournalItem, _Encoder, JobIsCompleted
from datetime import datetime
import json

# TODO: Try to inherit from porc.Client and refactor accordingly.
class Job:
	def __init__(self, client):
		self._job_id = _generate_key()
		self._client = client
		self._locks = []
		self._journal = []
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
				return lock
		lock = _Lock()
		lock.job_id = self._job_id
		lock.timestamp = datetime.utcnow()
		lock.collection = collection
		lock.key = key
		lock_response = self._client.put(_locks_collection, _get_lock_collection_key(collection, key), json.loads(json.dumps(vars(lock), cls=_Encoder)), None, False)
		lock_response.raise_for_status()
		self._locks.append(lock)
		lock.lock_ref = lock_response.ref
		return lock

	def _add_journal_item(self, collection, key, value):
		journal_item = _JournalItem()
		journal_item.timestamp = datetime.utcnow()
		journal_item.collection = collection
		journal_item.key = key
		self._journal.append(journal_item)
		job_response = self._client.put(_jobs_collection, self._job_id, json.loads(json.dumps({'items: ': self._journal}, cls=_Encoder)), None, False)
		job_response.raise_for_status()
		return journal_item
		
	def post(self, collection, value):
		self._verify_job_is_active()
		key = _generate_key()
		return self.put(collection, key, value)

	# TODO: Catch all exceptions - if one is caught, roll-back, and raise a wrapped exception.
	def put(self, collection, key, value, ref = None):	
		self._verify_job_is_active()
		lock = self._lock(collection, key, ref)
		# If ref was passed, ensure that the value has not changed - if ref was not passed, retrieve the current ref and store it.
		journal_item = self._add_journal_item(collection, key, value)
		response = self._client.get(collection, key, ref, False)
		# Indicates a new record will be created.
		if response.status_code == 404: 
			pass
		# Indicates an existing record will be updated so store the original ref and value.
		elif response.status_code == 200: 
			journal_item.original_value = response.json
			journal_item.original_ref = response.ref
		else:
			response.raise_for_status()
		response = self._client.put(collection, key, value, ref, False)
		response.raise_for_status()
		# Store the new ref and value.
		journal_item.new_value = value
		journal_item.new_ref = response.ref
		return response

	def complete(self):
		for lock in self._locks:
			response = self._client.delete(_locks_collection, _get_lock_collection_key(lock.collection, lock.key), lock.lock_ref, False)
			response.raise_for_status()
		self._locks = []
		response = self._client.delete(_jobs_collection, self._job_id, None, False)
		response.raise_for_status()
		self._journal = []
		self.is_completed = True
