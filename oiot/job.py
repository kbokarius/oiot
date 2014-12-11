import sys, traceback
from . import _locks_collection, _jobs_collection, _get_lock_collection_key, \
		_generate_key, _Lock, _JournalItem, _Encoder, JobIsCompleted, \
		JobIsRolledBack, JobIsFailed, FailedToComplete, \
		FailedToRollBack, RollbackCausedByException, \
		_max_job_time_in_ms, JobIsTimedOut, _roll_back_journal_item, \
		CollectionKeyIsLocked, _deleted_object_value, \
		_create_and_add_lock

from datetime import datetime
import json

# TODO: Add fields to the roll back exceptions to indicate what the original
# exception and stack trace was.

class Job:
	def __init__(self, client):
		self._job_id = _generate_key()
		self._timestamp = datetime.utcnow()
		self._client = client
		self._locks = []
		self._journal = []
		self.is_completed = False
		self.is_rolled_back = False
		# A job should fail only in the event of an exception during
		# completion or roll-back.
		self.is_failed = False

	def _verify_job_is_active(self):
		if self.is_failed:
			raise JobIsFailed
		elif self.is_completed:
			raise JobIsCompleted
		elif self.is_rolled_back:
			raise JobIsRolledBack
		self._raise_if_job_is_timed_out()

	def _raise_if_job_is_timed_out(self):
		elapsed_milliseconds = (datetime.utcnow() - 
				self._timestamp).total_seconds() * 1000.0
		if elapsed_milliseconds > _max_job_time_in_ms:
			raise JobIsTimedOut('Ran for ' + str(elapsed_milliseconds) + 'ms')

	def _remove_locks(self):		
		for lock in self._locks:
			self._raise_if_job_is_timed_out()
			response = self._client.delete(_locks_collection, 
					_get_lock_collection_key(lock.collection, lock.key), 
					lock.lock_ref, False)
			response.raise_for_status()
		self._locks = []

	def _remove_job(self):
		self._raise_if_job_is_timed_out()
		response = self._client.delete(_jobs_collection, self._job_id, 
				None, False)
		response.raise_for_status()
		self._journal = []

	def _get_lock(self, collection, key):
		for lock in self._locks:
			if lock.collection == collection and lock.key == key:
				return lock
		self._raise_if_job_is_timed_out()
		lock = _create_and_add_lock(self._client, collection, key,
				self._job_id, self._timestamp)
		self._locks.append(lock)
		return lock

	def _add_journal_item(self, collection, key, new_value, original_value):
		self._raise_if_job_is_timed_out()		
		journal_item = _JournalItem(datetime.utcnow(), collection, key,
				original_value, new_value)
		self._journal.append(journal_item)
		job_response = self._client.put(_jobs_collection, self._job_id, 
				json.loads(json.dumps({'timestamp': self._timestamp,
				'items': self._journal}, cls=_Encoder)), None, False)
		job_response.raise_for_status()
		return journal_item
		
	def get(self, collection, key, ref = None):	
		self._verify_job_is_active()
		try:
			lock = self._get_lock(collection, key)
			self._raise_if_job_is_timed_out()
			response = self._client.get(collection, key, ref, False)
			response.raise_for_status()
			self._raise_if_job_is_timed_out()
			return response
		except Exception as e:
			self.roll_back((e, traceback.format_exc()))

	def post(self, collection, value):
		key = _generate_key()
		return self.put(collection, key, value)

	def put(self, collection, key, value, ref = None):	
		self._verify_job_is_active()
		try:
			lock = self._get_lock(collection, key)
			self._raise_if_job_is_timed_out()
			# If ref was passed, ensure that the value has not changed.
			# If ref was not passed, retrieve the current ref and store it.
			response = self._client.get(collection, key, ref, False)
			# Indicates a new record will be created.
			if response.status_code == 404: 
				original_value = None
			# Indicates an existing record will be updated so store the 
			# original ref and value.
			elif response.status_code == 200: 
				original_value = response.json
			else:
				response.raise_for_status()
			journal_item = self._add_journal_item(collection, key,
					value, original_value)
			self._raise_if_job_is_timed_out()
			response = self._client.put(collection, key, value, ref, False)
			response.raise_for_status()
			self._raise_if_job_is_timed_out()
			return response
		except Exception as e:
			self.roll_back((e, traceback.format_exc()))

	def delete(self, collection, key, ref = None):	
		self._verify_job_is_active()
		try:
			lock = self._get_lock(collection, key)
			self._raise_if_job_is_timed_out()
			# The record must be present in order to delete it.
			response = self._client.get(collection, key, ref, False)
			response.raise_for_status()
			original_value = response.json
			journal_item = self._add_journal_item(collection, key,
					_deleted_object_value, original_value)
			self._raise_if_job_is_timed_out()
			response = self._client.delete(collection, key, ref, False)
			response.raise_for_status()
			self._raise_if_job_is_timed_out()
			return response
		except Exception as e:
			self.roll_back((e, traceback.format_exc()))

	def roll_back(self, exception_causing_rollback = None):
		self._verify_job_is_active()
		try:
			for journal_item in self._journal:
				_roll_back_journal_item(self._client, journal_item, 
						self._raise_if_job_is_timed_out)
			self._remove_job()
			self._remove_locks()
			self.is_rolled_back = True
			if exception_causing_rollback:
				raise RollbackCausedByException(exception_causing_rollback[0],
						exception_causing_rollback[1])
		except RollbackCausedByException as e:
			raise e
		except Exception as e:
			self.is_failed = True			
			if exception_causing_rollback:
				raise FailedToRollBack(e, traceback.format_exc(),
						exception_causing_rollback[0], 
						exception_causing_rollback[1])
			else:
				raise FailedToRollBack(e, traceback.format_exc())

	def complete(self):
		self._verify_job_is_active()
		try: 
			self._remove_job()
			self._remove_locks()
			self.is_completed = True
		except Exception as e:
			self.is_failed = True
			raise FailedToComplete(e, traceback.format_exc())
