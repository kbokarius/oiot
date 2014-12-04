import sys, traceback
from . import _locks_collection, _jobs_collection, _get_lock_collection_key, \
			  _generate_key, _Lock, _JournalItem, _Encoder, JobIsCompleted, \
			  JobIsRolledBack, JobIsFailed, FailedToComplete, \
			  FailedToRollBack, _format_exception, RollbackCausedByException, \
			  _max_job_time_in_ms, JobIsTimedOut, _roll_back_journal_item

from datetime import datetime
import json

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

	def _get_lock(self, collection, key, ref = None):
		for lock in self._locks:
			if lock.collection == collection and lock.key == key:
				return lock
		self._raise_if_job_is_timed_out()
		lock = _Lock(self._job_id, self._timestamp, datetime.utcnow(), 
					 collection, key, None)
		lock_response = self._client.put(_locks_collection, 
						_get_lock_collection_key(collection, key), 
						json.loads(json.dumps(vars(lock), cls=_Encoder)), 
						None, False)
		lock_response.raise_for_status()
		self._locks.append(lock)
		lock.lock_ref = lock_response.ref
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
		
	def post(self, collection, value):
		key = _generate_key()
		return self.put(collection, key, value)

	def put(self, collection, key, value, ref = None):	
		self._verify_job_is_active()
		try:
			lock = self._get_lock(collection, key, ref)
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
			# Store the new ref and value.
			journal_item.new_value = value
			return response
		except Exception as e:
			self.roll_back(_format_exception(e))

	def roll_back(self, exception_causing_rollback = ''):
		self._verify_job_is_active()
		try:
			for journal_item in self._journal:
				_roll_back_journal_item(self._client, journal_item, 
										self._raise_if_job_is_timed_out)
			self._remove_locks()
			self._remove_job()
			self.is_rolled_back = True
			if exception_causing_rollback is not '':
				raise RollbackCausedByException(exception_causing_rollback)		
		except RollbackCausedByException as e:
			raise e
		except Exception as e:
			self.is_failed = True			
			all_exception_details = (_format_exception(e) + 
									 exception_causing_rollback)
			raise FailedToRollBack(all_exception_details)

	def complete(self):
		self._verify_job_is_active()
		try: 
			self._remove_locks()
			self._remove_job()
			self.is_completed = True
		except Exception as e:
			self.is_failed = True
			raise FailedToComplete(_format_exception(e))
