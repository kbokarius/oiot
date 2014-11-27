import sys, traceback
from . import _locks_collection, _jobs_collection, _get_lock_collection_key, \
			  _generate_key, _Lock, _JournalItem, _Encoder, JobIsCompleted, \
			  JobIsRolledBack, JobIsFailed, FailedToComplete, \
			  FailedToRollBack, _format_exception, RollbackCausedByException, \
			  _max_job_time_in_ms, JobTimedOut

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

	def _verify_job_is_not_timed_out(self):
		elapsed_milliseconds = (datetime.utcnow() - 
								self._timestamp).microseconds / 1000.0
		if elapsed_milliseconds > _max_job_time_in_ms:
			raise JobTimedOut('Ran for ' + str(JobTimedOut) + 'ms')

	def _remove_locks(self):		
		for lock in self._locks:
			self._verify_job_is_not_timed_out()
			response = self._client.delete(_locks_collection, 
					   _get_lock_collection_key(lock.collection, lock.key), 
					   lock.lock_ref, False)
			response.raise_for_status()
		self._locks = []

	def _remove_jobs(self):
		self._verify_job_is_not_timed_out()
		response = self._client.delete(_jobs_collection, self._job_id, 
				   None, False)
		response.raise_for_status()
		self._journal = []

	def _get_lock(self, collection, key, ref = None):
		for lock in self._locks:
			if lock.collection == collection and lock.key == key:
				return lock
		self._verify_job_is_not_timed_out()
		lock = _Lock()
		lock.job_id = self._job_id
		lock.timestamp = datetime.utcnow()
		lock.collection = collection
		lock.key = key
		lock_response = self._client.put(_locks_collection, 
						_get_lock_collection_key(collection, key), 
						json.loads(json.dumps(vars(lock), cls=_Encoder)), 
						None, False)
		lock_response.raise_for_status()
		self._locks.append(lock)
		lock.lock_ref = lock_response.ref
		return lock

	def _add_journal_item(self, collection, key, value):
		self._verify_job_is_not_timed_out()
		journal_item = _JournalItem()
		journal_item.timestamp = datetime.utcnow()
		journal_item.collection = collection
		journal_item.key = key
		self._journal.append(journal_item)
		job_response = self._client.put(_jobs_collection, self._job_id, 
					   json.loads(json.dumps({'timestamp': self._timestamp,
					   'items': self._journal}, cls=_Encoder)), None, False)
		job_response.raise_for_status()
		return journal_item
		
	def post(self, collection, value):
		key = _generate_key()
		return self.put(collection, key, value)

	# TODO: Catch all exceptions - if one is caught, roll-back, and 
	# raise a wrapped exception.
	def put(self, collection, key, value, ref = None):	
		self._verify_job_is_active()
		try:
			lock = self._get_lock(collection, key, ref)
			# If ref was passed, ensure that the value has not changed.
			# If ref was not passed, retrieve the current ref and store it.
			journal_item = self._add_journal_item(collection, key, value)
			self._verify_job_is_not_timed_out()
			response = self._client.get(collection, key, ref, False)
			# Indicates a new record will be created.
			if response.status_code == 404: 
				pass
			# Indicates an existing record will be updated so store the 
			# original ref and value.
			elif response.status_code == 200: 
				journal_item.original_value = response.json
				journal_item.original_ref = response.ref
			else:
				response.raise_for_status()
			self._verify_job_is_not_timed_out()
			response = self._client.put(collection, key, value, ref, False)
			response.raise_for_status()
			# Store the new ref and value.
			journal_item.new_value = value
			journal_item.new_ref = response.ref
			return response
		except Exception as e:
			self.roll_back(_format_exception(e))
			raise e

	def roll_back(self, exception_causing_rollback = ''):
		try: 
			for journal_item in self._journal:
				self._verify_job_is_not_timed_out()
				# Was a new value successfully set?
				if journal_item.new_ref:
					# Was there an original value?
					if journal_item.original_ref:
						# Put back the original value only if the new
						# ref matches.
						try: 
							response = self._client.put(
									   journal_item.collection,
									   journal_item.key, 
									   journal_item.original_value, 
									   journal_item.new_ref, False)
							response.raise_for_status()
						except Exception as e:
							# Ignore 412 error if the ref did not match.
							if e.__class__.__name__ is "HTTPError":
								pass
							else:
								raise e
					# No original value indicates that a new record was 
					# added and should be deleted.
					else:
						try:
							response = self._client.delete(
									   journal_item.collection,
									   journal_item.key, 
									   journal_item.new_ref, False)
							response.raise_for_status()
						except Exception as e:
							# Ignore 412 error if the ref did not match.
							if e.__class__.__name__ is "HTTPError":
								pass
							else:
								raise e
			self._remove_locks()
			self._remove_jobs()
			self.is_rolled_back = True
			if exception_causing_rollback is not '':
				raise RollbackCausedByException(exception_causing_rollback)		
		except RollbackCausedByException as e:
			raise e
		except Exception as e:
			self._is_failed = True			
			all_exception_details = (_format_exception(e) + 
									 exception_causing_rollback)
			raise FailedToRollBack(all_exception_details)

	def complete(self):
		try: 
			self._remove_locks()
			self._remove_jobs()
			self.is_completed = True
		except Exception as e:
			self._is_failed = True
			raise FailedToComplete(_format_exception(e))
