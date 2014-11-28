import os, sys, unittest, time
from datetime import datetime
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut
from . import _were_collections_cleared, _oio_api_key, _clear_test_collections

def _run_test_job1(client):
	# Add a record without a job.
	response1 = client.post('test1', {'testvalue1_key' : 'testvalue1_value'})
	response1.raise_for_status()
	# Create a new job.
	job = Job(client)
	# Add a record using the job thereby locking the record and journaling 
	# the work.
	response2 = job.post('test2', {'testvalue2_key' : 'testvalue2_value'})
	# Update the very first record with the second record's key using the
	# job, thereby locking the very first record and journaling the work.
	response3 = job.put('test1', response1.key, { 'test2key': response2.key })
	return job

class JobTests(unittest.TestCase):
	def setUp(self):
		# Verify o.io is up and the key is valid.
		global _oio_api_key
		self._client = OiotClient(_oio_api_key)
		self._client.ping().raise_for_status()
		global _were_collections_cleared
		if _were_collections_cleared is not True:
			_clear_test_collections(self._client)
			# Sleep to give o.io time to delete the collections. Without this
			# delay inconsistent results will be encountered.
			time.sleep(4)
			_were_collections_cleared = True

	def test_basic_job_completion(self):
		job = _run_test_job1(self._client)
		job.complete() 
		self.assertRaises(JobIsCompleted, job.post, None, None)
		self.assertRaises(JobIsCompleted, job.put, None, None, None)
		self.assertRaises(JobIsCompleted, job.complete)
		self.assertRaises(JobIsCompleted, job.roll_back)

	def test_basic_job_rollback(self):
		job = _run_test_job1(self._client)
		job.roll_back() 
		self.assertRaises(JobIsRolledBack, job.post, None, None)
		self.assertRaises(JobIsRolledBack, job.put, None, None, None)
		self.assertRaises(JobIsRolledBack, job.complete)
		self.assertRaises(JobIsRolledBack, job.roll_back)

	def test_rollback_caused_by_exception(self):
		job = Job(self._client)
		key = _generate_key()
		response = self._client.put('test1', key, {})
		response.raise_for_status()
		response = job.put('test1', key, { 'value_testkey': 
				   'value_testvalue'})
		self.assertRaises(RollbackCausedByException, job.put, 'test1', 
						  key, None)

	def test_failed_completion(self):
		job = Job(self._client)
		key = _generate_key()
		response = self._client.put('test1', key, {})
		response.raise_for_status()
		response = job.put('test1', key, { 'value_testkey': 
				   'value_testvalue'})
		job._client = None
		self.assertRaises(FailedToComplete, job.complete)
		job._client = self._client
		self.assertRaises(JobIsFailed, job.post, None, None)
		self.assertRaises(JobIsFailed, job.put, None, None, None)
		self.assertRaises(JobIsFailed, job.complete)
		self.assertRaises(JobIsFailed, job.roll_back)

	def test_failed_rollback(self):
		job = Job(self._client)
		key = _generate_key()
		response = self._client.put('test1', key, {})
		response.raise_for_status()
		response = job.put('test1', key, { 'value_testkey': 
				   'value_testvalue'})
		job._client = None
		self.assertRaises(FailedToRollBack, job.roll_back)
		job._client = self._client
		self.assertRaises(JobIsFailed, job.post, None, None)
		self.assertRaises(JobIsFailed, job.put, None, None, None)

	def test_job_timeout(self):
		job = Job(self._client)
		time.sleep(6)
		self.assertRaises(JobIsTimedOut, job.post, 'test2', {})
		self.assertRaises(JobIsTimedOut, job.put, 'test2', 
						  _generate_key(), {})
		self.assertRaises(JobIsTimedOut, job.complete)
		self.assertRaises(JobIsTimedOut, job.roll_back)

	# TODO: Add test for verifying a lock was created.
	# TODO: Add test for verifying a lock was removed.
	# TODO: Add test for verifying a job was created.
	# TODO: Add test for verifying a job was removed.
	# TODO: Verify that the operations are actually being written.
	# TODO: Verify that the operations are actually being rolled back.

if __name__ == '__main__':
	unittest.main()
