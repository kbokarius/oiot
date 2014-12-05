import os, sys, unittest, time, dateutil, json
from datetime import datetime
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut, \
				 _get_lock_collection_key, _get_httperror_status_code, \
				 _jobs_collection
from . import _were_collections_cleared, _oio_api_key, \
			  _verify_job_creation, _clear_test_collections, \
			  _verify_lock_creation

def run_test_job1(client):
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
	response3 = job.put('test1', response1.key, {'test2key': response2.key})
	return job

def run_test_basic_job_completion(client, test_instance):
	job = run_test_job1(client)
	job.complete() 
	test_instance.assertRaises(JobIsCompleted, job.post, None, None)
	test_instance.assertRaises(JobIsCompleted, job.put, None, None, None)
	test_instance.assertRaises(JobIsCompleted, job.complete)
	test_instance.assertRaises(JobIsCompleted, job.roll_back)

def run_test_basic_job_rollback(client, test_instance):
	job = run_test_job1(client)
	job.roll_back() 
	test_instance.assertRaises(JobIsRolledBack, job.post, None, None)
	test_instance.assertRaises(JobIsRolledBack, job.put, None, None, None)
	test_instance.assertRaises(JobIsRolledBack, job.complete)
	test_instance.assertRaises(JobIsRolledBack, job.roll_back)

def run_test_rollback_caused_by_exception(client, test_instance):
	job = Job(client)
	key = _generate_key()
	response = client.put('test1', key, {})
	response.raise_for_status()
	response = job.put('test1', key, {'value_testkey': 
			   'value_testvalue'})
	test_instance.assertRaises(RollbackCausedByException, job.put, 'test1', 
					  key, None)

def run_test_failed_completion(client, test_instance):
	job = Job(client)
	key = _generate_key()
	response = client.put('test1', key, {})
	response.raise_for_status()
	response = job.put('test1', key, {'value_testkey': 
			   'value_testvalue'})
	job._client = None
	test_instance.assertRaises(FailedToComplete, job.complete)
	job._client = client
	test_instance.assertRaises(JobIsFailed, job.post, None, None)
	test_instance.assertRaises(JobIsFailed, job.put, None, None, None)
	test_instance.assertRaises(JobIsFailed, job.complete)
	test_instance.assertRaises(JobIsFailed, job.roll_back)

def run_test_failed_rollback(client, test_instance):
	job = Job(client)
	key = _generate_key()
	response = client.put('test1', key, {})
	response.raise_for_status()
	response = job.put('test1', key, {'value_testkey': 
			   'value_testvalue'})
	job._client = None
	test_instance.assertRaises(FailedToRollBack, job.roll_back)
	job._client = client
	test_instance.assertRaises(JobIsFailed, job.post, None, None)
	test_instance.assertRaises(JobIsFailed, job.put, None, None, None)

def run_test_job_timeout(client, test_instance):
	job = Job(client)
	time.sleep(6)
	test_instance.assertRaises(JobIsTimedOut, job.post, 'test2', {})
	test_instance.assertRaises(JobIsTimedOut, job.put, 'test2', 
					  _generate_key(), {})
	test_instance.assertRaises(JobIsTimedOut, job.complete)
	test_instance.assertRaises(JobIsTimedOut, job.roll_back)

def run_test_job_and_lock_creation_and_removal(client, test_instance):		
	job = Job(client)
	response2 = job.post('test2', {})
	_verify_lock_creation(test_instance, job, 'test2', response2.key)
	_verify_job_creation(test_instance, job)
	test3_key = _generate_key()
	response3 = job.put('test3', test3_key, {})
	_verify_lock_creation(test_instance, job, 'test3', test3_key)
	job.complete()
	was_404_error_caught = False
	try:
		_verify_lock_creation(test_instance, job, 'test2', response2.key)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)
	was_404_error_caught = False
	try:
		_verify_lock_creation(test_instance, job, 'test3', test3_key)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)
	was_404_error_caught = False
	try:
		_verify_job_creation(test_instance, job)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)

def run_test_job_and_lock_creation_and_removal2(client, test_instance):		
	job = Job(client)
	response2 = job.post('test2', {})
	_verify_lock_creation(test_instance, job, 'test2', response2.key)
	_verify_job_creation(test_instance, job)
	test3_key = _generate_key()
	response3 = job.put('test3', test3_key, {})
	_verify_lock_creation(test_instance, job, 'test3', test3_key)
	job.roll_back()
	was_404_error_caught = False
	try:
		_verify_lock_creation(test_instance, job, 'test2', response2.key)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)
	was_404_error_caught = False
	try:
		_verify_lock_creation(test_instance, job, 'test3', test3_key)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)
	was_404_error_caught = False
	try:
		_verify_job_creation(test_instance, job)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)

def run_test_verify_writes_and_roll_back(client, test_instance):
	test3_key = _generate_key()
	response3 = client.put('test3', test3_key,
				{'value_key3': 'value_value3'})
	response3.raise_for_status()
	response = client.get('test3', test3_key, 
			   response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)
	job = Job(client)
	response2 = job.post('test2', {'value_key2': 'value_value2'})
	response2.raise_for_status()
	response = client.get('test2', response2.key,
			   response2.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key2': 'value_value2'}, response.json)
	response3 = job.put('test3', test3_key,
				{'value_newkey3': 'value_newvalue3'}, response3.ref)
	response3.raise_for_status()
	response = client.get('test3', test3_key, 
			   response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_newkey3': 'value_newvalue3'}, response.json)
	job.roll_back()
	response = client.get('test2', response2.key,
			   None, False)
	test_instance.assertEqual(response.status_code, 404)
	response = client.get('test3', test3_key, 
			   None, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)

def run_test_exception_raised_when_key_locked(client, test_instance):		
	job = Job(client)
	response2 = job.post('test2', {})	
	job2 = Job(client)
	test_instance.assertRaises(RollbackCausedByException, job2.put, 
					 'test2', response2.key, {})

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
		run_test_basic_job_completion(self._client, self)

	def test_basic_job_rollback(self):	
		run_test_basic_job_rollback(self._client, self)

	def test_rollback_caused_by_exception(self):	
		run_test_rollback_caused_by_exception(self._client, self)

	def test_failed_completion(self):	
		run_test_failed_completion(self._client, self)

	def test_failed_rollback(self):
		run_test_failed_rollback(self._client, self)

	def test_job_timeout(self):
		run_test_job_timeout(self._client, self)

	def test_job_and_lock_creation_and_removal(self):
		run_test_job_and_lock_creation_and_removal(self._client, self)

	def test_job_and_lock_creation_and_removal2(self):
		run_test_job_and_lock_creation_and_removal2(self._client, self)

	def test_verify_writes_and_roll_back(self):
		run_test_verify_writes_and_roll_back(self._client, self)

	def test_exception_raised_when_key_locked(self):
		run_test_exception_raised_when_key_locked(self._client, self)

if __name__ == '__main__':
	unittest.main()
