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
		_verify_lock_creation, _verify_lock_deletion

def verify_locked_exception_is_raised(test_instance, operation, *args):
	try:
		operation(*args)
		test_instance.fail('RollbackCausedByException not raised')
	except Exception as e:
		test_instance.assertEqual(e.__class__.__name__, 
				'RollbackCausedByException')
		test_instance.assertEqual(e.exception_causing_rollback.
				__class__.__name__, 'CollectionKeyIsLocked')

def run_test_job1(client):
	# Add records without a job.
	response1 = client.post('test1', {'testvalue1_key' : 'testvalue1_value'})
	response1.raise_for_status()
	response3 = client.post('test3', {'testvalue3_key' : 'testvalue3_value'})
	response3.raise_for_status()
	# Create a new job.
	job = Job(client)
	# Add a record using the job thereby locking the record and journaling 
	# the work.
	response2 = job.post('test2', {'testvalue2_key' : 'testvalue2_value'})
	job.get('test3', response3.key, response3.ref)
	# Update the very first record with the second record's key using the
	# job, thereby locking the very first record and journaling the work.
	job.put('test1', response1.key, {'test2key': response2.key})
	job.delete('test3', response3.key, response3.ref)
	return job

def run_test_basic_job_completion(client, test_instance):
	job = run_test_job1(client)
	job.complete() 
	test_instance.assertRaises(JobIsCompleted, job.post, None, None)
	test_instance.assertRaises(JobIsCompleted, job.put, None, None, None)
	test_instance.assertRaises(JobIsCompleted, job.get, None, None, None)
	test_instance.assertRaises(JobIsCompleted, job.delete, None, None, None)
	test_instance.assertRaises(JobIsCompleted, job.complete)
	test_instance.assertRaises(JobIsCompleted, job.roll_back)

def run_test_basic_job_rollback(client, test_instance):
	job = run_test_job1(client)
	job.roll_back() 
	test_instance.assertRaises(JobIsRolledBack, job.post, None, None)
	test_instance.assertRaises(JobIsRolledBack, job.put, None, None, None)
	test_instance.assertRaises(JobIsRolledBack, job.get, None, None, None)
	test_instance.assertRaises(JobIsRolledBack, job.delete, None, None, None)
	test_instance.assertRaises(JobIsRolledBack, job.complete)
	test_instance.assertRaises(JobIsRolledBack, job.roll_back)

def run_test_rollback_caused_by_exception(client, test_instance):
	job = Job(client)
	key = _generate_key()
	response = client.put('test1', key, {})
	response.raise_for_status()
	response = job.put('test1', key, {'value_testkey': 'value_testvalue'})
	test_instance.assertRaises(RollbackCausedByException, job.put, 'test1', 
			key, None)

def run_test_failed_completion(client, test_instance):
	job = Job(client)
	key = _generate_key()
	response = client.put('test1', key, {})
	response.raise_for_status()
	response = job.put('test1', key, {'value_testkey': 'value_testvalue'})
	job._client = None
	test_instance.assertRaises(FailedToComplete, job.complete)
	job._client = client
	test_instance.assertRaises(JobIsFailed, job.post, None, None)
	test_instance.assertRaises(JobIsFailed, job.put, None, None, None)
	test_instance.assertRaises(JobIsFailed, job.get, None, None, None)
	test_instance.assertRaises(JobIsFailed, job.delete, None, None, None)
	test_instance.assertRaises(JobIsFailed, job.complete)
	test_instance.assertRaises(JobIsFailed, job.roll_back)

def run_test_failed_rollback(client, test_instance):
	job = Job(client)
	key = _generate_key()
	response = client.put('test1', key, {})
	response.raise_for_status()
	response = job.put('test1', key, {'value_testkey': 'value_testvalue'})
	job._client = None
	test_instance.assertRaises(FailedToRollBack, job.roll_back)
	job._client = client
	test_instance.assertRaises(JobIsFailed, job.post, None, None)
	test_instance.assertRaises(JobIsFailed, job.put, None, None, None)
	test_instance.assertRaises(JobIsFailed, job.get, None, None, None)
	test_instance.assertRaises(JobIsFailed, job.delete, None, None, None)

def run_test_job_timeout(client, test_instance):
	job = Job(client)
	time.sleep(6)
	test_instance.assertRaises(JobIsTimedOut, job.post, 'test2', {})
	test_instance.assertRaises(JobIsTimedOut, job.put, 'test2', 
			_generate_key(), {})
	test_instance.assertRaises(JobIsTimedOut, job.get, 'test2', 
			_generate_key())
	test_instance.assertRaises(JobIsTimedOut, job.delete, 'test2', 
			_generate_key())
	test_instance.assertRaises(JobIsTimedOut, job.complete)
	test_instance.assertRaises(JobIsTimedOut, job.roll_back)

def run_test_job_and_lock_creation_and_removal(client, test_instance):
	response4 = client.post('test4', {'testvalue4_key' : 'testvalue4_value'})
	response4.raise_for_status()	
	response5 = client.post('test5', {'testvalue5_key' : 'testvalue5_value'})
	response5.raise_for_status()		
	job = Job(client)
	response2 = job.post('test2', {})
	_verify_lock_creation(test_instance, job, 'test2', response2.key)
	_verify_job_creation(test_instance, job)
	test3_key = _generate_key()
	response3 = job.put('test3', test3_key, {})
	_verify_lock_creation(test_instance, job, 'test3', test3_key)
	job.get('test4', response4.key, response4.ref)
	_verify_lock_creation(test_instance, job, 'test4', response4.key)
	job.delete('test5', response5.key, response5.ref)
	_verify_lock_creation(test_instance, job, 'test5', response5.key)
	job.complete()
	was_404_error_caught = False
	_verify_lock_deletion(test_instance, job, 'test2', response2.key)
	_verify_lock_deletion(test_instance, job, 'test3', test3_key)
	_verify_lock_deletion(test_instance, job, 'test4', response4.key)
	_verify_lock_deletion(test_instance, job, 'test5', response5.key)
	was_404_error_caught = False
	try:
		_verify_job_creation(test_instance, job)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)

def run_test_job_and_lock_creation_and_removal2(client, test_instance):	
	response4 = client.post('test4', {'testvalue4_key' : 'testvalue4_value'})
	response4.raise_for_status()	
	response5 = client.post('test5', {'testvalue5_key' : 'testvalue5_value'})
	response5.raise_for_status()	
	job = Job(client)
	response2 = job.post('test2', {})
	_verify_lock_creation(test_instance, job, 'test2', response2.key)
	_verify_job_creation(test_instance, job)
	test3_key = _generate_key()
	response3 = job.put('test3', test3_key, {})
	_verify_lock_creation(test_instance, job, 'test3', test3_key)
	job.get('test4', response4.key, response4.ref)
	_verify_lock_creation(test_instance, job, 'test4', response4.key)
	job.delete('test5', response5.key, response5.ref)
	_verify_lock_creation(test_instance, job, 'test5', response5.key)
	job.roll_back()
	_verify_lock_deletion(test_instance, job, 'test2', response2.key)
	_verify_lock_deletion(test_instance, job, 'test3', test3_key)
	_verify_lock_deletion(test_instance, job, 'test4', response4.key)
	_verify_lock_deletion(test_instance, job, 'test5', response5.key)
	was_404_error_caught = False
	try:
		_verify_job_creation(test_instance, job)
	except Exception as e:
		if _get_httperror_status_code(e) == 404:
			was_404_error_caught = True
	test_instance.assertTrue(was_404_error_caught)

def run_test_verify_operations_and_roll_back(client, test_instance):
	test3_key = _generate_key()
	response3 = client.put('test3', test3_key,
			{'value_key3': 'value_value3'})
	response3.raise_for_status()
	response = client.get('test3', test3_key, response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)
	job = Job(client)
	response2 = job.post('test2', {'value_key2': 'value_value2'})
	response2.raise_for_status()
	response = client.get('test2', response2.key, response2.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key2': 'value_value2'}, response.json)
	response3 = job.put('test3', test3_key,
			{'value_newkey3': 'value_newvalue3'}, response3.ref)
	response3.raise_for_status()
	response = client.get('test3', test3_key, response3.ref, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_newkey3': 'value_newvalue3'}, 
			response.json)
	response = job.get('test3', test3_key, response3.ref)
	response.raise_for_status()
	test_instance.assertEqual({'value_newkey3': 'value_newvalue3'}, 
			response.json)
	response4 = client.post('test4', {'value_key4': 'value_value4'})
	response4.raise_for_status()
	job.delete('test4', response4.key)
	response = client.get('test4', response4.key, response4.ref, False)
	test_instance.assertEqual(response.status_code, 404)
	job.roll_back()
	response = client.get('test2', response2.key, None, False)
	test_instance.assertEqual(response.status_code, 404)
	response = client.get('test3', test3_key, None, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key3': 'value_value3'}, response.json)
	response = client.get('test4', response4.key, None, False)
	response.raise_for_status()
	test_instance.assertEqual({'value_key4': 'value_value4'}, response.json)

def run_test_exception_raised_when_key_locked(client, test_instance):		
	job = Job(client)
	response2 = job.post('test2', {})	
	verify_locked_exception_is_raised(test_instance, Job(client).put,
			'test2', response2.key, {})
	verify_locked_exception_is_raised(test_instance, Job(client).get,
			'test2', response2.key)
	verify_locked_exception_is_raised(test_instance, Job(client).delete,
			'test2', response2.key)

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

	def test_verify_operations_and_roll_back(self):
		run_test_verify_operations_and_roll_back(self._client, self)

	def test_exception_raised_when_key_locked(self):
		run_test_exception_raised_when_key_locked(self._client, self)

if __name__ == '__main__':
	unittest.main()
