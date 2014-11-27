import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection

def _clear_test_collections(client):
	# Clear test collections.
	client.delete('test1')
	client.delete('test2')
	client.delete(_locks_collection)
	client.delete(_jobs_collection)

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

_were_collections_cleared = False

class OiotTests(unittest.TestCase):
	def setUp(self):
		# Verify Oio is up and the key is valid.
		self._client = OiotClient('69b4329e-990e-4969-b0ec-b7ef680fd32b')
		self._client.ping().raise_for_status()
		global _were_collections_cleared
		if _were_collections_cleared is not True:
			_clear_test_collections(self._client)
			# Sleep to give o.io time to delete the collections. Without this
			# delay bugs will be encountered.
			time.sleep(3)
			_were_collections_cleared = True

	def test_basic_job_completion(self):
		job = _run_test_job1(self._client)
		# Complete the job which removes all locks used by the job and clears
		# the journal.
		job.complete() 
		# Attempt to put or post to the job and verify that JobIsCompleted
		# is raised.
		self.assertRaises(JobIsCompleted, job.post, None, None)
		self.assertRaises(JobIsCompleted, job.put, None, None, None)

	def test_basic_job_rollback(self):
		job = _run_test_job1(self._client)
		# Roll back the job which rolls backk all of the items in the journal,
		# removes all locks used by the job, and clears the journal.
		job.roll_back() 
		# Attempt to put or post to the job and verify that JobIsRolledBack
		# is raised.
		self.assertRaises(JobIsRolledBack, job.post, None, None)
		self.assertRaises(JobIsRolledBack, job.put, None, None, None)

	# TODO: Add test for verifying a lock was created.
	# TODO: Add test for verifying a lock was removed.
	# TODO: Add test for verifying a job was created.
	# TODO: Add test for verifying a job was removed.

	# TODO: Implement into test:
	# Attempt to write to the new test2 record using the client and verify
	# CollectionKeyIsLocked is raised.
	#self.assertRaises(CollectionKeyIsLocked, client.put, 'test2', 
	#				  response2.key, {})

if __name__ == '__main__':
	unittest.main()
