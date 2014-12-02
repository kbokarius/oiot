import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut

from . import _oio_api_key, _clear_test_collections

class ClientTests(unittest.TestCase):
	def setUp(self):
		# Verify o.io is up and the key is valid.
		global _oio_api_key
		self._client = OiotClient(_oio_api_key)
		self._client.ping().raise_for_status()

	def test_collection_key_locked(self):
		job = Job(self._client)
		response = job.post('test1', {})
		self.assertRaises(CollectionKeyIsLocked, self._client.put, 'test1', 
				response.key, {})
		self.assertRaises(CollectionKeyIsLocked, self._client.get,
				'test1', response.key)
		self.assertRaises(CollectionKeyIsLocked, self._client.delete, 
				'test1', response.key)

	# TODO: Ensure that all applicable methods raise CollectionKeyIsLocked.

if __name__ == '__main__':
	unittest.main()
