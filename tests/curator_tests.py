import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
				 JobIsRolledBack, JobIsFailed, FailedToComplete, \
				 FailedToRollBack, _locks_collection, _jobs_collection, \
				 _generate_key, RollbackCausedByException, JobIsTimedOut

from . import _were_collections_cleared, _oio_api_key, _clear_test_collections
from subprocess import Popen

class CuratorTests(unittest.TestCase):
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
		self._curator_process = Popen(['python', 'run_curator.py', _oio_api_key])

	def tearDown(self):
		self._curator_process.kill()

	def test_everything(self):
		job = Job(self._client)

		response = job.post('test1', {})
		self.assertRaises(CollectionKeyIsLocked, self._client.put, 'test1', 
				response.key, {})
		self.assertRaises(CollectionKeyIsLocked, self._client.get,
				'test1', response.key)
		self.assertRaises(CollectionKeyIsLocked, self._client.delete, 
				'test1', response.key)

if __name__ == '__main__':
	unittest.main()
