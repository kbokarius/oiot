import os, sys, unittest, time
from oiot import OiotClient, Job, CollectionKeyIsLocked, JobIsCompleted, \
                 JobIsRolledBack, JobIsFailed, FailedToComplete, \
                 FailedToRollBack, _locks_collection, _jobs_collection, \
                 _generate_key, RollbackCausedByException, JobIsTimedOut, \
                 _get_lock_collection_key, _create_and_add_lock
from datetime import datetime
from . import _oio_api_key, _clear_test_collections

class ClientTests(unittest.TestCase):
    def setUp(self):
        # Verify o.io is up and the key is valid.
        global _oio_api_key
        self._client = OiotClient(_oio_api_key)
        self._client.ping().raise_for_status()

    def test_lock_key_and_execute_operation(self):
        response = self._client._lock_key_and_execute_operation(True,
                super(self._client.__class__, self._client).put, 'test1',
                _generate_key(), {}, None)
        response.raise_for_status()
        self._client.get('test1', response.key, None, 
                False).raise_for_status()
        self.assertEqual(self._client.get(_locks_collection,
                _get_lock_collection_key(
                'test1', response.key), None, 
                False).status_code, 404)

    def test_add_and_remove_lock(self):
        key = _generate_key()
        lock = _create_and_add_lock(self._client, 'test1', key, None,
                datetime.utcnow())
        self._client.get(_locks_collection, _get_lock_collection_key(
                'test1', key), None,
                False).raise_for_status()
        self._client._remove_lock(lock)
        self.assertEqual(self._client.get(_locks_collection,
                _get_lock_collection_key(
                'test1', key), None,
                False).status_code, 404)
    
    def test_ignore_locks(self):
        job = Job(self._client)
        response = job.post('test1', {})
        self._client.put('test1', response.key, {}, None,
                False).raise_for_status()
        self._client.get('test1', response.key, None, 
                False).raise_for_status()
        self._client.delete('test1', response.key, None, 
                False).raise_for_status()

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
