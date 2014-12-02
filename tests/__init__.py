from oiot import _locks_collection, _jobs_collection, _curators_collection, \
				 _get_lock_collection_key
from datetime import datetime
import dateutil

_were_collections_cleared = False
_oio_api_key = '69b4329e-990e-4969-b0ec-b7ef680fd32b'

def _clear_test_collections(client):
	client.delete('test1')
	client.delete('test2')
	client.delete(_locks_collection)
	client.delete(_jobs_collection)
	client.delete(_curators_collection)

def _verify_job_creation(testinstance, job):
	response = job._client.get(_jobs_collection, job._job_id,
			   None, False)
	response.raise_for_status()
	testinstance.assertTrue((datetime.utcnow() - 
					 dateutil.parser.parse(
					 response.json['timestamp'])).
					 total_seconds() < 2.0)
	testinstance.assertTrue('items' in response.json)

def _verify_lock_creation(testinstance, job, collection, key):
	response = job._client.get(_locks_collection, 
			   _get_lock_collection_key(collection, key), 
			   None, False)
	response.raise_for_status()
	testinstance.assertEqual(response.json['job_id'], job._job_id)
	testinstance.assertEqual(response.json['collection'], collection)
	testinstance.assertEqual(response.json['key'], key)

