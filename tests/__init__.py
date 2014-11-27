from oiot import _locks_collection, _jobs_collection

_were_collections_cleared = False
_oio_api_key = '69b4329e-990e-4969-b0ec-b7ef680fd32b'

def _clear_test_collections(client):
	client.delete('test1')
	client.delete('test2')
	client.delete(_locks_collection)
	client.delete(_jobs_collection)

