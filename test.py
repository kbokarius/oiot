import os, sys

import json, uuid
from porc import Client
from oiotransactions.oiot import OiotClient, Job

# Verify Oio is up and the key is valid.
client = OiotClient('69b4329e-990e-4969-b0ec-b7ef680fd32b')
client.ping().raise_for_status()

# Clear test tables.
client.delete('test1')
client.delete('test2')
client.delete('oiot-locks')
client.delete('oiot-jobs')

test1uuid = uuid.uuid4()
test1value = { 'test1uuid': str(test1uuid) }
response1 = client.post('test1', test1value)
response1.raise_for_status()
print 'Added test1 record with key ' + response1.key + ' with test1 UUID: ' + str(test1uuid)

job = Job(client)

test2uuid = uuid.uuid4()
test2value = { 'test2uuid': str(test2uuid) }
response2 = job.post('test2', test2value)
print 'Added test2 record with key ' + response2.key + ' with test2 UUID: ' + str(test2uuid)

updatedtest1value = { 'test1uuid': str(test1uuid), 'test2key': response2.key }
response3 = job.put('test1', response1.key, updatedtest1value)
print 'Updated test1 record with test2 key'

job.complete()
