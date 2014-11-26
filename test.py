import os, sys
import oiot
from oiot import OiotClient, Job

# Verify Oio is up and the key is valid.
client = OiotClient('69b4329e-990e-4969-b0ec-b7ef680fd32b')
client.ping().raise_for_status()

# Clear test tables.
client.delete('test1')
client.delete('test2')
client.delete('oiot-locks')
client.delete('oiot-jobs')

# Add a record without a job.
response1 = client.post('test1', {})
response1.raise_for_status()
print 'Added test1 record with key ' + response1.key

# Create a new job.
job = Job(client)

# Add a record using the job thereby locking the record and journaling the work.
response2 = job.post('test2', {})
print 'Added test2 record with key ' + response2.key

# Update the very first record with the second record's key using the job, thereby locking the very first record and journaling the work.
response3 = job.put('test1', response1.key, { 'test2key': response2.key })
print 'Updated test1 record with test2 key'

# Complete the job which removes all locks used by the job and clears the journal.
job.complete() 
