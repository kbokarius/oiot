from porc import Client
from datetime import datetime
from . import CollectionKeyIsLocked, _create_and_add_lock, \
        _get_lock_collection_key
from .settings import _locks_collection 

class OiotClient(Client):
    """
    The oiot implementation of porc.Client. Used for ensuring that locked
    o.io objects cannot be read or written to.
    """
    def __init__(self, api_key, custom_url = None,
            use_async = False, **kwargs):
        super(self.__class__, self).__init__(api_key, custom_url = None,
                use_async = False, **kwargs)

    def _remove_lock(self, lock):
        """
        Remove the specified lock from the locks collection in o.io.
        :param lock: the specified lock to remove
        """
        try:
            # Ignore exceptions and do not raise for status.
            # If necessary the curator will clean up the orphaned lock.
            super(self.__class__, self).delete(_locks_collection,
                    _get_lock_collection_key(lock.collection, lock.key),
                    lock.lock_ref)
        except:
            pass

    def _lock_key_and_execute_operation(self, raise_if_locked, operation, *args):
        """
        Execute the specified o.io operation by first locking the collection
        key and then executing the operation.
        :param raise_if_locked: whether to raise an exception if the key is
        already locked
        :param operation: the specified o.io operation
        :param args: the specified o.io operation's arguments
        :return: the o.io operation's response
        """
        lock = None
        response = None
        if raise_if_locked:
            lock = _create_and_add_lock(self, args[0], args[1], None,
                    datetime.utcnow())
        try:
            response = operation(*args)
        except Exception:
            if raise_if_locked:
                self._remove_lock(lock)
            raise
        if raise_if_locked:
            self._remove_lock(lock)
        return response

    def put(self, collection, key, value, ref = None, raise_if_locked = True):
        return self._lock_key_and_execute_operation(raise_if_locked,
                super(self.__class__, self).put, collection, key, value, ref)

    def get(self, collection, key, ref = None, raise_if_locked = True):
        return self._lock_key_and_execute_operation(raise_if_locked,
                super(self.__class__, self).get, collection, key, ref)

    def delete(self, collection, key = None, ref = None,
                raise_if_locked = True):
        # Deleting an entire collection does not lock the collection.
        if key is None:
            return super(self.__class__, self).delete(collection)
        return self._lock_key_and_execute_operation(raise_if_locked,
                super(self.__class__, self).delete, collection, key, ref)
