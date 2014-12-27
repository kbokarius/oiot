class CollectionKeyIsLocked(Exception):
    """
    Raised when a locked key is accessed.
    """
    pass


class FailedToComplete(Exception):
    """
    Raised when a job fails to complete.
    """
    def __init__(self, exception_failing_completion=None,
                stacktrace_failing_completion=None):
        """
        Create a FailedToComplete instance.
        :param exception_failing_completion: the exception that caused the
        job completion to fail
        :param stacktrace_failing_completion: the stacktrace that caused the
        job completion to fail
        """
        super(FailedToComplete, self).__init__()
        self.exception_failing_completion = exception_failing_completion
        self.stacktrace_failing_completion = stacktrace_failing_completion


class FailedToRollBack(Exception):
    """
    Raised when a job fails to roll back.
    """
    def __init__(self, exception_causing_rollback=None,
                stacktrace_causing_rollback=None,
                exception_failing_rollback=None,
                stacktrace_failing_rollback=None):
        """
        Create a FailedToRollBack instance.
        :param exception_causing_rollback: the exception that caused the
        job to roll back
        :param stacktrace_causing_rollback: the stacktrace that caused the
        job to roll back
        :param exception_failing_rollback: the exception that caused the
        job roll back to fail
        :param stacktrace_failing_rollback: the stacktrace that caused the
        job roll back to fail
        """
        super(FailedToRollBack, self).__init__()
        self.exception_causing_rollback = exception_causing_rollback
        self.stacktrace_causing_rollback = stacktrace_causing_rollback
        self.exception_failing_rollback = exception_failing_rollback
        self.stacktrace_failing_rollback = stacktrace_failing_rollback


class RollbackCausedByException(Exception):
    """
    Raised when a roll back is caused by an exception.
    """
    def __init__(self, exception_causing_rollback=None,
                stacktrace_causing_rollback=None):
        """
        Create a RollbackCausedByException instance.
        :param exception_causing_rollback: the exception that caused the
        job to roll back
        :param stacktrace_causing_rollback: the stacktrace that caused the
        job to roll back
        """
        super(RollbackCausedByException, self).__init__()
        self.exception_causing_rollback = exception_causing_rollback
        self.stacktrace_causing_rollback = stacktrace_causing_rollback


class JobIsFailed(Exception):
    """
    Raised when a failed job is used.
    """
    pass


class JobIsCompleted(Exception):
    """
    Raised when a completed job is used.
    """
    pass


class JobIsRolledBack(Exception):
    """
    Raised when a rolled back job is used.
    """
    pass


class JobIsTimedOut(Exception):
    """
    Raised when a timed out job is used.
    """
    pass


class _CuratorNoLongerActive(Exception):
    """
    Raised when an active curator is no longer active.
    """
    pass

def _get_httperror_status_code(exception):
    """
    Get the HTTPError status code from the specified exception.
    :param exception: the specified exception
    :return: the HTTPError status code
    """
    if exception.__class__.__name__ is 'HTTPError':
        return exception.response.status_code
    else:
        return None

def _format_exception(e):
    """
    Format the specified exception into a readable string.
    :param exception: the specified exception
    :return: the formatted exception
    """
    return traceback.format_exc()
