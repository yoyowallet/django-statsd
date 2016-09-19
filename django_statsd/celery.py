import time

from django_statsd.clients import statsd

_task_start_times = {}
_task_publish_start_times = {}


def on_before_task_publish(sender=None, **kwargs):
    """
    Handle Celery ``before_task_publish`` signals.
    """
    _task_publish_start_times[sender] = time.time()


def on_after_task_publish(sender=None, **kwargs):
    """
    Handle Celery ``after_task_publish`` signals.
    """
    # Increase statsd counter.
    statsd.incr('celery.%s.sent' % sender)

    # Log publish duration.
    start_time = _task_publish_start_times.pop(sender, False)
    if start_time:
        ms = int((time.time() - start_time) * 1000)
        statsd.timing('celery.%s.publish.runtime' % sender, ms)


def on_task_prerun(sender=None, task_id=None, task=None, **kwargs):
    """
    Handle Celery ``task_prerun``signals.
    """
    # Increase statsd counter.
    statsd.incr('celery.%s.start' % task.name)

    # Keep track of start times. (For logging the duration in the postrun.)
    _task_start_times[task_id] = time.time()


def on_task_postrun(sender=None, task_id=None, task=None, **kwargs):
    """
    Handle Celery ``task_postrun`` signals.
    """
    # Increase statsd counter.
    statsd.incr('celery.%s.done' % task.name)

    # Log duration.
    start_time = _task_start_times.pop(task_id, False)
    if start_time:
        ms = int((time.time() - start_time) * 1000)
        statsd.timing('celery.%s.runtime' % task.name, ms)


def on_task_failure(sender=None, task_id=None, task=None, **kwargs):
    """
    Handle Celery ``task_failure`` signals.
    """
    # Increase statsd counter.
    statsd.incr('celery.%s.failure' % task)


def register_celery_events():
    try:
        from celery import signals
    except ImportError:
        pass
    else:
        signals.before_task_publish.connect(on_before_task_publish)
        signals.after_task_publish.connect(on_after_task_publish)
        signals.task_prerun.connect(on_task_prerun)
        signals.task_postrun.connect(on_task_postrun)
        signals.task_failure.connect(on_task_failure)
