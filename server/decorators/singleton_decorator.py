"""Decorates classes to become Singletons

The decorator creates a private instance variable that returns
the same instance of the decorated class.
"""

import functools


def singleton(cls):
    """Wraps a class to produce a singleton.

    `server` names these singletons with the suffix `Manager`.

    Args:
        cls: Class that becomes a singleton

    Returns:
        Singleton instance of cls
    """
    @functools.wraps(cls)
    def wrapper_singleton(*args, **kwargs):
        if not wrapper_singleton.instance:
            wrapper_singleton.instance = cls(*args, **kwargs)
        return wrapper_singleton.instance
    
    wrapper_singleton.instance = None
    return wrapper_singleton
