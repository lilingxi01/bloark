import functools
import inspect
import warnings


def _unstable_warning_decorator(func):
    """
    This is a decorator which can be used to mark functions as unstable. It will result in a warning being emitted
    when the function is used.
    """
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', Warning)  # turn off filter
        warnings.warn("You are using an unstable module/function {}.".format(func.__name__),
                      category=Warning,
                      stacklevel=2)
        warnings.simplefilter('default', Warning)  # reset filter
        return func(*args, **kwargs)
    return new_func


def unstable(cls=None, since=None, message=None):
    """
    This decorator marks a class or all methods of a class as unstable and adds a marker for Sphinx documentation.

    Parameters
    ----------
    cls : class, optional
        The class (or function) to be marked as unstable.
    since : str, optional
        The version in which the class or method was marked as unstable.
    message : str, optional
        A message to be displayed in the documentation.

    """
    if cls is None:
        def _unstable_internal_wrapper(cls):
            cls.__unstable__ = True
            if since:
                cls.__unstable_since__ = since
            if message:
                cls.__unstable_message__ = message
            if inspect.isclass(cls):
                for name, method in inspect.getmembers(cls, inspect.isfunction):
                    setattr(cls, name, _unstable_warning_decorator(method))
            else:
                cls = _unstable_warning_decorator(cls)
            return cls
        return _unstable_internal_wrapper

    cls.__unstable__ = True
    if inspect.isclass(cls):
        for name, method in inspect.getmembers(cls, inspect.isfunction):
            setattr(cls, name, _unstable_warning_decorator(method))
    else:
        cls = _unstable_warning_decorator(cls)
    return cls


def _internal_deprecated_decorator(func):
    """
    This is a decorator which can be used to mark functions as deprecated. It will result in a warning being emitted
    when the function is used.
    """
    @functools.wraps(func)
    def new_func(*args, **kwargs):
        warnings.simplefilter('always', DeprecationWarning)  # turn off filter
        warnings.warn("Call to deprecated function {}.".format(func.__name__),
                      category=DeprecationWarning,
                      stacklevel=2)
        warnings.simplefilter('default', DeprecationWarning)  # reset filter
        return func(*args, **kwargs)
    return new_func


def deprecated(version=None, message=None):
    """
    This decorator marks a class or all methods of a class as unstable and adds a marker for Sphinx documentation.

    Parameters
    ----------
    version : str, optional
        The version in which the class or method was deprecated.
    message : str, optional
        A message to be displayed in the documentation.

    """
    def _internal_deprecated_wrapper(cls):
        cls.__deprecated__ = True
        cls.__deprecated_version__ = version
        if message:
            cls.__deprecated_message__ = message
        if inspect.isclass(cls):
            for name, method in inspect.getmembers(cls, inspect.isfunction):
                setattr(cls, name, _internal_deprecated_decorator(method))
        else:
            cls = _internal_deprecated_decorator(cls)
        return cls
    return _internal_deprecated_wrapper
