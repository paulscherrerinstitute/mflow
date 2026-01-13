
from .mflow import *


#TODO: deprecate the tools name in favor of utils for consistency with all our other projects
class _DeprecationWrapper:

    def __init__(self, deprecated, message):
        self.deprecated = deprecated
        self.message = message

    def __getattr__(self, name):
        import warnings
        warnings.warn(self.message, category=DeprecationWarning, stacklevel=2)
        return getattr(self.deprecated, name)

    def __dir__(self):
        return dir(self.deprecated)

    def __repr__(self):
        return repr(self.deprecated)


from . import utils
tools = _DeprecationWrapper(utils, "the `mflow.tools` alias is deprecated; use `mflow.utils` instead")


