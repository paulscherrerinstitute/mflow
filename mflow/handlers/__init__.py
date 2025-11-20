
import glob
import os


# automatically populates __all__ with the handler modules
# this is not needed as it is the default for the current file/folder structure anyway
__all__ = []
htypes = {}
for f in glob.glob(os.path.dirname(__file__) + "/*.py"):
    if os.path.isfile(f) and not os.path.basename(f).startswith("_"):
        __all__.append(os.path.basename(f)[:-3])


# converts a human-readable xyz-1.0 to a module xyz_1_0.py and imports it
# this is not needed (and not used) since the two dicts in mflow.py (receive_handlers and send_handlers) handle this explicitly
def load(htype):
    # Todo: Add some more logic to use an more general handler if possible
    # i.e. htype-1.2 can be handled with an htype_1.py handler
    s_type = __import__("handlers." + htype.replace(".", "_").replace("-", "_"), fromlist=".")
    return s_type.Handler()


