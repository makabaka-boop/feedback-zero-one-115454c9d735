import os
import sys

# Make the in-tree package importable even under "python -I" (isolated mode
# ignores PYTHONPATH and this environment has no pip for an editable install).
_SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)
