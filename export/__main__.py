from export.generate import export_all
import sys
from db.connection import init_db

init_db()
export_all(sys.argv[1] if len(sys.argv) > 1 else None)
