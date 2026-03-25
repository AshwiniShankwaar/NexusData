"""
scripts/create_test_db.py
Creates (or recreates) sim_test.db with the same rich schema as sample.db.
Run from the project root: python scripts/create_test_db.py
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from demo import create_demo_db

if __name__ == "__main__":
    create_demo_db("sim_test.db")
    print("sim_test.db created successfully.")
