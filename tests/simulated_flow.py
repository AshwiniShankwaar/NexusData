"""
tests/simulated_flow.py
Simulates a real conversation flow with Follow-ups and Caching.
Mocks the LLM to avoid local dependency issues while running REAL pipeline logic.
"""
from __future__ import annotations
import os
import json
from unittest.mock import MagicMock
from nexus_data.orchestrator import NexusData

def simulate():
    print("--- 1. Initializing NexusData ---")
    nd = NexusData(interactive_setup=False)
    
    # Mocking LLM results to simulate valid Stage 2/4 outputs
    nd._llm.generate = MagicMock()
    
    # 2. Setup Sample DB
    db_uri = "sqlite:///sim_test.db"
    if os.path.exists("sim_test.db"): os.remove("sim_test.db")
    import sqlite3
    conn = sqlite3.connect("sim_test.db")
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, region TEXT, status TEXT)")
    conn.execute("INSERT INTO users VALUES (1, 'Alice', 'US', 'active'), (2, 'Bob', 'UK', 'active'), (3, 'Charlie', 'US', 'inactive')")
    conn.commit()
    conn.close()

    nd.connect_and_initialize(db_uri)

    # 第一个 Query (Stage 2 & 4 LLM Generation)
    # Mock Goal Identifier Output (Stage 2)
    nd._llm.generate.side_effect = [
        # JSON for first Goal ID
        json.dumps({
            "operation": "select", "time_frame": "none", "filters": ["region = 'US'"],
            "grouping": [], "metrics": [], "relevant_tables": ["users"],
            "intent_summary": "Looking for US users"
        }),
        # SQL for first Planner (Stage 4)
        "SELECT * FROM users WHERE region = 'US'"
    ]

    print("\n--- QUERY 1: 'Show me US users' ---")
    res1 = nd.ask("Show me US users")
    print(f"SQL Generated: {res1.sql}")
    print(f"Rows: {res1.rows}")

    # 第二个 Query (Stage 3 Reference Resolver Flow)
    # This time we only need the LLM to identify the NEW goal.
    # The Resolver will merge the US filter from res1 above.
    nd._llm.generate.side_effect = [
        # JSON for second Goal ID (Follow-up) -> Stage 2
        json.dumps({
            "operation": "follow_up", "time_frame": "none", "filters": ["status = 'active'"],
            "grouping": [], "metrics": [], "relevant_tables": [],
            "intent_summary": "And just the active ones?"
        }),
        # SQL for second Planner (Stage 4)
        "SELECT * FROM users WHERE region = 'US' AND status = 'active'"
    ]

    print("\n--- QUERY 2 (Follow-up): 'And just the active ones?' ---")
    res2 = nd.ask("And just the active ones?")
    print(f"SQL: {res2.sql}")
    print(f"Outcome: {res2.rows} (Should only show US + Active)")

    # 第三个 Query (Stage 1 Vector Cache Flow)
    # We ask the first question again slightly differently
    print("\n--- QUERY 3 (Semantic Cache): 'What US users do we have?' ---")
    res3 = nd.ask("What US users do we have?")
    print(f"SQL: {res3.sql}")
    print(f"From Cache: {res3.from_cache}")

if __name__ == "__main__":
    simulate()
