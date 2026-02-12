"""
Generate Step Functions input and start the pipeline.

Usage:
  python trigger_pipeline.py 2024-01-01 2025-01-01
  python trigger_pipeline.py 2024-01-01 2025-01-01 --chunk-days 30
  python trigger_pipeline.py 2024-01-01 2025-01-01 --dry-run
"""
import argparse
import json
import os
import uuid
from datetime import datetime, timedelta

import boto3


def generate_chunks(start_date: str, end_date: str, chunk_days: int = 1):
    """Split a date range into chunks of chunk_days."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    chunks = []
    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_days), end)
        chunks.append({
            "start_date": current.strftime("%Y-%m-%d"),
            "end_date": chunk_end.strftime("%Y-%m-%d"),
        })
        current = chunk_end

    return chunks


def main():
    parser = argparse.ArgumentParser(description="Trigger ADS-B map-reduce pipeline")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD, inclusive)")
    parser.add_argument("end_date", help="End date (YYYY-MM-DD, exclusive)")
    parser.add_argument("--chunk-days", type=int, default=1,
                        help="Days per chunk (default: 1)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print input JSON without starting execution")
    args = parser.parse_args()

    run_id = f"run-{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:8]}"
    chunks = generate_chunks(args.start_date, args.end_date, args.chunk_days)

    # Inject run_id into each chunk
    for chunk in chunks:
        chunk["run_id"] = run_id

    sfn_input = {
        "run_id": run_id,
        "global_start_date": args.start_date,
        "global_end_date": args.end_date,
        "chunks": chunks,
    }

    print(f"Run ID:    {run_id}")
    print(f"Chunks:    {len(chunks)} (at {args.chunk_days} days each)")
    print(f"Max concurrency: 3 (enforced by Step Functions Map state)")
    print()
    print(json.dumps(sfn_input, indent=2))

    if args.dry_run:
        print("\n--dry-run: not starting execution")
        return

    client = boto3.client("stepfunctions")

    # Find the state machine ARN
    machines = client.list_state_machines()["stateMachines"]
    arn = next(
        m["stateMachineArn"]
        for m in machines
        if m["name"] == "adsb-map-reduce"
    )

    response = client.start_execution(
        stateMachineArn=arn,
        name=run_id,
        input=json.dumps(sfn_input),
    )

    print(f"\nStarted execution: {response['executionArn']}")


if __name__ == "__main__":
    main()
