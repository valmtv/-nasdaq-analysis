#!/usr/bin/env python3
"""
Databricks AI/BI Dashboard Extraction Utility
---------------------------------------------
Exports an active AI/BI Dashboard (Lakeview) from Databricks workspace as a 
serialized `.lvdash.json` asset for Infrastructure-as-Code (Databricks Asset Bundles) 
or cross-workspace migration.

Usage:
    python3 export_dashboard.py
    python3 export_dashboard.py --name "NASDAQ Market Radar" --output 06_dashboard/nasdaq_market_radar.lvdash.json
    python3 export_dashboard.py --id 01f1a7eb4a4b10fbb0102f20560e0f5e
    python3 export_dashboard.py --list
"""

import argparse
import json
import os
import subprocess
import sys


def run_databricks_cli(args, profile=None):
    cmd = ["databricks"] + args
    if profile:
        cmd.extend(["--profile", profile])
    try:
        res = subprocess.check_output(cmd, stderr=subprocess.PIPE, text=True)
        return json.loads(res)
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Databricks CLI command failed: {' '.join(cmd)}", file=sys.stderr)
        print(e.stderr, file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"[ERROR] Failed to parse CLI response as JSON: {e}", file=sys.stderr)
        sys.exit(1)


def list_dashboards(profile=None):
    return run_databricks_cli(["lakeview", "list"], profile=profile)


def get_dashboard(dashboard_id, profile=None):
    return run_databricks_cli(["lakeview", "get", dashboard_id], profile=profile)


def export_dashboard(dashboard_id=None, name_filter=None, output_path="06_dashboard/nasdaq_market_radar.lvdash.json", profile=None):
    dashboards = list_dashboards(profile=profile)
    if not dashboards:
        print("[ERROR] No AI/BI dashboards found in the workspace.")
        sys.exit(1)

    selected_dashboard = None

    if dashboard_id:
        for d in dashboards:
            if d.get("dashboard_id") == dashboard_id:
                selected_dashboard = d
                break
        if not selected_dashboard:
            print(f"[ERROR] Dashboard ID '{dashboard_id}' not found.")
            sys.exit(1)
    elif name_filter:
        matches = [d for d in dashboards if name_filter.lower() in d.get("display_name", "").lower()]
        if not matches:
            print(f"[ERROR] No dashboard found matching name: '{name_filter}'")
            sys.exit(1)
        selected_dashboard = matches[0]
    else:
        # Default to first active or matching NASDAQ
        nasdaq_matches = [d for d in dashboards if "nasdaq" in d.get("display_name", "").lower()]
        selected_dashboard = nasdaq_matches[0] if nasdaq_matches else dashboards[0]

    d_id = selected_dashboard.get("dashboard_id")
    d_name = selected_dashboard.get("display_name")
    print(f"[*] Extracting Dashboard: '{d_name}' (ID: {d_id})...")

    data = get_dashboard(d_id, profile=profile)
    serialized_str = data.get("serialized_dashboard")
    if not serialized_str:
        print("[ERROR] No 'serialized_dashboard' payload returned from Databricks API.")
        sys.exit(1)

    serialized = json.loads(serialized_str)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(serialized, f, indent=2)

    pages = serialized.get("pages", [])
    datasets = serialized.get("datasets", [])
    file_size = os.path.getsize(output_path)

    print("=" * 70)
    print(f"[SUCCESS] Dashboard exported to: {output_path}")
    print(f" - Display Name  : {d_name}")
    print(f" - Dashboard ID  : {d_id}")
    print(f" - Total Pages   : {len(pages)}")
    for i, p in enumerate(pages, 1):
        print(f"    Page {i}: {p.get('displayName', 'Untitled')}")
    print(f" - Total Datasets: {len(datasets)}")
    for ds in datasets:
        print(f"    - {ds.get('displayName', 'Unnamed')}")
    print(f" - File Size     : {file_size:,} bytes")
    print("=" * 70)
    print("[TIP] You can now deploy this dashboard to dev/prod via Databricks Asset Bundles:")
    print("      databricks bundle deploy -t prod_azure")
    print("      Or import it manually via Databricks UI -> Dashboards -> Import Dashboard.")


def main():
    parser = argparse.ArgumentParser(description="Extract Databricks AI/BI Dashboard as a serialized .lvdash.json asset")
    parser.add_argument("--id", help="Exact Dashboard ID to export (UUID)")
    parser.add_argument("--name", default="NASDAQ", help="Case-insensitive substring search for dashboard name")
    parser.add_argument("--output", default="06_dashboard/nasdaq_market_radar.lvdash.json", help="Destination file path for .lvdash.json")
    parser.add_argument("--profile", help="Databricks CLI authentication profile (~/.databrickscfg)")
    parser.add_argument("--list", action="store_true", help="List all available dashboards in the workspace and exit")

    args = parser.parse_args()

    if args.list:
        dashboards = list_dashboards(profile=args.profile)
        print(f"Found {len(dashboards)} AI/BI Dashboard(s):")
        for d in dashboards:
            print(f" - {d.get('dashboard_id')} : {d.get('display_name')} (State: {d.get('lifecycle_state')})")
        return

    export_dashboard(
        dashboard_id=args.id,
        name_filter=args.name,
        output_path=args.output,
        profile=args.profile
    )


if __name__ == "__main__":
    main()
