#!/usr/bin/env python3
"""
Generate CSV reports from JSON stats files.

Usage: python3 generate_reports.py

Reads JSON files from output/ and generates CSV reports in reports/
"""

import json
import csv
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime


def load_json(filepath):
    """Load a JSON file."""
    with open(filepath, 'r') as f:
        return json.load(f)


def generate_records_by_day_csv(input_file, output_file):
    """Generate CSV for records by day."""
    data = load_json(input_file)

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'Records'])

        total = 0
        for date, count in sorted(data.items()):
            writer.writerow([date, count])
            total += count

        writer.writerow([])
        writer.writerow(['TOTAL', total])

    print(f"Wrote: {output_file}")


def generate_records_by_month_csv(input_file, output_file):
    """Generate CSV for records by month."""
    data = load_json(input_file)

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Month', 'Records'])

        total = 0
        for month, count in sorted(data.items()):
            writer.writerow([month, count])
            total += count

        writer.writerow([])
        writer.writerow(['TOTAL', total])

    print(f"Wrote: {output_file}")


def generate_user_month_csv(input_file, output_file, value_label="Records"):
    """Generate CSV for user by month data with month columns."""
    data = load_json(input_file)

    # Get all unique months across all users
    all_months = set()
    for user_data in data.values():
        all_months.update(user_data.keys())
    months_sorted = sorted(all_months)

    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)

        # Header row
        header = ['User'] + months_sorted + ['Total']
        writer.writerow(header)

        # Data rows sorted by user
        month_totals = {month: 0 for month in months_sorted}
        grand_total = 0

        for user in sorted(data.keys()):
            user_data = data[user]
            row = [user]
            user_total = 0

            for month in months_sorted:
                count = user_data.get(month, 0)
                row.append(count)
                user_total += count
                month_totals[month] += count

            row.append(user_total)
            grand_total += user_total
            writer.writerow(row)

        # Totals row
        writer.writerow([])
        totals_row = ['TOTAL'] + [month_totals[m] for m in months_sorted] + [grand_total]
        writer.writerow(totals_row)

    print(f"Wrote: {output_file}")


def generate_by_day_histogram(input_file, output_file, start_date, end_date, title, ylabel, color='steelblue'):
    """Generate a histogram PNG for data by day."""
    data = load_json(input_file)

    # Parse dates and counts
    dates = [datetime.strptime(d, '%Y-%m-%d') for d in sorted(data.keys())]
    counts = [data[d.strftime('%Y-%m-%d')] for d in dates]

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 6))

    # Plot as bar chart
    ax.bar(dates, counts, width=1, edgecolor='none', alpha=0.8, color=color)

    # Format x-axis
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_minor_locator(mdates.WeekdayLocator(byweekday=mdates.MO))

    plt.xticks(rotation=45, ha='right')

    # Labels and title
    ax.set_xlabel('Date')
    ax.set_ylabel(ylabel)
    ax.set_title(f'{title} ({start_date} to {end_date})')

    # Add grid
    ax.grid(axis='y', alpha=0.3)

    # Tight layout
    plt.tight_layout()

    # Save
    plt.savefig(output_file, dpi=150)
    plt.close()

    print(f"Wrote: {output_file}")


def get_date_range(output_dir):
    """Get date range from records_by_day.json."""
    data = load_json(output_dir / "records_by_day.json")
    dates = sorted(data.keys())
    if dates:
        return dates[0], dates[-1]
    return "unknown", "unknown"


def main():
    output_dir = Path(__file__).parent / "output"
    reports_dir = Path(__file__).parent / "reports"
    reports_dir.mkdir(exist_ok=True)

    print("Generating reports...\n")

    # Get date range for filenames
    start_date, end_date = get_date_range(output_dir)
    date_suffix = f"_{start_date}_to_{end_date}"

    # Records by day
    if (output_dir / "records_by_day.json").exists():
        generate_records_by_day_csv(
            output_dir / "records_by_day.json",
            reports_dir / f"marva_records_by_day{date_suffix}.csv"
        )
        generate_by_day_histogram(
            output_dir / "records_by_day.json",
            reports_dir / f"marva_records_by_day{date_suffix}.png",
            start_date,
            end_date,
            'MARVA Published Records by Day',
            'Records Published'
        )

    # Records by month
    if (output_dir / "records_by_month.json").exists():
        generate_records_by_month_csv(
            output_dir / "records_by_month.json",
            reports_dir / f"marva_records_by_month{date_suffix}.csv"
        )

    # Records by user by month
    if (output_dir / "records_by_user_month.json").exists():
        generate_user_month_csv(
            output_dir / "records_by_user_month.json",
            reports_dir / f"marva_records_by_user_month{date_suffix}.csv",
            "Records"
        )

    # NARs by day
    if (output_dir / "nars_by_day.json").exists():
        generate_records_by_day_csv(
            output_dir / "nars_by_day.json",
            reports_dir / f"marva_nars_by_day{date_suffix}.csv"
        )
        generate_by_day_histogram(
            output_dir / "nars_by_day.json",
            reports_dir / f"marva_nars_by_day{date_suffix}.png",
            start_date,
            end_date,
            'MARVA New NARs by Day',
            'New NARs',
            color='darkorange'
        )

    # NARs by month
    if (output_dir / "nars_by_month.json").exists():
        generate_records_by_month_csv(
            output_dir / "nars_by_month.json",
            reports_dir / f"marva_nars_by_month{date_suffix}.csv"
        )

    # NARs by user by month
    if (output_dir / "nars_by_user_month.json").exists():
        generate_user_month_csv(
            output_dir / "nars_by_user_month.json",
            reports_dir / f"marva_nars_by_user_month{date_suffix}.csv",
            "NARs"
        )

    print("\nDone!")


if __name__ == "__main__":
    main()
