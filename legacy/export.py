import csv


def to_csv(data, filename, fieldnames=None):
    """Export a list of dicts to a CSV file.

    Args:
        data: List of dicts to export.
        filename: Output CSV path.
        fieldnames: Column headers. If None, derived from the first record.

    Returns:
        The filename written.
    """
    if not data:
        print("No data to export.")
        return None

    if fieldnames is None:
        fieldnames = list(data[0].keys())

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

    print(f"Exported {len(data)} rows to {filename}")
    return filename
