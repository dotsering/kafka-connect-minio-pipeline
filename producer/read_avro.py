import fastavro
import json
import sys

# Usage: python read_avro.py <path_to_file>
filename = sys.argv[1] if len(sys.argv) > 1 else "users+0+0000000000.avro"

try:
    with open(filename, 'rb') as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        print(f"--- Schema found in file ---")
        print(json.dumps(schema, indent=2))
        print(f"\n--- Data Records ---")

        for record in reader:
            print(json.dumps(record, indent=2))

except FileNotFoundError:
    print(f"Error: Could not find file '{filename}'.")
    print("Make sure you downloaded it from MinIO and placed it in this folder.")