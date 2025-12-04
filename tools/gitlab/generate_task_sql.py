#!/usr/bin/env python3
import os
import sys
import json
from datetime import datetime

def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def write_sql(dir_path, table_name, sources, author):
    fname = f"{table_name}_process.sql"
    path = os.path.join(dir_path, fname)
    create_time = datetime.utcnow().strftime('%Y-%m-%d')
    header = [
        f"-- @source_tables: {','.join(sources)}",
        f"-- @target_table: {table_name}",
        f"-- @author: {author}",
        f"-- @create_time: {create_time}",
        "",
    ]
    body = [
        f"-- TODO: add SQL to process into {table_name}",
        f"-- Example: INSERT INTO {table_name} SELECT * FROM {sources[0]}"
    ]
    with open(path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(header + body))
    print(f"Generated {path}")

def main():
    if len(sys.argv) < 3:
        print("Usage: generate_task_sql.py <spec.json> <output_dir> [author]", file=sys.stderr)
        sys.exit(1)
    spec_path, out_dir = sys.argv[1], sys.argv[2]
    author = sys.argv[3] if len(sys.argv) > 3 else os.getenv('GITLAB_USER_NAME', 'developer')
    ensure_dir(out_dir)
    spec = json.load(open(spec_path))
    for item in spec:
        write_sql(out_dir, item['target_table'], item.get('source_tables', []), author)

if __name__ == '__main__':
    main()
