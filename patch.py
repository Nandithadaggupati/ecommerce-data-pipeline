import glob

files = [
    'scripts/data_generation/generate_data.py',
    'scripts/ingestion/ingest_to_staging.py',
    'scripts/quality_checks/validate_data.py',
    'scripts/transformation/staging_to_production.py',
    'scripts/transformation/load_warehouse.py'
]

for filepath in files:
    with open(filepath, 'r') as f:
        content = f.read()

    # Rename keys for tests
    content = content.replace('"timestamp":', '"generated_at":')
    
    if 'if __name__ == "__main__":' in content:
        parts = content.split('if __name__ == "__main__":')
        main_code = parts[1]
        
        # We need to preserve the indent for the function
        # Since it was originally under if __name__ == "__main__": it is already indented
        new_content = parts[0] + "def main():\n" + main_code + "\nif __name__ == '__main__':\n    main()\n"
        
        with open(filepath, 'w') as f:
            f.write(new_content)
    print(f"Patched {filepath}")
