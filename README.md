# Local Data Warehouse with PySpark + Iceberg

This repository processes raw booking JSON data with PySpark and writes the cleaned output into a local Iceberg table.

## What is included

- `job/` — ETL entrypoints and runners, including normal run and batch migration
- `service/extractor.py` — read raw JSON into a Spark DataFrame
- `service/transformer.py` — clean and transform the data into final schema
- `repository/iceberg_repository.py` — create or append to the Iceberg table
- `utils/mapping_loader.py` — load status and region mappings
- `config/` — project configuration and settings
- `warehouse/` — local Iceberg data and metadata output

## Requirements

- Python 3.9+
- Java 8 or 11

## Setup

1. Go to the project root:

   ```bash
   cd /home/w3e57/Desktop/cron-booking
   ```

2. Create and activate a virtual environment:

   ```bash
   python -m venv venv
   source venv/bin/activate
   ```

3. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

## Run the job

Run the pipeline from the repository root:

```bash
cd /home/w3e57/Desktop/cron-booking
source venv/bin/activate
python job/cli.py
```

To run the batch migration flow instead of the normal run:

```bash
python job/cli.py --batch
```

> Running from inside `job/` may fail because Python cannot resolve the top-level `config` package. Use the project root or the full script path.

## What happens when you run it

- Reads raw JSON from `data/bookings_large.jsonl`
- Cleans and transforms the data
- Writes to the Iceberg table defined in `config/config.toml`

## Notes

- `warehouse/` stores Iceberg metadata and data files
- This folder should be ignored by Git so the generated files are not committed
- The first run creates the table, later runs append new rows

## Directory structure

```
cron-job/
├── config/
│   ├── config.toml
│   ├── settings.py
│   └── spark_session.py
├── data/
│   └── bookings_large.jsonl
├── job/
│   ├── batch_migration/
│   ├── cli.py
│   ├── common.py
│   ├── normal_run.py
│   └── verify_booking_table.py
├── repository/
│   └── iceberg_repository.py
├── service/
│   ├── extractor.py
│   ├── exchange_rate_service.py
│   ├── transformer.py
│   └── validator.py
├── utils/
│   ├── mapping_loader.py
│   ├── exceptions.py
│   ├── logger.py
│   ├── region_mapping.json
│   └── status_mapping.json
├── warehouse/
├── requirements.txt
└── README.md
```

## Next step

Add a validation step to read back a few rows from the Iceberg table after writing, if you want to verify the output automatically.
