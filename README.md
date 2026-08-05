# Local Data Warehouse with PySpark + Iceberg

This repository processes raw booking JSON data with PySpark and writes the cleaned output into a local Iceberg table.

## What is included

- `job/booking_job.py` — main ETL entry point
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
   cd /home/sajib/Desktop/cron-job
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
cd /home/sajib/Desktop/cron-job
source venv/bin/activate
python job/booking_job.py
```

> Running from inside `job/` may fail because Python cannot resolve the top-level `config` package. Use the project root or the full script path.

## What happens when you run it

- Reads raw JSON from `data/sample_booking.json`
- Cleans and transforms the data
- Writes to the Iceberg table `local.booking_db.bookings`

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
│   └── sample_booking.json
├── job/
│   └── booking_job.py
├── repository/
│   └── iceberg_repository.py
├── service/
│   ├── extractor.py
│   └── transformer.py
├── utils/
│   ├── mapping_loader.py
│   └── *.json
├── warehouse/
├── requirements.txt
└── README.md
```

## Next step

Add a validation step to read back a few rows from the Iceberg table after writing, if you want to verify the output automatically.
