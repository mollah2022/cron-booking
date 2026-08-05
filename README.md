# Data Warehouse (PySpark + Iceberg - Local)

Ei project e raw booking JSON data ke PySpark diye process kore, Apache Iceberg format e local machine e save kora hocche (no S3, no Docker - shob local).

## Prerequisites
- Python 3.9+
- Java 8 or 11 (Spark er jonno lage)

## Setup (First Time Only)

1. Virtual environment banao:
   ```
   python -m venv venv
   source venv/bin/activate      # Windows: venv\Scripts\activate
   ```

2. Dependencies install koro:
   ```
   pip install -r requirements.txt
   ```

## Project Structure
```
data_warehouse/
├── job/            -> main entry point (job run kora hoy ekhan theke)
├── service/         -> extractor, transformer, loader classes
├── utils/            -> spark session, mapping loader helpers
├── config/           -> status_mapping.json, region_mapping.json, settings.py
├── warehouse/        -> Iceberg data + metadata (auto-generated, git e push hoy na)
├── requirements.txt
└── README.md
```

## How to Run
(Ei part porer step e update hobe, jokhon job.py toiri hobe)
```
python job/booking_job.py
```

## Notes
- Kono Docker lage na - shudhu local Python + Java environment thakle e cholbe.
- `warehouse/` folder e Iceberg nijei data+metadata generate korbe - eita `.gitignore` e ache, tai GitHub e push hobe na.
