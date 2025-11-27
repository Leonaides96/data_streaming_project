Technologies
- Kafka (Docker) — message streaming
- Python Producer — simulates trading data
- Python Consumer — ETL: cleaning + storing

Storage (choose one):
✔ Local JSON/CSV
✔ SQLite
✔ or S3 later on AWS


Architect design:

- Python Producer → Kafka Topic → Python Consumer (ETL) → Local Database/CSV