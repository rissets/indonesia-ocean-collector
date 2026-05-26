# Local VMS Tracking Import

Use this from a local machine connected to the VPN that can reach `192.168.12.126:5432`.
The script forces every source DB transaction to `READ ONLY`; only the target `maritime-os`
database is written.

## Environment

Set these variables in your shell or in `.env`:

```bash
export VMS_SOURCE_HOST=192.168.12.126
export VMS_SOURCE_PORT=5432
export VMS_SOURCE_DB=VMS
export VMS_SOURCE_USER=execusr
export VMS_SOURCE_PASSWORD='p@5uk4n4nt1t1dur'

export DB_HOST=217.217.252.155
export DB_PORT=5432
export DB_NAME=maritime-os
export DB_USER=maritime-os
export DB_PASSWORD='@Maritime210526'
```

You can also use `MARITIME_TARGET_DSN` instead of the `DB_*` variables.

## Plan Only

This checks source privileges, source date range, and chunk sizes.

```bash
python scripts/import_vms_tracking.py --plan-only --chunk year
```

Plan weekly chunks for a large month:

```bash
python scripts/import_vms_tracking.py --start 2025-06-01 --end 2025-06-30 --plan-only --chunk week
```

To split any year estimated above 1 GB into monthly chunks:

```bash
python scripts/import_vms_tracking.py --plan-only --chunk year --max-bytes 1000000000
```

## Dump And Import

Import all available source data, chunked per year, auto-splitting large years by month:

```bash
python scripts/import_vms_tracking.py --chunk year --max-bytes 1000000000
```

Import a specific year:

```bash
python scripts/import_vms_tracking.py --start 2021-01-01 --end 2021-12-31 --chunk year
```

Import a large month in weekly chunks:

```bash
python scripts/import_vms_tracking.py --start 2025-06-01 --end 2025-06-30 --chunk week
```

Dump only, without loading into `maritime-os`:

```bash
python scripts/import_vms_tracking.py --start 2021-01-01 --end 2021-12-31 --dump-only
```

Load existing dumps only:

```bash
python scripts/import_vms_tracking.py --start 2025-06-01 --end 2025-06-30 --chunk week --load-only
```

Dump files are written to `data/vms_dumps/*.csv.gz`.

## Mapping

Source `public.api_pusdal_lastdatavms_det` maps to `master_vessel_tracking`:

- `nomor_buku_kapal` -> `nomor_bkp`
- `transmitter_no` -> `transmitter_no`
- `ping_time` -> `timestamp`
- `last_latitude` -> `latitude`
- `last_longitude` -> `longitude`
- `heading` -> `direction`
- `speed` -> `speed`
- `source` -> `VMS_DB`

`nama_kapal` is resolved from `master_kapal` using `nomor_bkp` or `transmitter_no`.
Rows are inserted idempotently; existing `(nomor_bkp, transmitter_no, timestamp, latitude, longitude)`
records are skipped.
