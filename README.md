# Indonesia Ocean Fishing Ground Collector

Python project untuk mengumpulkan data oseanografi dan aktivitas perikanan di perairan Indonesia dari berbagai sumber, lalu memetakannya ke dalam CSV yang rapi dan terstruktur.

## Fitur

- Collect data dari **3 sumber**: NOAA ERDDAP, Copernicus Marine (CMEMS), Global Fishing Watch (GFW)
- **Configurable**: date range, max records, WPP region, grid resolution
- **Mock mode**: CMEMS dan GFW otomatis fallback ke data realistis jika credentials tidak tersedia
- Output **unified CSV** dengan Fishing Ground Index per grid cell
- Mencakup **11 WPP** (Wilayah Pengelolaan Perikanan) Indonesia

## Output Schema

| Kolom | Deskripsi | Satuan |
|-------|-----------|--------|
| `month` | Periode bulan | YYYY-MM |
| `lat_grid` | Latitude grid centre | degrees |
| `lon_grid` | Longitude grid centre | degrees |
| `sst_mean` | Rata-rata Sea Surface Temperature | °C |
| `chlorophyll_mean` | Rata-rata klorofil-a | mg/m³ |
| `u_current_mean` | Arus laut arah timur | m/s |
| `v_current_mean` | Arus laut arah utara | m/s |
| `ssh_mean` | Sea Surface Height anomaly | m |
| `fishing_effort_hours` | Total jam aktivitas penangkapan | hours |
| `vessel_count` | Jumlah kapal | count |
| `fishing_ground_index` | Indeks potensi fishing ground (0–1) | - |
| `wpp_region` | Wilayah Pengelolaan Perikanan | - |
| `data_sources` | Sumber data yang digunakan | - |

## Instalasi

```bash
git clone https://github.com/YOUR_USERNAME/indonesia-ocean-collector.git
cd indonesia-ocean-collector

python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

pip install -r requirements.txt

cp .env.example .env
# Edit .env dengan credentials Anda
```

## Konfigurasi Credentials

Edit file `.env`:

```env
# Copernicus Marine — daftar di https://marine.copernicus.eu
COPERNICUSMARINE_SERVICE_USERNAME=your_username
COPERNICUSMARINE_SERVICE_PASSWORD=your_password

# Global Fishing Watch — daftar di https://globalfishingwatch.org/data-download
GFW_API_TOKEN=your_token
```

> Tanpa credentials, CMEMS dan GFW otomatis menggunakan **mock data** yang realistis (ditandai `source=CMEMS_MOCK` / `GFW_MOCK`). NOAA ERDDAP tidak memerlukan login.

## Penggunaan

### Basic — semua sumber, Jan–Mar 2023

```bash
python main.py --start 2023-01-01 --end 2023-03-31
```

### Custom date range dan jumlah data

```bash
python main.py --start 2022-06-01 --end 2022-12-31 --max-records 2000
```

### Pilih sumber tertentu

```bash
# Hanya ERDDAP (tidak butuh login)
python main.py --start 2023-01-01 --end 2023-06-30 --sources erddap

# ERDDAP + CMEMS
python main.py --start 2023-01-01 --end 2023-03-31 --sources erddap cmems
```

### Filter WPP tertentu

```bash
python main.py --start 2023-01-01 --end 2023-12-31 \
  --wpp WPP_712_Laut_Jawa WPP_713_Selat_Makassar WPP_714_Laut_Banda
```

### Custom output dan grid resolution

```bash
python main.py --start 2023-01-01 --end 2023-03-31 \
  --output my_output/ \
  --grid-resolution 1.0
```

### Semua opsi

```
python main.py --help

options:
  --start YYYY-MM-DD      Start date (default: 2023-01-01)
  --end YYYY-MM-DD        End date (default: 2023-03-31)
  --max-records N         Max records per source (default: 1000)
  --sources SOURCE [...]  erddap cmems gfw (default: all)
  --wpp WPP [...]         WPP regions to include (default: all 11 WPP)
  --output DIR            Output directory (default: data/processed)
  --raw-dir DIR           Raw data directory (default: data/raw)
  --grid-resolution DEG   Grid resolution in degrees (default: 0.5)
```

## WPP Indonesia yang Didukung

| WPP | Nama | Cakupan |
|-----|------|---------|
| WPP_571 | Selat Malaka | Selat Malaka & Laut Andaman |
| WPP_572 | Samudera Hindia Barat | Barat Sumatera |
| WPP_573 | Samudera Hindia Selatan | Selatan Jawa–Nusa Tenggara |
| WPP_711 | Laut Natuna | Selat Karimata, Laut China Selatan |
| WPP_712 | Laut Jawa | Laut Jawa |
| WPP_713 | Selat Makassar | Selat Makassar, Teluk Bone, Laut Flores |
| WPP_714 | Laut Banda | Laut Banda |
| WPP_715 | Laut Maluku | Laut Maluku, Laut Halmahera, Laut Seram |
| WPP_716 | Laut Sulawesi | Laut Sulawesi, Samudera Pasifik Utara Papua |
| WPP_717 | Teluk Cendrawasih | Teluk Cendrawasih, Samudera Pasifik |
| WPP_718 | Laut Arafuru | Laut Aru, Laut Arafuru, Laut Timor |

## Fishing Ground Index

FGI dihitung sebagai kombinasi klorofil-a (produktivitas laut) dan SST (suhu optimal):

```
FGI = 0.6 × chl_norm + 0.4 × sst_score

chl_norm  = min-max normalisasi klorofil
sst_score = 1 - |SST - 27.5°C| / 10  (clipped 0–1)
```

Nilai FGI mendekati **1.0** = potensi fishing ground tinggi.

## Struktur Project

```
indonesia-ocean-collector/
├── config.py                  # Konfigurasi pusat (bbox, WPP, defaults)
├── main.py                    # CLI entrypoint
├── collectors/
│   ├── erddap_collector.py    # NOAA ERDDAP (tanpa login)
│   ├── cmems_collector.py     # Copernicus Marine
│   └── gfw_collector.py       # Global Fishing Watch
├── processors/
│   └── mapper.py              # Merge semua sumber → unified CSV
├── sample_data/               # Sample output (committed)
│   ├── fishing_ground_20230101_20230331.csv
│   └── raw/
├── requirements.txt
└── .env.example
```

## Sumber Data

- **NOAA ERDDAP**: https://coastwatch.pfeg.noaa.gov/erddap
- **Copernicus Marine**: https://marine.copernicus.eu
- **Global Fishing Watch**: https://globalfishingwatch.org
- **KKP WPP Reference**: https://kkp.go.id
