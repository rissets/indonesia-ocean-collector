-- Migration: 001_schema.sql
-- Creates all master tables for the maritime-os database
-- Run as superuser: psql -U maritime-os -d maritime-os -f 001_schema.sql

-- Enable pgcrypto for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- ============================================================
-- master_wpp (referenced by vessel tracking, create first)
-- ============================================================
CREATE TABLE IF NOT EXISTS master_wpp (
    id                  SERIAL PRIMARY KEY,
    kode_wpp            VARCHAR(10)    NOT NULL UNIQUE,
    nama_wpp            VARCHAR(200)   NOT NULL,
    wilayah_perairan    TEXT,
    luas_km2            DECIMAL(12,2),
    batas_utara         VARCHAR(150),
    batas_selatan       VARCHAR(150),
    batas_barat         VARCHAR(150),
    batas_timur         VARCHAR(150),
    provinsi_terkait    TEXT,
    koordinat_pusat_lat DECIMAL(9,6),
    koordinat_pusat_lon DECIMAL(9,6),
    geojson_polygon     JSONB,
    status              VARCHAR(20)    NOT NULL DEFAULT 'aktif',
    created_at          TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_master_wpp_kode ON master_wpp (kode_wpp);

-- Seed: 12 official WPP regions (Permen KP No. 18/2014)
INSERT INTO master_wpp (kode_wpp, nama_wpp) VALUES
    ('WPP-711', 'Selat Karimata, Laut Natuna, dan Laut China Selatan'),
    ('WPP-712', 'Laut Jawa'),
    ('WPP-713', 'Selat Makassar, Teluk Bone, Laut Flores, dan Laut Bali'),
    ('WPP-714', 'Teluk Tolo dan Laut Banda'),
    ('WPP-715', 'Teluk Tomini, Laut Maluku, Laut Halmahera, Laut Seram, dan Teluk Berau'),
    ('WPP-716', 'Laut Sulawesi dan Sebelah Utara Pulau Halmahera'),
    ('WPP-717', 'Teluk Cendrawasih dan Samudera Pasifik'),
    ('WPP-718', 'Laut Aru, Laut Arafuru, dan Laut Timor Bagian Timur'),
    ('WPP-571', 'Selat Malaka dan Laut Andaman'),
    ('WPP-572', 'Samudera Hindia Sebelah Barat Sumatera dan Selat Sunda'),
    ('WPP-573', 'Samudera Hindia Sebelah Selatan Jawa, Bali, dan Nusa Tenggara'),
    ('WPP-574', 'Laut Timor Bagian Barat')
ON CONFLICT (kode_wpp) DO NOTHING;

-- ============================================================
-- master_jenis_ikan
-- ============================================================
CREATE TABLE IF NOT EXISTS master_jenis_ikan (
    id          SERIAL PRIMARY KEY,
    nama_lokal  VARCHAR(100),
    nama_ilmiah VARCHAR(150),
    kode_fao    VARCHAR(10),
    kategori    VARCHAR(50)
);

CREATE INDEX IF NOT EXISTS idx_master_jenis_ikan_kode_fao ON master_jenis_ikan (kode_fao);

-- ============================================================
-- master_kapal
-- ============================================================
CREATE TABLE IF NOT EXISTS master_kapal (
    id                  UUID          NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    nama_kapal          VARCHAR(150),
    nomor_bkp           VARCHAR(50)   UNIQUE,
    tanda_selar         VARCHAR(100),
    ukuran_kapal        DECIMAL(8,2),
    no_transmitter      VARCHAR(100)  UNIQUE,
    pemilik             VARCHAR(150),
    alat_tangkap        VARCHAR(100),
    kekuatan_mesin      DECIMAL(8,2),
    merek_mesin         VARCHAR(100),
    wilayah_tangkap     VARCHAR(100),
    pelabuhan_pangkalan VARCHAR(150),
    aktif               BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_master_kapal_nomor_bkp      ON master_kapal (nomor_bkp);
CREATE INDEX IF NOT EXISTS idx_master_kapal_no_transmitter ON master_kapal (no_transmitter);

-- ============================================================
-- master_oceanography
-- ============================================================
CREATE TABLE IF NOT EXISTS master_oceanography (
    id                UUID          NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    tanggal           DATE,
    latitude          DECIMAL(9,6),
    longitude         DECIMAL(9,6),
    suhu_permukaan    DECIMAL(5,2),
    sst               DECIMAL(5,2),
    ssh               DECIMAL(6,3),
    klorofil          DECIMAL(8,4),
    arus_laut_u       DECIMAL(6,3),
    arus_laut_v       DECIMAL(6,3),
    kecepatan_arus    DECIMAL(6,3),
    tinggi_gelombang  DECIMAL(5,2),
    kecepatan_angin   DECIMAL(5,2),
    arah_angin        DECIMAL(5,2),
    kedalaman_laut    DECIMAL(8,2),
    radiasi_matahari  DECIMAL(8,2),
    jarak_padang      DECIMAL(8,3),
    cuaca             VARCHAR(50),
    pasang_surut      DECIMAL(5,3),
    sumber_data       VARCHAR(100),
    created_at        TIMESTAMP     NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_master_oceanography_lat_lon  ON master_oceanography (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_master_oceanography_tanggal  ON master_oceanography (tanggal);

-- ============================================================
-- master_vessel_tracking
-- ============================================================
CREATE TABLE IF NOT EXISTS master_vessel_tracking (
    id              UUID          NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    nama_kapal      VARCHAR(100),
    nomor_bkp       VARCHAR(50),
    transmitter_no  VARCHAR(100),
    mmsi            VARCHAR(20),
    latitude        DECIMAL(9,6),
    longitude       DECIMAL(9,6),
    direction       DECIMAL(5,2),
    speed           DECIMAL(5,2),
    timestamp       TIMESTAMP,
    status_kapal    VARCHAR(30),
    wpp_id          INTEGER       REFERENCES master_wpp (id) ON DELETE SET NULL,
    source          VARCHAR(20),
    created_at      TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_master_vessel_tracking_lat_lon       ON master_vessel_tracking (latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_master_vessel_tracking_timestamp     ON master_vessel_tracking (timestamp);
CREATE INDEX IF NOT EXISTS idx_master_vessel_tracking_nomor_bkp     ON master_vessel_tracking (nomor_bkp);
CREATE INDEX IF NOT EXISTS idx_master_vessel_tracking_transmitter   ON master_vessel_tracking (transmitter_no);

-- ============================================================
-- master_tangkapan_pipp
-- ============================================================
CREATE TABLE IF NOT EXISTS master_tangkapan_pipp (
    id               UUID           NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    nama_kapal       VARCHAR(150),
    nomor_bkp        VARCHAR(50),
    tanggal_bongkar  DATE,
    pelabuhan        VARCHAR(150),
    pelabuhan_kode   VARCHAR(20),
    total_tangkapan  DECIMAL(10,2),
    jenis_ikan       VARCHAR(100),
    kode_ikan        VARCHAR(20),
    berat_per_jenis  DECIMAL(10,2),
    nilai_tangkapan  DECIMAL(15,2),
    harga_per_kg     DECIMAL(10,2),
    wpp_tangkap      VARCHAR(20),
    trip_ke          INTEGER,
    lama_trip        INTEGER,
    jumlah_abk       INTEGER,
    sumber_data      VARCHAR(50),
    created_at       TIMESTAMP      NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_master_tangkapan_pipp_nomor_bkp      ON master_tangkapan_pipp (nomor_bkp);
CREATE INDEX IF NOT EXISTS idx_master_tangkapan_pipp_tanggal_bongkar ON master_tangkapan_pipp (tanggal_bongkar);

-- ============================================================
-- readonly_user: create if not exists, then grant SELECT
-- ============================================================
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'readonly_user') THEN
        CREATE ROLE readonly_user WITH LOGIN PASSWORD '@MaritimeRO';
    END IF;
END
$$;

GRANT CONNECT ON DATABASE "maritime-os" TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;
GRANT SELECT ON
    master_wpp,
    master_jenis_ikan,
    master_kapal,
    master_oceanography,
    master_vessel_tracking,
    master_tangkapan_pipp
TO readonly_user;

-- Also grant SELECT on any future tables in public schema
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO readonly_user;
