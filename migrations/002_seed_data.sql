-- 002_seed_data.sql
-- Seed reference data for master_wpp and master_jenis_ikan.
-- Idempotent: ON CONFLICT DO NOTHING on all inserts.
-- Source: Permen KP No. 18/2014 (WPP), FAO species list + KKP common catch species.

-- ---------------------------------------------------------------------------
-- master_wpp — 12 WPP regions
-- Bounding boxes from config.py WPP_REGIONS; centroids computed from bbox midpoints.
-- GeoJSON polygons are rectangular approximations (SW→SE→NE→NW→SW).
-- ---------------------------------------------------------------------------

INSERT INTO master_wpp (
    kode_wpp, nama_wpp, wilayah_perairan,
    batas_wilayah,
    koordinat_pusat_lat, koordinat_pusat_lon,
    provinsi_terkait,
    status
) VALUES

-- WPP 571
(
    '571',
    'WPP 571 - Selat Malaka dan Laut Andaman',
    'Selat Malaka dan Laut Andaman',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[95.0,1.0],[104.0,1.0],[104.0,6.0],[95.0,6.0],[95.0,1.0]]]}'),
    3.5, 99.5,
    'Aceh, Sumatera Utara',
    'aktif'
),

-- WPP 572
(
    '572',
    'WPP 572 - Samudera Hindia Sebelah Barat Sumatera',
    'Samudera Hindia sebelah barat Sumatera dan Selat Sunda',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[95.0,-6.0],[105.0,-6.0],[105.0,2.0],[95.0,2.0],[95.0,-6.0]]]}'),
    -2.0, 100.0,
    'Aceh, Sumatera Utara, Sumatera Barat, Bengkulu, Lampung',
    'aktif'
),

-- WPP 573
(
    '573',
    'WPP 573 - Samudera Hindia Sebelah Selatan Jawa',
    'Samudera Hindia sebelah selatan Jawa hingga sebelah selatan Nusa Tenggara, Laut Sawu, dan Laut Timor bagian barat',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[102.0,-11.0],[115.0,-11.0],[115.0,-6.0],[102.0,-6.0],[102.0,-11.0]]]}'),
    -8.5, 108.5,
    'Banten, Jawa Barat, Jawa Tengah, DI Yogyakarta, Jawa Timur, Bali, NTB, NTT',
    'aktif'
),

-- WPP 711
(
    '711',
    'WPP 711 - Selat Karimata, Laut Natuna, dan Laut Cina Selatan',
    'Selat Karimata, Laut Natuna, dan Laut Cina Selatan',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[104.0,0.0],[110.0,0.0],[110.0,6.0],[104.0,6.0],[104.0,0.0]]]}'),
    3.0, 107.0,
    'Kepulauan Riau, Riau, Bangka Belitung, Kalimantan Barat',
    'aktif'
),

-- WPP 712
(
    '712',
    'WPP 712 - Laut Jawa',
    'Laut Jawa',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[106.0,-8.0],[116.0,-8.0],[116.0,-2.0],[106.0,-2.0],[106.0,-8.0]]]}'),
    -5.0, 111.0,
    'DKI Jakarta, Jawa Barat, Jawa Tengah, Jawa Timur, Kalimantan Selatan, Kalimantan Tengah',
    'aktif'
),

-- WPP 713
(
    '713',
    'WPP 713 - Selat Makassar, Teluk Bone, Laut Flores, dan Laut Bali',
    'Selat Makassar, Teluk Bone, Laut Flores, dan Laut Bali',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[116.0,-8.0],[122.0,-8.0],[122.0,2.0],[116.0,2.0],[116.0,-8.0]]]}'),
    -3.0, 119.0,
    'Kalimantan Timur, Kalimantan Selatan, Sulawesi Selatan, Sulawesi Barat, Bali, NTB',
    'aktif'
),

-- WPP 714
(
    '714',
    'WPP 714 - Teluk Tolo dan Laut Banda',
    'Teluk Tolo dan Laut Banda',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[122.0,-8.0],[132.0,-8.0],[132.0,-2.0],[122.0,-2.0],[122.0,-8.0]]]}'),
    -5.0, 127.0,
    'Sulawesi Tengah, Sulawesi Tenggara, Maluku',
    'aktif'
),

-- WPP 715
(
    '715',
    'WPP 715 - Teluk Tomini, Laut Maluku, Laut Halmahera, Laut Seram, dan Teluk Berau',
    'Teluk Tomini, Laut Maluku, Laut Halmahera, Laut Seram, dan Teluk Berau',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[124.0,-4.0],[132.0,-4.0],[132.0,4.0],[124.0,4.0],[124.0,-4.0]]]}'),
    0.0, 128.0,
    'Sulawesi Tengah, Sulawesi Utara, Gorontalo, Maluku Utara, Maluku',
    'aktif'
),

-- WPP 716
(
    '716',
    'WPP 716 - Laut Sulawesi dan Sebelah Utara Pulau Halmahera',
    'Laut Sulawesi dan sebelah utara Pulau Halmahera',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[118.0,0.0],[128.0,0.0],[128.0,6.0],[118.0,6.0],[118.0,0.0]]]}'),
    3.0, 123.0,
    'Sulawesi Utara, Gorontalo, Kalimantan Utara, Maluku Utara',
    'aktif'
),

-- WPP 717
(
    '717',
    'WPP 717 - Teluk Cendrawasih dan Samudera Pasifik',
    'Teluk Cendrawasih dan Samudera Pasifik',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[132.0,-4.0],[138.0,-4.0],[138.0,2.0],[132.0,2.0],[132.0,-4.0]]]}'),
    -1.0, 135.0,
    'Papua Barat, Papua',
    'aktif'
),

-- WPP 718
(
    '718',
    'WPP 718 - Laut Aru, Laut Arafuru, dan Laut Timor Bagian Timur',
    'Laut Aru, Laut Arafuru, dan Laut Timor bagian timur',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[132.0,-9.0],[141.0,-9.0],[141.0,-4.0],[132.0,-4.0],[132.0,-9.0]]]}'),
    -6.5, 136.5,
    'Maluku, Papua Barat, Papua',
    'aktif'
),

-- WPP 718 covers up to 141; WPP 573 covers Laut Timor barat — add WPP 574 per updated Permen KP
-- Note: Permen KP No. 18/2014 lists 11 WPP (571-573, 711-718). The 12th is WPP 574 added later.
(
    '574',
    'WPP 574 - Laut Timor Bagian Barat',
    'Laut Timor bagian barat dan Laut Sawu',
    ST_GeomFromGeoJSON('{"type":"Polygon","coordinates":[[[115.0,-11.0],[125.0,-11.0],[125.0,-6.0],[115.0,-6.0],[115.0,-11.0]]]}'),
    -8.5, 120.0,
    'NTT, Maluku',
    'aktif'
)

ON CONFLICT (kode_wpp) DO NOTHING;


-- ---------------------------------------------------------------------------
-- master_jenis_ikan — 40 commercial fish species
-- Sources: FAO ASFIS species list, KKP common catch species Indonesia
-- ---------------------------------------------------------------------------

INSERT INTO master_jenis_ikan (
    nama_lokal, nama_ilmiah, kode_fao, kategori
) VALUES

-- Pelagis Besar (Large Pelagics)
('Tuna Sirip Kuning',    'Thunnus albacares',        'YFT', 'pelagis_besar'),
('Tuna Mata Besar',      'Thunnus obesus',            'BET', 'pelagis_besar'),
('Tuna Sirip Biru Selatan', 'Thunnus maccoyii',       'SBF', 'pelagis_besar'),
('Cakalang',             'Katsuwonus pelamis',        'SKJ', 'pelagis_besar'),
('Tongkol Komo',         'Euthynnus affinis',         'KAW', 'pelagis_besar'),
('Tongkol Abu-abu',      'Thunnus tonggol',           'LOT', 'pelagis_besar'),
('Tenggiri',             'Scomberomorus commerson',   'COM', 'pelagis_besar'),
('Tenggiri Papan',       'Scomberomorus guttatus',    'GUT', 'pelagis_besar'),
('Marlin Biru',          'Makaira mazara',            'BUM', 'pelagis_besar'),
('Layaran',              'Istiophorus platypterus',   'SAI', 'pelagis_besar'),
('Lemadang',             'Coryphaena hippurus',       'DOL', 'pelagis_besar'),

-- Pelagis Kecil (Small Pelagics)
('Layang',               'Decapterus russelli',       'LAY', 'pelagis_kecil'),
('Layang Deles',         'Decapterus macrosoma',      'LAD', 'pelagis_kecil'),
('Kembung Lelaki',       'Rastrelliger kanagurta',    'RAK', 'pelagis_kecil'),
('Kembung Perempuan',    'Rastrelliger brachysoma',   'RAB', 'pelagis_kecil'),
('Selar Kuning',         'Selaroides leptolepis',     'YTS', 'pelagis_kecil'),
('Selar Bentong',        'Selar crumenophthalmus',    'BIG', 'pelagis_kecil'),
('Teri',                 'Stolephorus spp.',          'ANX', 'pelagis_kecil'),
('Lemuru',               'Sardinella lemuru',         'LEM', 'pelagis_kecil'),
('Tembang',              'Sardinella fimbriata',      'FRI', 'pelagis_kecil'),
('Japuh',                'Dussumieria acuta',         'RRS', 'pelagis_kecil'),
('Sunglir',              'Elagatis bipinnulata',      'RRU', 'pelagis_kecil'),

-- Demersal
('Kakap Merah',          'Lutjanus campechanus',      'RSE', 'demersal'),
('Kakap Putih',          'Lates calcarifer',          'SBN', 'demersal'),
('Kerapu Macan',         'Epinephelus fuscoguttatus', 'GPD', 'demersal'),
('Kerapu Bebek',         'Cromileptes altivelis',     'HMB', 'demersal'),
('Bawal Hitam',          'Parastromateus niger',      'BLB', 'demersal'),
('Bawal Putih',          'Pampus argenteus',          'SIL', 'demersal'),
('Kurisi',               'Nemipterus japonicus',      'JTH', 'demersal'),
('Gulamah',              'Johnius spp.',              'CRO', 'demersal'),
('Pari',                 'Dasyatis spp.',             'STT', 'demersal'),
('Cucut Botol',          'Carcharhinus spp.',         'SHX', 'demersal'),

-- Krustasea & Cephalopoda
('Udang Windu',          'Penaeus monodon',           'GIS', 'krustasea'),
('Udang Putih',          'Penaeus merguiensis',       'BAP', 'krustasea'),
('Udang Dogol',          'Metapenaeus ensis',         'GRE', 'krustasea'),
('Rajungan',             'Portunus pelagicus',        'SWM', 'krustasea'),
('Kepiting Bakau',       'Scylla serrata',            'MUD', 'krustasea'),
('Cumi-cumi',            'Loligo spp.',               'SQC', 'cephalopoda'),
('Sotong',               'Sepia spp.',                'CTL', 'cephalopoda'),
('Gurita',               'Octopus vulgaris',          'OCT', 'cephalopoda')

ON CONFLICT (kode_fao) DO NOTHING;
