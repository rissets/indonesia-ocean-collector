-- 002_seed_data.sql
-- Seed reference data for master_wpp and master_jenis_ikan.
-- Idempotent: UPDATE ... WHERE for WPP, ON CONFLICT DO NOTHING for jenis ikan.
-- Sources: Permen KP No. 18/2014 (WPP), KKP global-search-ikan API, FAO ASFIS species list.

-- ---------------------------------------------------------------------------
-- master_wpp — 12 WPP regions (UPDATE to fill NULL columns)
-- kode_wpp format in DB: 'WPP-711' etc.
-- luas_km2: approximate from official KKP/BIG references
-- batas_*: cardinal boundary descriptions per Permen KP No. 18/2014
-- geojson_polygon: rectangular bbox (SW→SE→NE→NW→SW) from config.py WPP_REGIONS
-- ---------------------------------------------------------------------------

UPDATE master_wpp SET
    wilayah_perairan    = 'Selat Malaka dan Laut Andaman',
    luas_km2            = 93800.00,
    batas_utara         = 'Malaysia, Thailand',
    batas_selatan       = 'Provinsi Aceh dan Sumatera Utara',
    batas_barat         = 'Perairan India (Kepulauan Andaman)',
    batas_timur         = 'Pantai timur Sumatera Utara',
    provinsi_terkait    = 'Aceh, Sumatera Utara',
    koordinat_pusat_lat = 3.500000,
    koordinat_pusat_lon = 99.500000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[95.0,1.0],[104.0,1.0],[104.0,6.0],[95.0,6.0],[95.0,1.0]]]}'
WHERE kode_wpp = 'WPP-571';

UPDATE master_wpp SET
    wilayah_perairan    = 'Samudera Hindia sebelah barat Sumatera dan Selat Sunda',
    luas_km2            = 279000.00,
    batas_utara         = 'Provinsi Aceh',
    batas_selatan       = 'Samudera Hindia',
    batas_barat         = 'Samudera Hindia',
    batas_timur         = 'Pantai barat Sumatera, Selat Sunda',
    provinsi_terkait    = 'Aceh, Sumatera Utara, Sumatera Barat, Bengkulu, Lampung, Banten',
    koordinat_pusat_lat = -2.000000,
    koordinat_pusat_lon = 100.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[95.0,-6.0],[105.0,-6.0],[105.0,2.0],[95.0,2.0],[95.0,-6.0]]]}'
WHERE kode_wpp = 'WPP-572';

UPDATE master_wpp SET
    wilayah_perairan    = 'Samudera Hindia sebelah selatan Jawa hingga sebelah selatan Nusa Tenggara, Laut Sawu, dan Laut Timor bagian barat',
    luas_km2            = 711000.00,
    batas_utara         = 'Pantai selatan Jawa, Bali, NTB, NTT',
    batas_selatan       = 'Samudera Hindia, Australia',
    batas_barat         = 'WPP-572 (Selat Sunda)',
    batas_timur         = 'WPP-574 (Laut Timor bagian barat)',
    provinsi_terkait    = 'Banten, Jawa Barat, Jawa Tengah, DI Yogyakarta, Jawa Timur, Bali, NTB, NTT',
    koordinat_pusat_lat = -8.500000,
    koordinat_pusat_lon = 108.500000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[102.0,-11.0],[115.0,-11.0],[115.0,-6.0],[102.0,-6.0],[102.0,-11.0]]]}'
WHERE kode_wpp = 'WPP-573';

UPDATE master_wpp SET
    wilayah_perairan    = 'Laut Timor bagian barat dan Laut Sawu',
    luas_km2            = 186000.00,
    batas_utara         = 'Provinsi NTT',
    batas_selatan       = 'Australia (Timor Sea)',
    batas_barat         = 'WPP-573',
    batas_timur         = 'Timor Leste, WPP-718',
    provinsi_terkait    = 'NTT, Maluku',
    koordinat_pusat_lat = -8.500000,
    koordinat_pusat_lon = 120.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[115.0,-11.0],[125.0,-11.0],[125.0,-6.0],[115.0,-6.0],[115.0,-11.0]]]}'
WHERE kode_wpp = 'WPP-574';

UPDATE master_wpp SET
    wilayah_perairan    = 'Selat Karimata, Laut Natuna, dan Laut Cina Selatan',
    luas_km2            = 396000.00,
    batas_utara         = 'Malaysia, Vietnam, Laut Cina Selatan',
    batas_selatan       = 'Pulau Bangka, Belitung, Kalimantan Barat',
    batas_barat         = 'Pantai timur Sumatera, Kepulauan Riau',
    batas_timur         = 'Kalimantan Barat',
    provinsi_terkait    = 'Kepulauan Riau, Riau, Bangka Belitung, Kalimantan Barat',
    koordinat_pusat_lat = 3.000000,
    koordinat_pusat_lon = 107.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[104.0,0.0],[110.0,0.0],[110.0,6.0],[104.0,6.0],[104.0,0.0]]]}'
WHERE kode_wpp = 'WPP-711';

UPDATE master_wpp SET
    wilayah_perairan    = 'Laut Jawa',
    luas_km2            = 433000.00,
    batas_utara         = 'Kalimantan Selatan, Kalimantan Tengah',
    batas_selatan       = 'Pantai utara Jawa',
    batas_barat         = 'WPP-711 (Selat Karimata)',
    batas_timur         = 'WPP-713 (Selat Makassar)',
    provinsi_terkait    = 'DKI Jakarta, Jawa Barat, Jawa Tengah, Jawa Timur, Kalimantan Selatan, Kalimantan Tengah',
    koordinat_pusat_lat = -5.000000,
    koordinat_pusat_lon = 111.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[106.0,-8.0],[116.0,-8.0],[116.0,-2.0],[106.0,-2.0],[106.0,-8.0]]]}'
WHERE kode_wpp = 'WPP-712';

UPDATE master_wpp SET
    wilayah_perairan    = 'Selat Makassar, Teluk Bone, Laut Flores, dan Laut Bali',
    luas_km2            = 460000.00,
    batas_utara         = 'Kalimantan Timur, Kalimantan Selatan',
    batas_selatan       = 'Bali, NTB',
    batas_barat         = 'WPP-712 (Laut Jawa)',
    batas_timur         = 'Sulawesi Selatan, Sulawesi Tenggara',
    provinsi_terkait    = 'Kalimantan Timur, Kalimantan Selatan, Sulawesi Selatan, Sulawesi Barat, Bali, NTB',
    koordinat_pusat_lat = -3.000000,
    koordinat_pusat_lon = 119.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[116.0,-8.0],[122.0,-8.0],[122.0,2.0],[116.0,2.0],[116.0,-8.0]]]}'
WHERE kode_wpp = 'WPP-713';

UPDATE master_wpp SET
    wilayah_perairan    = 'Teluk Tolo dan Laut Banda',
    luas_km2            = 470000.00,
    batas_utara         = 'Sulawesi Tengah, Sulawesi Tenggara',
    batas_selatan       = 'NTT, WPP-574',
    batas_barat         = 'WPP-713 (Selat Makassar)',
    batas_timur         = 'Maluku, WPP-718',
    provinsi_terkait    = 'Sulawesi Tengah, Sulawesi Tenggara, Maluku',
    koordinat_pusat_lat = -5.000000,
    koordinat_pusat_lon = 127.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[122.0,-8.0],[132.0,-8.0],[132.0,-2.0],[122.0,-2.0],[122.0,-8.0]]]}'
WHERE kode_wpp = 'WPP-714';

UPDATE master_wpp SET
    wilayah_perairan    = 'Teluk Tomini, Laut Maluku, Laut Halmahera, Laut Seram, dan Teluk Berau',
    luas_km2            = 390000.00,
    batas_utara         = 'Sulawesi Utara, Gorontalo, Maluku Utara',
    batas_selatan       = 'Sulawesi Tengah, Maluku',
    batas_barat         = 'WPP-716 (Laut Sulawesi)',
    batas_timur         = 'Papua Barat (Teluk Berau)',
    provinsi_terkait    = 'Sulawesi Tengah, Sulawesi Utara, Gorontalo, Maluku Utara, Maluku',
    koordinat_pusat_lat = 0.000000,
    koordinat_pusat_lon = 128.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[124.0,-4.0],[132.0,-4.0],[132.0,4.0],[124.0,4.0],[124.0,-4.0]]]}'
WHERE kode_wpp = 'WPP-715';

UPDATE master_wpp SET
    wilayah_perairan    = 'Laut Sulawesi dan sebelah utara Pulau Halmahera',
    luas_km2            = 110000.00,
    batas_utara         = 'Filipina',
    batas_selatan       = 'Sulawesi Utara, Gorontalo',
    batas_barat         = 'Kalimantan Utara',
    batas_timur         = 'Maluku Utara (Halmahera)',
    provinsi_terkait    = 'Sulawesi Utara, Gorontalo, Kalimantan Utara, Maluku Utara',
    koordinat_pusat_lat = 3.000000,
    koordinat_pusat_lon = 123.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[118.0,0.0],[128.0,0.0],[128.0,6.0],[118.0,6.0],[118.0,0.0]]]}'
WHERE kode_wpp = 'WPP-716';

UPDATE master_wpp SET
    wilayah_perairan    = 'Teluk Cendrawasih dan Samudera Pasifik',
    luas_km2            = 420000.00,
    batas_utara         = 'Samudera Pasifik',
    batas_selatan       = 'Papua Barat, Papua',
    batas_barat         = 'WPP-715 (Laut Maluku)',
    batas_timur         = 'Papua Nugini',
    provinsi_terkait    = 'Papua Barat, Papua',
    koordinat_pusat_lat = -1.000000,
    koordinat_pusat_lon = 135.000000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[132.0,-4.0],[138.0,-4.0],[138.0,2.0],[132.0,2.0],[132.0,-4.0]]]}'
WHERE kode_wpp = 'WPP-717';

UPDATE master_wpp SET
    wilayah_perairan    = 'Laut Aru, Laut Arafuru, dan Laut Timor bagian timur',
    luas_km2            = 650000.00,
    batas_utara         = 'Papua, Papua Barat',
    batas_selatan       = 'Australia',
    batas_barat         = 'WPP-714 (Laut Banda)',
    batas_timur         = 'Papua Nugini',
    provinsi_terkait    = 'Maluku, Papua Barat, Papua',
    koordinat_pusat_lat = -6.500000,
    koordinat_pusat_lon = 136.500000,
    geojson_polygon     = '{"type":"Polygon","coordinates":[[[132.0,-9.0],[141.0,-9.0],[141.0,-4.0],[132.0,-4.0],[132.0,-9.0]]]}'
WHERE kode_wpp = 'WPP-718';


-- ---------------------------------------------------------------------------
-- master_jenis_ikan — commercial fish species from KKP global-search-ikan API
-- nama_lokal: from KKP API (ikan_tangkap type), nama_ilmiah + kode_fao: FAO ASFIS
-- ON CONFLICT on kode_fao; species without unique FAO code use nama_lokal as dedup key
-- ---------------------------------------------------------------------------

INSERT INTO master_jenis_ikan (nama_lokal, nama_ilmiah, kode_fao, kategori) VALUES

-- Pelagis Besar
('Madidihang',              'Thunnus albacares',           'YFT', 'pelagis_besar'),
('Tuna mata besar',         'Thunnus obesus',              'BET', 'pelagis_besar'),
('Tuna sirip biru selatan', 'Thunnus maccoyii',            'SBF', 'pelagis_besar'),
('Albakora',                'Thunnus alalunga',            'ALB', 'pelagis_besar'),
('Cakalang',                'Katsuwonus pelamis',          'SKJ', 'pelagis_besar'),
('Tongkol abu-abu',         'Thunnus tonggol',             'LOT', 'pelagis_besar'),
('Tongkol banyar',          'Euthynnus affinis',           'KAW', 'pelagis_besar'),
('Tenggiri',                'Scomberomorus commerson',     'COM', 'pelagis_besar'),
('Tenggiri papan',          'Scomberomorus guttatus',      'GUT', 'pelagis_besar'),
('Tenggiri batang',         'Scomberomorus lineolatus',    'SSM', 'pelagis_besar'),
('Marlin biru',             'Makaira mazara',              'BUM', 'pelagis_besar'),
('Setuhuk loreng',          'Tetrapturus audax',           'STB', 'pelagis_besar'),
('Ikan layaran',            'Istiophorus platypterus',     'SAI', 'pelagis_besar'),
('Lemadang',                'Coryphaena hippurus',         'DOL', 'pelagis_besar'),
('Gindara',                 'Lepidocybium flavobrunneum',  'ESC', 'pelagis_besar'),
('Meka',                    'Xiphias gladius',             'SWO', 'pelagis_besar'),
('Ikan terbang',            'Exocoetus volitans',          'FLY', 'pelagis_besar'),

-- Pelagis Kecil
('Layang',                  'Decapterus russelli',         'LAY', 'pelagis_kecil'),
('Layang deles',            'Decapterus macrosoma',        'LAD', 'pelagis_kecil'),
('Kembung lelaki',          'Rastrelliger kanagurta',      'RAK', 'pelagis_kecil'),
('Kembung perempuan',       'Rastrelliger brachysoma',     'RAB', 'pelagis_kecil'),
('Selar kuning',            'Selaroides leptolepis',       'YTS', 'pelagis_kecil'),
('Selar bentong',           'Selar crumenophthalmus',      'SEL', 'pelagis_kecil'),
('Teri',                    'Stolephorus spp.',            'ANX', 'pelagis_kecil'),
('Lemuru',                  'Sardinella lemuru',           'LEM', 'pelagis_kecil'),
('Tembang',                 'Sardinella fimbriata',        'FRI', 'pelagis_kecil'),
('Japuh',                   'Dussumieria acuta',           'DUA', 'pelagis_kecil'),
('Siro',                    'Amblygaster sirm',            'SIR', 'pelagis_kecil'),
('Sunglir',                 'Elagatis bipinnulata',        'RRU', 'pelagis_kecil'),
('Tetengkek',               'Megalaspis cordyla',          'LYT', 'pelagis_kecil'),
('Talang-talang',           'Scomberoides commersonnianus','TAL', 'pelagis_kecil'),
('Kuwe',                    'Caranx spp.',                 'CRX', 'pelagis_kecil'),
('Bandeng',                 'Chanos chanos',               'MIL', 'pelagis_kecil'),
('Julung-julung',           'Hemiramphus spp.',            'HEM', 'pelagis_kecil'),
('Alu-alu',                 'Sphyraena barracuda',         'BAR', 'pelagis_kecil'),
('Barakuda',                'Sphyraena jello',             'SFJ', 'pelagis_kecil'),
('Slengseng',               'Scomber australasicus',       'SAU', 'pelagis_kecil'),
('Cobia',                   'Rachycentron canadum',        'CBA', 'pelagis_kecil'),

-- Demersal
('Kakap merah',             'Lutjanus malabaricus',        'RSN', 'demersal'),
('Kakap putih',             'Lates calcarifer',            'SBN', 'demersal'),
('Kakap',                   'Lutjanus spp.',               'LUT', 'demersal'),
('Kerapu macan',            'Epinephelus fuscoguttatus',   'GPD', 'demersal'),
('Kerapu bebek',            'Cromileptes altivelis',       'HMB', 'demersal'),
('Kerapu sunu',             'Plectropomus leopardus',      'LPL', 'demersal'),
('Kerapu',                  'Epinephelus spp.',            'GPX', 'demersal'),
('Bawal hitam',             'Parastromateus niger',        'BLB', 'demersal'),
('Bawal putih',             'Pampus argenteus',            'SIL', 'demersal'),
('Kurisi',                  'Nemipterus japonicus',        'JTH', 'demersal'),
('Gulamah',                 'Johnius spp.',                'CRO', 'demersal'),
('Kuniran',                 'Upeneus spp.',                'MUX', 'demersal'),
('Peperek',                 'Leiognathus spp.',            'PON', 'demersal'),
('Gerot-gerot',             'Pomadasys spp.',              'GRU', 'demersal'),
('Swanggi',                 'Priacanthus tayenus',         'PRI', 'demersal'),
('Layur',                   'Trichiurus lepturus',         'LHT', 'demersal'),
('Manyung',                 'Arius thalassinus',           'CAX', 'demersal'),
('Belanak',                 'Mugil cephalus',              'MUL', 'demersal'),
('Lencam',                  'Lethrinus lentjan',           'EME', 'demersal'),
('Ekor Kuning',             'Caesio cuning',               'CSI', 'demersal'),
('Pisang-pisang',           'Caesio spp.',                 'CSX', 'demersal'),
('Napoleon',                'Cheilinus undulatus',         'NPN', 'demersal'),
('Baronang',                'Siganus spp.',                'SIG', 'demersal'),
('Kuro',                    'Polynemus spp.',              'THF', 'demersal'),
('Pari',                    'Dasyatis spp.',               'STT', 'demersal'),
('Cucut botol',             'Carcharhinus spp.',           'SHX', 'demersal'),

-- Krustasea
('Udang windu',             'Penaeus monodon',             'GIS', 'krustasea'),
('Udang putih',             'Penaeus merguiensis',         'BAP', 'krustasea'),
('Udang dogol',             'Metapenaeus ensis',           'GRE', 'krustasea'),
('Udang jerbung',           'Fenneropenaeus indicus',      'WIS', 'krustasea'),
('Udang krosok',            'Parapenaeopsis spp.',         'PPX', 'krustasea'),
('Udang barong',            'Panulirus spp.',              'SLV', 'krustasea'),
('Udang galah',             'Macrobrachium rosenbergii',   'GFR', 'krustasea'),
('Rajungan',                'Portunus pelagicus',          'SWM', 'krustasea'),
('Kepiting bakau',          'Scylla serrata',              'MUD', 'krustasea'),
('Lobster kipas',           'Ibacus spp.',                 'SLI', 'krustasea'),

-- Cephalopoda & Moluska
('Cumi-cumi',               'Loligo spp.',                 'SQC', 'cephalopoda'),
('Sotong',                  'Sepia spp.',                  'CTL', 'cephalopoda'),
('Gurita',                  'Octopus vulgaris',            'OCT', 'cephalopoda'),
('Blekutak',                'Sepioteuthis lessoniana',     'BTK', 'cephalopoda'),
('Teripang',                'Holothuria spp.',             'SLU', 'moluska'),
('Kerang darah',            'Tegillarca granosa',          'CKG', 'moluska'),
('Remis',                   'Corbicula spp.',              'CLX', 'moluska')

ON CONFLICT DO NOTHING;
