\echo Fast spatial oceanography enrichment :start .. :end

SET statement_timeout = 0;

DROP TABLE IF EXISTS tmp_ocean_target;
DROP TABLE IF EXISTS tmp_ocean_bucket;

CREATE TEMP TABLE tmp_ocean_target AS
SELECT
  id,
  date_trunc('month', tanggal::timestamp)::date AS month_key,
  round(latitude::numeric / 2) * 2 AS lat_key,
  round(longitude::numeric / 2) * 2 AS lon_key
FROM master_oceanography
WHERE tanggal BETWEEN :'start'::date AND :'end'::date
  AND (
    suhu_permukaan IS NULL OR sst IS NULL OR ssh IS NULL OR klorofil IS NULL
    OR arus_laut_u IS NULL OR arus_laut_v IS NULL OR kecepatan_arus IS NULL
    OR tinggi_gelombang IS NULL OR periode_gelombang IS NULL
    OR kecepatan_angin IS NULL OR arah_angin IS NULL OR radiasi_matahari IS NULL
    OR kedalaman_laut IS NULL OR jarak_padang IS NULL OR pasang_surut IS NULL OR cuaca IS NULL
  );

CREATE INDEX tmp_ocean_target_key_idx ON tmp_ocean_target (month_key, lat_key, lon_key);

CREATE TEMP TABLE tmp_ocean_bucket AS
SELECT
  date_trunc('month', tanggal::timestamp)::date AS month_key,
  round(latitude::numeric / 2) * 2 AS lat_key,
  round(longitude::numeric / 2) * 2 AS lon_key,
  avg(suhu_permukaan) FILTER (WHERE suhu_permukaan IS NOT NULL) AS suhu_permukaan,
  avg(sst) FILTER (WHERE sst IS NOT NULL) AS sst,
  avg(ssh) FILTER (WHERE ssh IS NOT NULL) AS ssh,
  avg(klorofil) FILTER (WHERE klorofil IS NOT NULL) AS klorofil,
  avg(arus_laut_u) FILTER (WHERE arus_laut_u IS NOT NULL) AS arus_laut_u,
  avg(arus_laut_v) FILTER (WHERE arus_laut_v IS NOT NULL) AS arus_laut_v,
  avg(kecepatan_arus) FILTER (WHERE kecepatan_arus IS NOT NULL) AS kecepatan_arus,
  avg(tinggi_gelombang) FILTER (WHERE tinggi_gelombang IS NOT NULL) AS tinggi_gelombang,
  avg(periode_gelombang) FILTER (WHERE periode_gelombang IS NOT NULL) AS periode_gelombang,
  avg(kecepatan_angin) FILTER (WHERE kecepatan_angin IS NOT NULL) AS kecepatan_angin,
  avg(arah_angin) FILTER (WHERE arah_angin IS NOT NULL) AS arah_angin,
  avg(radiasi_matahari) FILTER (WHERE radiasi_matahari IS NOT NULL) AS radiasi_matahari,
  avg(kedalaman_laut) FILTER (WHERE kedalaman_laut IS NOT NULL) AS kedalaman_laut,
  avg(jarak_padang) FILTER (WHERE jarak_padang IS NOT NULL) AS jarak_padang,
  avg(pasang_surut) FILTER (WHERE pasang_surut IS NOT NULL) AS pasang_surut,
  max(cuaca) FILTER (WHERE cuaca IS NOT NULL) AS cuaca
FROM master_oceanography
WHERE tanggal BETWEEN :'start'::date AND :'end'::date
GROUP BY 1, 2, 3;

CREATE INDEX tmp_ocean_bucket_key_idx ON tmp_ocean_bucket (month_key, lat_key, lon_key);
ANALYZE tmp_ocean_target;
ANALYZE tmp_ocean_bucket;

UPDATE master_oceanography t
SET
  suhu_permukaan   = COALESCE(t.suhu_permukaan, round(b.suhu_permukaan, 2)),
  sst              = COALESCE(t.sst, round(b.sst, 2)),
  ssh              = COALESCE(t.ssh, round(b.ssh, 3)),
  klorofil         = COALESCE(t.klorofil, round(b.klorofil, 4)),
  arus_laut_u      = COALESCE(t.arus_laut_u, round(b.arus_laut_u, 3)),
  arus_laut_v      = COALESCE(t.arus_laut_v, round(b.arus_laut_v, 3)),
  kecepatan_arus   = COALESCE(
    t.kecepatan_arus,
    round(b.kecepatan_arus, 3),
    CASE
      WHEN COALESCE(t.arus_laut_u, b.arus_laut_u) IS NOT NULL
       AND COALESCE(t.arus_laut_v, b.arus_laut_v) IS NOT NULL
      THEN round(sqrt(power(COALESCE(t.arus_laut_u, b.arus_laut_u), 2) + power(COALESCE(t.arus_laut_v, b.arus_laut_v), 2)), 3)
    END
  ),
  tinggi_gelombang = COALESCE(t.tinggi_gelombang, round(b.tinggi_gelombang, 2)),
  periode_gelombang= COALESCE(t.periode_gelombang, round(b.periode_gelombang, 2)),
  kecepatan_angin  = COALESCE(t.kecepatan_angin, round(b.kecepatan_angin, 2)),
  arah_angin       = COALESCE(t.arah_angin, round(b.arah_angin, 2)),
  radiasi_matahari = COALESCE(t.radiasi_matahari, round(b.radiasi_matahari, 2)),
  kedalaman_laut   = COALESCE(t.kedalaman_laut, round(b.kedalaman_laut, 2)),
  jarak_padang     = COALESCE(t.jarak_padang, round(b.jarak_padang, 3)),
  pasang_surut     = COALESCE(t.pasang_surut, t.ssh, round(b.pasang_surut, 3), round(b.ssh, 3)),
  cuaca            = COALESCE(t.cuaca, b.cuaca),
  updated_at       = now()
FROM tmp_ocean_target tg
JOIN tmp_ocean_bucket b
  ON b.month_key = tg.month_key
 AND b.lat_key = tg.lat_key
 AND b.lon_key = tg.lon_key
WHERE t.id = tg.id;
