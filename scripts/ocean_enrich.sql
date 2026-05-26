-- Usage:
--   psql ... -v start='2021-01-01' -v end='2021-12-31' -f scripts/ocean_enrich.sql

\echo Enriching oceanography window :start .. :end

WITH day_bucket AS (
    SELECT
        tanggal,
        round(latitude::numeric * 2) / 2  AS lat_key,
        round(longitude::numeric * 2) / 2 AS lon_key,
        max(suhu_permukaan) FILTER (WHERE suhu_permukaan IS NOT NULL) AS suhu_permukaan,
        max(sst) FILTER (WHERE sst IS NOT NULL) AS sst,
        max(ssh) FILTER (WHERE ssh IS NOT NULL) AS ssh,
        max(klorofil) FILTER (WHERE klorofil IS NOT NULL) AS klorofil,
        max(arus_laut_u) FILTER (WHERE arus_laut_u IS NOT NULL) AS arus_laut_u,
        max(arus_laut_v) FILTER (WHERE arus_laut_v IS NOT NULL) AS arus_laut_v,
        max(kecepatan_arus) FILTER (WHERE kecepatan_arus IS NOT NULL) AS kecepatan_arus,
        max(tinggi_gelombang) FILTER (WHERE tinggi_gelombang IS NOT NULL) AS tinggi_gelombang,
        max(periode_gelombang) FILTER (WHERE periode_gelombang IS NOT NULL) AS periode_gelombang,
        max(kecepatan_angin) FILTER (WHERE kecepatan_angin IS NOT NULL) AS kecepatan_angin,
        max(arah_angin) FILTER (WHERE arah_angin IS NOT NULL) AS arah_angin,
        max(radiasi_matahari) FILTER (WHERE radiasi_matahari IS NOT NULL) AS radiasi_matahari,
        max(kedalaman_laut) FILTER (WHERE kedalaman_laut IS NOT NULL) AS kedalaman_laut,
        max(jarak_padang) FILTER (WHERE jarak_padang IS NOT NULL) AS jarak_padang,
        max(pasang_surut) FILTER (WHERE pasang_surut IS NOT NULL) AS pasang_surut,
        max(cuaca) FILTER (WHERE cuaca IS NOT NULL) AS cuaca
    FROM master_oceanography
    WHERE tanggal BETWEEN :'start'::date AND :'end'::date
    GROUP BY 1, 2, 3
)
UPDATE master_oceanography t
SET
    suhu_permukaan   = COALESCE(t.suhu_permukaan, b.suhu_permukaan),
    sst              = COALESCE(t.sst, b.sst),
    ssh              = COALESCE(t.ssh, b.ssh),
    klorofil         = COALESCE(t.klorofil, b.klorofil),
    arus_laut_u      = COALESCE(t.arus_laut_u, b.arus_laut_u),
    arus_laut_v      = COALESCE(t.arus_laut_v, b.arus_laut_v),
    kecepatan_arus   = COALESCE(
        t.kecepatan_arus,
        b.kecepatan_arus,
        CASE
            WHEN COALESCE(t.arus_laut_u, b.arus_laut_u) IS NOT NULL
             AND COALESCE(t.arus_laut_v, b.arus_laut_v) IS NOT NULL
            THEN sqrt(
                power(COALESCE(t.arus_laut_u, b.arus_laut_u), 2) +
                power(COALESCE(t.arus_laut_v, b.arus_laut_v), 2)
            )
            ELSE NULL
        END
    ),
    tinggi_gelombang = COALESCE(t.tinggi_gelombang, b.tinggi_gelombang),
    periode_gelombang= COALESCE(t.periode_gelombang, b.periode_gelombang),
    kecepatan_angin  = COALESCE(t.kecepatan_angin, b.kecepatan_angin),
    arah_angin       = COALESCE(t.arah_angin, b.arah_angin),
    radiasi_matahari = COALESCE(t.radiasi_matahari, b.radiasi_matahari),
    kedalaman_laut   = COALESCE(t.kedalaman_laut, b.kedalaman_laut),
    jarak_padang     = COALESCE(t.jarak_padang, b.jarak_padang),
    pasang_surut     = COALESCE(t.pasang_surut, b.pasang_surut, t.ssh, b.ssh),
    cuaca            = COALESCE(t.cuaca, b.cuaca),
    updated_at       = NOW()
FROM day_bucket b
WHERE t.tanggal = b.tanggal
  AND round(t.latitude::numeric * 2) / 2 = b.lat_key
  AND round(t.longitude::numeric * 2) / 2 = b.lon_key
  AND t.tanggal BETWEEN :'start'::date AND :'end'::date
  AND (
      t.suhu_permukaan IS NULL OR t.sst IS NULL OR t.ssh IS NULL OR t.klorofil IS NULL
      OR t.arus_laut_u IS NULL OR t.arus_laut_v IS NULL OR t.kecepatan_arus IS NULL
      OR t.tinggi_gelombang IS NULL OR t.periode_gelombang IS NULL
      OR t.kecepatan_angin IS NULL OR t.arah_angin IS NULL
      OR t.radiasi_matahari IS NULL OR t.kedalaman_laut IS NULL
      OR t.jarak_padang IS NULL OR t.pasang_surut IS NULL OR t.cuaca IS NULL
  );

WITH month_bucket AS (
    SELECT
        date_trunc('month', tanggal)::date AS month_key,
        round(latitude::numeric * 2) / 2  AS lat_key,
        round(longitude::numeric * 2) / 2 AS lon_key,
        max(suhu_permukaan) FILTER (WHERE suhu_permukaan IS NOT NULL) AS suhu_permukaan,
        max(sst) FILTER (WHERE sst IS NOT NULL) AS sst,
        max(ssh) FILTER (WHERE ssh IS NOT NULL) AS ssh,
        max(klorofil) FILTER (WHERE klorofil IS NOT NULL) AS klorofil,
        max(arus_laut_u) FILTER (WHERE arus_laut_u IS NOT NULL) AS arus_laut_u,
        max(arus_laut_v) FILTER (WHERE arus_laut_v IS NOT NULL) AS arus_laut_v,
        max(kecepatan_arus) FILTER (WHERE kecepatan_arus IS NOT NULL) AS kecepatan_arus
    FROM master_oceanography
    WHERE tanggal BETWEEN :'start'::date AND :'end'::date
    GROUP BY 1, 2, 3
)
UPDATE master_oceanography t
SET
    suhu_permukaan = COALESCE(t.suhu_permukaan, m.suhu_permukaan),
    sst            = COALESCE(t.sst, m.sst),
    ssh            = COALESCE(t.ssh, m.ssh),
    klorofil       = COALESCE(t.klorofil, m.klorofil),
    arus_laut_u    = COALESCE(t.arus_laut_u, m.arus_laut_u),
    arus_laut_v    = COALESCE(t.arus_laut_v, m.arus_laut_v),
    kecepatan_arus = COALESCE(
        t.kecepatan_arus,
        m.kecepatan_arus,
        CASE
            WHEN COALESCE(t.arus_laut_u, m.arus_laut_u) IS NOT NULL
             AND COALESCE(t.arus_laut_v, m.arus_laut_v) IS NOT NULL
            THEN sqrt(
                power(COALESCE(t.arus_laut_u, m.arus_laut_u), 2) +
                power(COALESCE(t.arus_laut_v, m.arus_laut_v), 2)
            )
            ELSE NULL
        END
    ),
    pasang_surut   = COALESCE(t.pasang_surut, t.ssh, m.ssh),
    updated_at     = NOW()
FROM month_bucket m
WHERE date_trunc('month', t.tanggal)::date = m.month_key
  AND round(t.latitude::numeric * 2) / 2 = m.lat_key
  AND round(t.longitude::numeric * 2) / 2 = m.lon_key
  AND t.tanggal BETWEEN :'start'::date AND :'end'::date
  AND (
      t.suhu_permukaan IS NULL OR t.sst IS NULL OR t.ssh IS NULL OR t.klorofil IS NULL
      OR t.arus_laut_u IS NULL OR t.arus_laut_v IS NULL OR t.kecepatan_arus IS NULL
      OR t.pasang_surut IS NULL
  );
