import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "gs://cdc-epi-lake-2026"

CONFIGURACIONES = [
    ("base", False, False),
    ("cache", True, False),
    ("cache_broadcast", True, True),
]


def medir(spark, epi, conteo, usar_cache, usar_broadcast):
    spark.catalog.clearCache()
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    if usar_broadcast:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold",
                       str(10 * 1024 * 1024))
    else:
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    e = epi.cache() if usar_cache else epi
    c = conteo.cache() if usar_cache else conteo

    filas_join = 0
    filas_salida = 0
    casos_join = 0
    tiempos = []

    for _ in range(2):
        inicio = time.time()
        if usar_broadcast:
            unido = e.join(F.broadcast(c), "ubigeo", "left")
        else:
            unido = e.join(c, "ubigeo", "left")
        resumen = (unido.groupBy("dep", "grupo")
                   .agg(F.sum("casos").alias("casos"),
                        F.count(F.lit(1)).alias("semanas"))
                   .agg(F.sum("semanas").alias("filas"),
                        F.count(F.lit(1)).alias("grupos"),
                        F.sum("casos").alias("casos"))
                   .first())
        tiempos.append(round(time.time() - inicio, 2))
        filas_join = int(resumen["filas"])
        filas_salida = int(resumen["grupos"])
        casos_join = int(resumen["casos"])

    e.unpersist()
    c.unpersist()
    return filas_join, filas_salida, casos_join, tiempos


def main():
    spark = SparkSession.builder.appName("03-opt").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # El panel ya trae n_ipress; se quita para medir el join que lo genera.
    epi = (spark.read.parquet(f"{BUCKET}/processed/epi_clean/")
           .drop("n_ipress"))
    ipr = spark.read.parquet(f"{BUCKET}/processed/ipress_dim/")
    conteo = ipr.groupBy("ubigeo").agg(F.count(F.lit(1)).alias("n_ipress"))

    filas_epi = epi.count()
    filas_ipr = ipr.count()
    filas_conteo = conteo.count()
    print(f"panel filas={filas_epi} ipress filas={filas_ipr} "
          f"ubigeos={filas_conteo}")
    print(f"spark master={spark.sparkContext.master} version={spark.version}")
    print("\nconfiguracion,filas_join,filas_salida,casos,t1_s,t2_s")

    resultados = []
    for nombre, usar_cache, usar_broadcast in CONFIGURACIONES:
        filas_join, filas_salida, casos_join, tiempos = medir(
            spark, epi, conteo, usar_cache, usar_broadcast)
        resultados.append({
            "configuracion": nombre,
            "filas_join": filas_join,
            "filas_salida": filas_salida,
            "casos": casos_join,
            "t1": tiempos[0],
            "t2": tiempos[1],
        })
        print(f"{nombre},{filas_join},{filas_salida},{casos_join},"
              f"{tiempos[0]},{tiempos[1]}")

    base = resultados[0]["t1"]
    print("\n--- mejora frente a base en frio ---")
    for r in resultados:
        mejora = 0.0 if base == 0 else (base - r["t1"]) / base * 100
        print(f"{r['configuracion']}: t1={r['t1']}s "
              f"mejora={round(mejora, 2)}% "
              f"t2={r['t2']}s")

    print("\nFIN 03_opt: comparacion base, cache y cache con broadcast")
    spark.stop()


if __name__ == "__main__":
    main()
