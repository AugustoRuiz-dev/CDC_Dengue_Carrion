import sys
import time
from functools import reduce

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "gs://cdc-epi-lake-2026"


def replicar(base, factor):
    """Copia el panel factor veces con una columna distinta por copia."""
    if factor == 1:
        return base
    partes = [base.withColumn("copia", F.lit(i)) for i in range(factor)]
    return reduce(lambda a, b: a.unionByName(b), partes)


def pipeline(df, conteo):
    d = (df
         .withColumn("ubigeo", F.lpad(F.trim(F.col("ubigeo")), 6, "0"))
         .withColumn("ano", F.col("ano").cast("int"))
         .withColumn("semana", F.col("semana").cast("int"))
         .withColumn("casos", F.col("casos").cast("int")))

    d = d.filter(F.col("ubigeo").rlike("^[0-9]{6}$") &
                 F.col("ano").between(2000, 2024) &
                 F.col("semana").between(1, 53))

    d = (d.groupBy("ubigeo", "dep", "ano", "semana", "grupo")
         .agg(F.sum("casos").alias("casos")))

    d = (d.join(F.broadcast(conteo), "ubigeo", "left")
         .fillna({"n_ipress": 0}))

    return d.agg(F.count(F.lit(1)).alias("filas"),
                 F.sum("casos").alias("casos")).first()


def main():
    factores = [1, 10, 100]
    if len(sys.argv) > 1:
        factores = [int(x) for x in sys.argv[1].split(",")]

    spark = SparkSession.builder.appName("04-scale").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    base = spark.read.parquet(f"{BUCKET}/processed/epi_clean/").cache()
    ipr = spark.read.parquet(f"{BUCKET}/processed/ipress_dim/").cache()
    conteo = ipr.groupBy("ubigeo").agg(F.count(F.lit(1)).alias("n_ipress"))

    filas_base = base.count()
    filas_ipr = ipr.count()
    filas_conteo = conteo.count()
    print(f"panel base filas={filas_base}")
    print(f"ipress filas={filas_ipr} ubigeos={filas_conteo}")
    print(f"spark master={spark.sparkContext.master} version={spark.version}")
    print(f"executors={spark.conf.get('spark.executor.instances', 'no definido')}")
    print("\nfactor,filas_entrada,filas_salida,casos_total,tiempo_s,filas_por_s")

    for factor in factores:
        entrada = replicar(base, factor)
        inicio = time.time()
        resumen = pipeline(entrada, conteo)
        segundos = round(time.time() - inicio, 2)

        filas_entrada = filas_base * factor
        filas_salida = int(resumen["filas"])
        velocidad = 0 if segundos == 0 else round(filas_entrada / segundos)

        print(f"{factor}x,{filas_entrada},{filas_salida},"
              f"{int(resumen['casos'])},{segundos},{velocidad}")

    base.unpersist()
    ipr.unpersist()
    print("\nFIN 04_scale: escalabilidad medida con el pipeline completo")
    spark.stop()


if __name__ == "__main__":
    main()
