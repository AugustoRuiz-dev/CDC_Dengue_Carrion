from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = "gs://cdc-epi-lake-2026"
SALIDA_EPI = f"{BUCKET}/processed/epi_clean/"
SALIDA_EPI_BQ = f"{BUCKET}/processed/epi_clean_bq/"
SALIDA_IPRESS = f"{BUCKET}/processed/ipress_dim/"

ANIO_MIN = 2000
ANIO_MAX = 2024

# Nombres por posicion. El archivo tiene 33 columnas y se lee en Latin-1.
COLUMNAS_IPRESS = [
    "institucion", "codigo_unico", "nombre", "clasificacion", "tipo",
    "departamento", "provincia", "distrito", "ubigeo", "direccion",
    "codigo_disa", "codigo_red", "codigo_microrred", "disa", "red",
    "microrred", "codigo_ue", "unidad_ejecutora", "categoria", "telefono",
    "tipo_doc_categorizacion", "nro_doc_categorizacion", "horario",
    "inicio_actividad", "director", "estado", "situacion", "condicion",
    "inspeccion", "norte", "este", "cota", "camas",
]

# Solo se conserva lo que usa el proyecto. Fuera telefono y director.
COLUMNAS_IPRESS_UTILES = [
    "institucion", "codigo_unico", "nombre", "clasificacion", "tipo",
    "departamento", "provincia", "distrito", "ubigeo", "disa", "red",
    "microrred", "categoria", "condicion", "norte", "este", "camas",
]


def leer(spark, fuente, separador, encoding):
    return (spark.read
            .option("header", True)
            .option("delimiter", separador)
            .option("quote", '"')
            .option("encoding", encoding)
            .csv(f"{BUCKET}/raw/{fuente}/*"))


def normalizar(df, grupo):
    """Quita duplicados exactos, valida llaves y deja una fila por caso."""
    total = df.count()

    # El dropDuplicates va sobre las 14 columnas: cada fila es un caso.
    # Si se deduplica solo la llave, todos los conteos quedan en 1.
    limpio = (df.dropDuplicates()
              .withColumn("ubigeo", F.lpad(F.trim(F.col("ubigeo")), 6, "0"))
              .withColumn("ano", F.col("ano").cast("int"))
              .withColumn("semana", F.col("semana").cast("int"))
              .withColumn("grupo", F.lit(grupo))
              .select("ubigeo", "ano", "semana", "grupo"))

    sin_duplicados = limpio.count()

    validos = limpio.filter(
        F.col("ubigeo").rlike("^[0-9]{6}$") &
        F.col("ano").between(ANIO_MIN, ANIO_MAX) &
        F.col("semana").between(1, 53)
    ).cache()

    n_validos = validos.count()
    print(f"{grupo}: crudas={total} sin_duplicados={sin_duplicados} "
          f"validas={n_validos} rechazadas={sin_duplicados - n_validos}")
    return validos


def main():
    spark = SparkSession.builder.appName("02-clean").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    dengue = leer(spark, "dengue", ";", "UTF-8")
    carrion = leer(spark, "carrion", ",", "UTF-8")
    ipress = leer(spark, "ipress", ",", "ISO-8859-1")

    print(f"raw dengue={dengue.count()} carrion={carrion.count()} "
          f"ipress={ipress.count()}")

    d = normalizar(dengue, "DENGUE")
    c = normalizar(carrion, "CARRION")

    # Una fila por distrito, ano, semana y grupo.
    observado = (d.select("ubigeo", "ano", "semana", "grupo")
                 .unionByName(c.select("ubigeo", "ano", "semana", "grupo"))
                 .groupBy("ubigeo", "ano", "semana", "grupo")
                 .agg(F.count(F.lit(1)).alias("casos"))
                 .cache())
    print(f"panel observado: {observado.count()} filas")

    # Indice semanal continuo sobre las semanas realmente publicadas.
    calendario = (observado.select("ano", "semana").distinct()
                  .withColumn("t", (F.row_number().over(
                      Window.orderBy("ano", "semana")) - 1).cast("int")))
    observado = observado.join(calendario, ["ano", "semana"])

    # Rango observado por distrito y grupo, para no inventar ceros fuera.
    rango = observado.groupBy("ubigeo", "grupo").agg(
        F.min("t").alias("t_min"),
        F.max("t").alias("t_max"))

    panel = (rango
             .select("ubigeo", "grupo",
                     F.explode(F.sequence("t_min", "t_max")).alias("t"))
             .join(calendario.select("t", "ano", "semana"), "t")
             .join(observado.select("ubigeo", "grupo", "t", "casos"),
                   ["ubigeo", "grupo", "t"], "left")
             .fillna({"casos": 0})
             .withColumn("casos", F.col("casos").cast("int"))
             .withColumn("dep", F.substring("ubigeo", 1, 2)))

    # Dimension IPRESS.
    if len(ipress.columns) != len(COLUMNAS_IPRESS):
        raise ValueError(
            f"IPRESS tiene {len(ipress.columns)} columnas, "
            f"se esperaban {len(COLUMNAS_IPRESS)}")

    ipress = ipress.toDF(*COLUMNAS_IPRESS)

    ipr_dim = (ipress.select(*COLUMNAS_IPRESS_UTILES)
               .withColumn("ubigeo", F.lpad(F.trim(F.col("ubigeo")), 6, "0"))
               .filter(F.col("ubigeo").rlike("^[0-9]{6}$"))
               .withColumn("norte", F.col("norte").cast("double"))
               .withColumn("este", F.col("este").cast("double"))
               .withColumn("camas", F.col("camas").cast("int"))
               .dropDuplicates(["codigo_unico"]))

    print(f"ipress unicos por codigo={ipr_dim.count()} "
          f"ubigeos={ipr_dim.select('ubigeo').distinct().count()}")

    conteo = ipr_dim.groupBy("ubigeo").agg(F.count(F.lit(1)).alias("n_ipress"))

    panel = (panel.join(F.broadcast(conteo), "ubigeo", "left")
             .fillna({"n_ipress": 0})
             .withColumn("n_ipress", F.col("n_ipress").cast("int"))
             .select("ubigeo", "dep", "grupo", "ano", "semana", "t",
                     "casos", "n_ipress"))

    panel = panel.cache()

    total = panel.count()
    con_casos = panel.filter(F.col("casos") > 0).count()
    casos_total = panel.agg(F.sum("casos")).first()[0]
    print(f"panel final: filas={total} con_casos={con_casos} "
          f"ceros={total - con_casos} casos={casos_total}")
    print(f"distritos={panel.select('ubigeo').distinct().count()} "
          f"semanas={panel.select('t').distinct().count()}")
    print(f"filas sin IPRESS={panel.filter(F.col('n_ipress') == 0).count()}")

    panel.write.mode("overwrite").partitionBy("ano").parquet(SALIDA_EPI)
    # Copia plana: BigQuery no ve "ano" cuando es solo particion.
    panel.write.mode("overwrite").parquet(SALIDA_EPI_BQ)
    ipr_dim.write.mode("overwrite").parquet(SALIDA_IPRESS)

    print("FIN 02_clean: parquet en processed/epi_clean/, "
          "processed/epi_clean_bq/ y processed/ipress_dim/")
    panel.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
