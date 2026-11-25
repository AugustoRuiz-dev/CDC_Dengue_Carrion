from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BUCKET = "gs://cdc-epi-lake-2026"

FUENTES = [
    ("dengue", f"{BUCKET}/raw/dengue/*", ";", "UTF-8"),
    ("carrion", f"{BUCKET}/raw/carrion/*", ",", "UTF-8"),
    ("ipress", f"{BUCKET}/raw/ipress/*", ",", "ISO-8859-1"),
]

LLAVES = ["ubigeo", "UBIGEO", "ano", "semana"]


def perfil(spark, nombre, ruta, separador, encoding):
    print(f"\n===== FUENTE {nombre.upper()} =====")
    print(f"ruta       : {ruta}")
    print(f"separador  : {separador}")
    print(f"encoding   : {encoding}")

    df = (spark.read
          .option("header", True)
          .option("delimiter", separador)
          .option("quote", '"')
          .option("encoding", encoding)
          .csv(ruta))

    columnas = df.columns
    total = df.count()
    print(f"columnas   : {len(columnas)}")
    print(f"filas      : {total}")

    print("--- esquema ---")
    df.printSchema()

    print("--- 5 primeras filas ---")
    df.show(5, truncate=True)

    presentes = [c for c in LLAVES if c in columnas]
    if presentes:
        print("--- nulos en llaves ---")
        df.select([F.sum(F.col(c).isNull().cast("int")).alias(f"nulos_{c}")
                   for c in presentes]).show()

    if "ano" in columnas:
        df.select(F.min(F.col("ano").cast("int")).alias("ano_min"),
                  F.max(F.col("ano").cast("int")).alias("ano_max")).show()
    if "semana" in columnas:
        df.select(F.min(F.col("semana").cast("int")).alias("semana_min"),
                  F.max(F.col("semana").cast("int")).alias("semana_max")).show()

    duplicados = total - df.dropDuplicates().count()
    print(f"duplicados exactos: {duplicados}")
    print(f"filas unicas      : {total - duplicados}")


def main():
    spark = SparkSession.builder.appName("01-explore").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"spark master: {spark.sparkContext.master}")
    print(f"version     : {spark.version}")

    for nombre, ruta, separador, encoding in FUENTES:
        perfil(spark, nombre, ruta, separador, encoding)

    print("\nFIN 01_explore: perfilado completo, sin transformacion de datos")
    spark.stop()


if __name__ == "__main__":
    main()
