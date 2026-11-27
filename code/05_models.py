import math
import os
import time

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import (GBTRegressor, LinearRegression,
                                   RandomForestRegressor)
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# Configuracion general
# ---------------------------------------------------------------------------
BUCKET = "gs://cdc-epi-lake-2026"
# OUT permite escribir en local para una prueba: OUT=/tmp/results
OUT = os.environ.get("OUT", f"{BUCKET}/results")

# Los cortes usan el ano del objetivo, no el ano de la fila.
ANIO_ENTRENAMIENTO = 2021
ANIO_VALIDACION = (2022, 2023)
ANIO_PRUEBA = 2024

# Variables del modelo lineal base.
FEAT_BASE = ["lag1", "lag2", "semana", "n_ipress", "grupo_dengue"]

# Variables del modelo ampliado: rezagos, promedios moviles,
# estacionalidad y ubicacion.
FEAT_AMPLIADA = [
    "lag1", "lag2", "lag3", "lag4",
    "roll4_mean", "roll4_max", "casos_anio_prev",
    "semana", "sin_sem", "cos_sem",
    "n_ipress", "grupo_dengue", "dep_idx",
]

# Una fila por modelo, horizonte y conjunto evaluado.
ESQUEMA_METRICAS = StructType([
    StructField("modelo", StringType(), False),
    StructField("horizonte", IntegerType(), False),
    StructField("conjunto", StringType(), False),
    StructField("rmse", DoubleType(), False),
    StructField("mae", DoubleType(), False),
    StructField("mae_positivos", DoubleType(), False),
    StructField("sesgo", DoubleType(), False),
    StructField("n", IntegerType(), False),
    StructField("segundos", DoubleType(), False),
])


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def escribir_csv_unico(spark, df, ruta):
    """Escribe un solo CSV, con ruta local o de GCS."""
    # Spark escribe una carpeta con varios part-*.csv. Se escribe en una
    # carpeta temporal y se mueve el unico archivo a la ruta final.
    temporal = f"{ruta}_tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temporal)

    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    origen = jvm.org.apache.hadoop.fs.Path(temporal)
    fs = origen.getFileSystem(conf)
    destino = jvm.org.apache.hadoop.fs.Path(ruta)

    # Si ya existe un archivo de una corrida anterior, se reemplaza.
    if fs.exists(destino):
        fs.delete(destino, False)
    for estado in fs.listStatus(origen):
        if estado.getPath().getName().startswith("part-"):
            fs.rename(estado.getPath(), destino)
            break
    fs.delete(origen, True)


def predecir(nombre, modelo, df):
    """Devuelve una columna pred. Los conteos no pueden ser negativos."""
    # Las dos referencias no usan un modelo entrenado.
    if nombre == "persistencia":
        return df.withColumn("pred", F.col("lag1").cast("double"))
    if nombre == "estacional":
        return df.withColumn("pred", F.col("casos_anio_prev").cast("double"))
    # Los modelos necesitan la columna features que crea VectorAssembler.
    if "features" not in df.columns:
        raise ValueError(f"{nombre}: el conjunto no tiene la columna features")
    # Los modelos pueden predecir valores negativos; se recortan a cero.
    return modelo.transform(df).withColumn(
        "pred", F.greatest(F.lit(0.0), F.col("prediction")))


def evaluar(pred, modelo, horizonte, conjunto, segundos):
    # Todas las metricas se calculan en una sola pasada sobre las filas.
    fila = pred.agg(
        F.sqrt(F.avg((F.col("pred") - F.col("label")) ** 2)).alias("rmse"),
        F.avg(F.abs(F.col("pred") - F.col("label"))).alias("mae"),
        F.avg(F.when(F.col("label") > 0,
                     F.abs(F.col("pred") - F.col("label")))).alias("mae_pos"),
        F.avg(F.col("pred") - F.col("label")).alias("sesgo"),
        F.count(F.lit(1)).alias("n"),
    ).first()

    return {
        "modelo": modelo,
        "horizonte": horizonte,
        "conjunto": conjunto,
        "rmse": round(float(fila["rmse"]), 4),
        "mae": round(float(fila["mae"]), 4),
        "mae_positivos": round(float(fila["mae_pos"] or 0.0), 4),
        "sesgo": round(float(fila["sesgo"]), 4),
        "n": int(fila["n"]),
        "segundos": round(float(segundos), 2),
    }


def modelos_a_entrenar():
    """Modelos candidatos. Cada uno se instancia de nuevo por horizonte."""
    return [
        ("lineal_base", "base",
         LinearRegression(featuresCol="features", labelCol="label", maxIter=50)),
        ("lineal_ampliada", "amp",
         LinearRegression(featuresCol="features", labelCol="label", maxIter=50)),
        ("random_forest", "amp",
         RandomForestRegressor(featuresCol="features", labelCol="label",
                               numTrees=30, maxDepth=6, maxBins=16,
                               maxMemoryInMB=32, seed=42)),
        ("gradient_boosting", "amp",
         GBTRegressor(featuresCol="features", labelCol="label",
                      maxIter=30, maxDepth=4, maxBins=16,
                      maxMemoryInMB=32, stepSize=0.1, seed=42)),
    ]


def main():
    spark = SparkSession.builder.appName("05-models").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # -----------------------------------------------------------------------
    # 1. Panel limpio
    # -----------------------------------------------------------------------
    # El panel ya trae una fila por distrito, grupo y semana, con ceros.
    epi = spark.read.parquet(f"{BUCKET}/processed/epi_clean/").cache()

    # Respaldo por si la copia leida no tuviera el indice semanal.
    if "t" not in epi.columns:
        calendario = epi.select("ano", "semana").distinct()
        calendario = calendario.withColumn(
            "t", (F.row_number().over(
                Window.orderBy("ano", "semana")) - 1).cast("int"))
        epi = epi.join(calendario, ["ano", "semana"])

    filas_panel = epi.count()
    print(f"panel filas={filas_panel} distritos="
          f"{epi.select('ubigeo').distinct().count()}")

    # -----------------------------------------------------------------------
    # 2. Rezagos, promedios moviles y objetivos
    # -----------------------------------------------------------------------
    # Una serie por distrito y enfermedad, ordenada por semana continua.
    ventana = Window.partitionBy("ubigeo", "grupo").orderBy("t")
    ventana4 = ventana.rowsBetween(-3, 0)

    d = (epi
         # Semanas anteriores. lag1 es la semana previa, aunque tenga cero.
         .withColumn("lag1", F.lag("casos", 1).over(ventana))
         .withColumn("lag2", F.lag("casos", 2).over(ventana))
         .withColumn("lag3", F.lag("casos", 3).over(ventana))
         .withColumn("lag4", F.lag("casos", 4).over(ventana))
         # Promedio y maximo de las ultimas cuatro semanas, con la actual.
         .withColumn("roll4_mean", F.avg("casos").over(ventana4))
         .withColumn("roll4_max", F.max("casos").over(ventana4))
         # Estacionalidad: la semana del ano como seno y coseno.
         .withColumn("sin_sem",
                     F.sin(2 * math.pi * F.col("semana") / 53))
         .withColumn("cos_sem",
                     F.cos(2 * math.pi * F.col("semana") / 53))
         .withColumn("grupo_dengue",
                     (F.col("grupo") == "DENGUE").cast("int"))
         .withColumn("dep", F.substring("ubigeo", 1, 2))
         # Objetivos: casos de la semana siguiente y de dos semanas despues.
         .withColumn("y_t1", F.lead("casos", 1).over(ventana))
         .withColumn("y_t2", F.lead("casos", 2).over(ventana)))

    # Casos de la misma semana del ano anterior, sin depender del indice t.
    previo = (epi.select(
        F.col("ubigeo"), F.col("grupo"),
        (F.col("ano") + 1).alias("ano"),
        F.col("semana"),
        F.col("casos").alias("casos_anio_prev")))

    # El ano se desplaza para unir la fila actual con la del ano anterior.
    d = (d.join(previo, ["ubigeo", "grupo", "ano", "semana"], "left")
         .fillna({"casos_anio_prev": 0})
         .cache())

    calendario_t = epi.select("t", "ano", "semana").distinct()

    resultados = []
    predicciones_prueba = []

    # -----------------------------------------------------------------------
    # 3. Un horizonte por vez
    # -----------------------------------------------------------------------
    for horizonte, objetivo in [(1, "y_t1"), (2, "y_t2")]:
        print(f"\n===== HORIZONTE t+{horizonte} =====")

        # Calendario del objetivo: para cada t, el ano y la semana de t+h.
        mapa = (calendario_t
                .withColumn("t", (F.col("t") - horizonte).cast("int"))
                .withColumnRenamed("ano", "ano_obj")
                .withColumnRenamed("semana", "semana_obj"))

        # Una fila por distrito, grupo y semana, con su objetivo y su fecha.
        # Se exige historia minima de cuatro semanas y objetivo no nulo.
        base = (d.join(mapa, "t", "inner")
                .withColumn("label", F.col(objetivo).cast("double"))
                .filter(F.col("label").isNotNull() &
                        F.col("lag4").isNotNull()))

        # Cortes temporales por el ano del objetivo.
        entrenamiento = base.filter(F.col("ano_obj") <= ANIO_ENTRENAMIENTO)
        validacion = base.filter(F.col("ano_obj").between(*ANIO_VALIDACION))
        prueba = base.filter(F.col("ano_obj") == ANIO_PRUEBA)

        print(f"filas entrenamiento={entrenamiento.count()} "
              f"validacion={validacion.count()} prueba={prueba.count()}")

        # -------------------------------------------------------------------
        # 4. Indice de departamento
        # -------------------------------------------------------------------
        # El departamento se codifica con el entrenamiento y se reutiliza.
        indice = StringIndexer(inputCol="dep", outputCol="dep_idx",
                               handleInvalid="keep").fit(entrenamiento)
        entrenamiento = indice.transform(entrenamiento).cache()
        entrenamiento.count()
        validacion = indice.transform(validacion)
        prueba = indice.transform(prueba)

        # Cada conjunto de variables se ensambla una sola vez.
        conjuntos = {}
        for tipo, columnas in [("base", FEAT_BASE), ("amp", FEAT_AMPLIADA)]:
            ensamblador = VectorAssembler(
                inputCols=columnas, outputCol="features",
                handleInvalid="skip")
            conjuntos[tipo] = (
                ensamblador.transform(entrenamiento).cache(),
                ensamblador.transform(validacion),
                ensamblador.transform(prueba),
            )
        entrenamiento.unpersist()

        # -------------------------------------------------------------------
        # 6. Modelos entrenados
        # -------------------------------------------------------------------
        modelos = {}
        candidatos_modelos = modelos_a_entrenar()
        tipos = {nombre: tipo for nombre, tipo, _ in candidatos_modelos}
        for nombre, tipo, estimador in candidatos_modelos:
            train, val, _ = conjuntos[tipo]
            inicio = time.time()
            try:
                modelo = estimador.fit(train)
            except Exception as error:
                # Si el contexto se detuvo, no tiene sentido seguir: los
                # siguientes modelos fallarian en cadena y ocultarian la causa.
                if spark.sparkContext._jsc.sc().isStopped():
                    raise
                # Un modelo que falla no se evalua y no puede ser elegido.
                print(f"{nombre}: no se pudo entrenar ({error})")
                continue
            segundos = time.time() - inicio
            modelos[nombre] = modelo

            metrica = evaluar(predecir(nombre, modelo, val), nombre,
                              horizonte, "validacion", segundos)
            resultados.append(metrica)
            print(f"{nombre:20s} validacion  mae={metrica['mae']:.4f} "
                  f"rmse={metrica['rmse']:.4f} {segundos:.1f}s")

        # -------------------------------------------------------------------
        # 7. Seleccion por MAE de validacion
        # -------------------------------------------------------------------
        # Entre los modelos entrenados, el de menor MAE (RMSE desempate).
        candidatos = [r for r in resultados
                      if r["horizonte"] == horizonte
                      and r["conjunto"] == "validacion"
                      and r["modelo"] in tipos]
        if not candidatos:
            print(f"modelo elegido t+{horizonte}: ninguno (fallaron todos los modelos)")
            elegido = None
        else:
            elegido = min(candidatos, key=lambda r: (r["mae"], r["rmse"]))["modelo"]
            print(f"modelo elegido t+{horizonte}: {elegido}")

        # -------------------------------------------------------------------
        # 8. Reporte final en prueba
        # -------------------------------------------------------------------
        # Se reportan las predicciones del modelo elegido en prueba.
        if elegido and elegido in modelos:
            datos_elegido = (prueba if elegido not in tipos
                             else conjuntos[tipos[elegido]][2])
            pred = predecir(elegido, modelos.get(elegido), datos_elegido)
            metrica = evaluar(pred, elegido, horizonte, "prueba", 0.0)
            resultados.append(metrica)
            print(f"{elegido:20s} prueba      mae={metrica['mae']:.4f}")

        # Predicciones del elegido, en Parquet por horizonte.
        if elegido and elegido in modelos:
            pred.select(
                "ubigeo", "dep", "grupo",
                F.col("ano_obj").alias("ano"),
                F.col("semana_obj").alias("semana"),
                F.lit(horizonte).alias("horizonte"),
                F.col("label").alias("real"),
                F.col("pred").alias("prediccion"),
            ).write.mode("overwrite").parquet(
                f"{OUT}/predicciones/t{horizonte}")
            predicciones_prueba.append(pred.select(
                "ubigeo", "dep", "grupo",
                F.col("ano_obj").alias("ano"),
                F.col("semana_obj").alias("semana"),
                F.lit(horizonte).alias("horizonte"),
                F.col("label").alias("real"),
                F.col("pred").alias("prediccion")))

        # El modelo entrenado se guarda solo si el elegido es un modelo.
        if elegido in modelos:
            modelos[elegido].write().overwrite().save(
                f"{OUT}/modelos/{elegido}_t{horizonte}")

        # Mejora relativa frente a la linea base.
        prueba_h = {r["modelo"]: r for r in resultados
                    if r["horizonte"] == horizonte
                    and r["conjunto"] == "prueba"
                    and r["modelo"] in tipos}
        if "lineal_base" in prueba_h:
            base_mae = prueba_h["lineal_base"]["mae"]
            elegido_mae = prueba_h[elegido]["mae"] if elegido and elegido in prueba_h else 0
            mejora = 0.0 if base_mae == 0 else (
                (base_mae - elegido_mae) / base_mae * 100)
            print(f"mejora vs lineal_base: {mejora:.2f}%")

        # Se libera la memoria del entrenamiento de este horizonte.
        for datos in conjuntos.values():
            datos[0].unpersist()

    # -----------------------------------------------------------------------
    # 9. Salidas
    # -----------------------------------------------------------------------
    # Tabla de metricas.
    metricas = spark.createDataFrame(resultados, schema=ESQUEMA_METRICAS)
    escribir_csv_unico(spark, metricas, f"{OUT}/model_metrics.csv")

    # Predicciones del modelo elegido en prueba.
    if predicciones_prueba:
        predicciones = predicciones_prueba[0]
        for extra in predicciones_prueba[1:]:
            predicciones = predicciones.unionByName(extra)
        escribir_csv_unico(spark, predicciones, f"{OUT}/predicciones.csv")

    # Resumen de validacion, de menor a mayor MAE.
    print("\n--- metricas de validacion, de menor a mayor MAE ---")
    for horizonte in (1, 2):
        filas_h = sorted(
            [r for r in resultados
             if r["horizonte"] == horizonte
             and r["conjunto"] == "validacion"],
            key=lambda r: r["mae"])
        for r in filas_h:
            print(f"t+{horizonte} {r['modelo']:20s} mae={r['mae']:.4f} "
                  f"mae_pos={r['mae_positivos']:.4f} rmse={r['rmse']:.4f}")

    print(f"\nsalidas en {OUT}")
    print("FIN 05_models: comparacion de modelos y evaluacion final")
    d.unpersist()
    epi.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
