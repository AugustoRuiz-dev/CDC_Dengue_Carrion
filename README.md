# Proyecto CDC Perú - Big Data y Analítica de Datos

Predicción semanal de casos de dengue y enfermedad de Carrión por distrito y priorización de las 25 regiones del Perú.

## Pipeline cloud

- **Ingesta**: manual (`bq load` o `gcloud storage cp`).
- **Almacenamiento**: `gs://cdc-epi-lake-2026/{raw,processed,code,results,logs}`.
- **Procesamiento**: PySpark en Dataproc (clúster `n4-standard-4`, un nodo).
- **Visualización**: Looker Studio sobre BigQuery (`dataset epi_analytics`).
- **Modelo**: `lineal_ampliada` (t+1 y t+2), elegido por MAE de validación y RMSE de desempate.
- **Priorización**: SQL `06_priorizacion.sql` con ranking por enfermedad, índice escalado a 0-100 y `estado_dato` para "sin predicción" vs cero.

## Estructura

```
CDC_BIG_DATA/
├─ code/
│  ├─ 01_explore.py
│  ├─ 02_clean.py
│  ├─ 03_opt.py
│  ├─ 04_scale.py
│  ├─ 05_models.py
│  └─ 06_priorizacion.sql
├─ data_sample/
├─ README.md
├─ requirements.txt
└─ .gitignore
```

Las etapas se ingresan por commits, cada uno con su carpeta `logs/01..06`. `results/` solo aparece con la salida de `05_models.py`.

## Etapas

| Orden | Script | Entorno | Salida |
|---|---|---|---|
| 01 | `01_explore.py` | Dataproc | Log de perfilado |
| 02 | `02_clean.py` | Dataproc | `processed/epi_clean/`, `ipress_dim/` |
| 03 | `03_opt.py` | Dataproc | Log de optimización |
| 04 | `04_scale.py` | Dataproc | Log de escalabilidad |
| 05 | `05_models.py` | Dataproc | `model_metrics.csv`, `predicciones.csv` |
| 06 | `06_priorizacion.sql` | BigQuery | Tabla `priorizacion_regiones`, vista `vw_panel_priorizacion` |

La ingesta a `raw/` y la carga a BigQuery son pasos manuales.

## Requisitos

- Proyecto de Google Cloud con facturación habilitada.
- APIs activas: Cloud Storage, Dataproc y BigQuery.
- `gcloud` instalado, o Cloud Shell.
- Python 3.11 con `pip install -r requirements.txt` para revisar resultados en local.

## Data lake

```
cdc-epi-lake-2026/
├─ raw/          dengue/ carrion/ ipress/
├─ processed/    epi_clean/ ipress_dim/ boletines_master/
└─ results/
```

## Ingesta

```powershell
gcloud storage cp "datos_abiertos_vigilancia_dengue_2000_2024.csv" gs://cdc-epi-lake-2026/raw/dengue/
gcloud storage cp "datos_abiertos_vigilancia_enfermedad_carrion_2000_2024.csv" gs://cdc-epi-lake-2026/raw/carrion/
gcloud storage cp .\IPRESS.csv gs://cdc-epi-lake-2026/raw/ipress/
```

| Fuente | Separador | Codificación | Registros |
|---|---|---|---|
| Dengue | `;` | UTF-8 | 1,029,421 |
| Carrión | `,` | UTF-8 | 46,120 |
| IPRESS | `,` | Latin-1 | 20,819 |

## Clúster

```powershell
gcloud dataproc clusters create epi-cluster \
  --region=southamerica-east1 \
  --zone=southamerica-east1-b \
  --image-version=2.3.39-debian12 \
  --single-node \
  --master-machine-type=n4-standard-4 \
  --master-boot-disk-size=500GB
```

## Ejecutar

```powershell
gcloud dataproc jobs submit pyspark code/01_explore.py \
  --cluster=epi-cluster --region=southamerica-east1 2>&1 |
  Tee-Object -FilePath logs/01_explore/01_explore.log

# En Cloud Shell: tee en lugar de Tee-Object.
```

## BigQuery y priorización

```powershell
bq mk --location=southamerica-east1 epi_analytics
bq load --source_format=PARQUET --replace epi_analytics.epi_clean "gs://cdc-epi-lake-2026/processed/epi_clean/*.parquet"
bq load --source_format=PARQUET --replace epi_analytics.ipress_dim "gs://cdc-epi-lake-2026/processed/ipress_dim/*.parquet"
bq query --use_legacy_sql=false < code/06_priorizacion.sql
```

`epi_clean` conserva `ano` como columna entera.

## Modelos

Se comparan modelos lineales: `lineal_base` y `lineal_ampliada`. Elección por **MAE en validación** y **RMSE como desempate**.

- `lineal_ampliada` gana en t+1 (MAE 1.8286, mejora 30.56% vs base) y t+2 (MAE 2.4214, mejora 24.31%).
- `random_forest` y `gradient_boosting` no entrenan por límite de `maxBins` (feature `dep_idx` tiene 23 valores, `maxBins=16` es insuficiente).
- Sin `persistencia` ni `estacional`: se eliminaron por petición.

| Horizonte | Modelo | MAE validación | Mejora vs base |
|---|---|---|---|
| t+1 | lineal_ampliada | 1.8286 | 30.56% |
| t+2 | lineal_ampliada | 2.4214 | 24.31% |

- Sin nombres de personas ni teléfonos en informe, logs ni repositorio.
- `02_clean.py` excluye `Teléfono` y `Director Médico`.

## Privacidad y costos

- Sin nombres de personas ni teléfonos en el informe, logs o el repositorio.
- `02_clean.py` excluye `Teléfono` y `Director Médico` del catálogo IPRESS.
- El costo se concentra en las máquinas del clúster y en el disco de 500 GB. Detener el clúster no elimina el disco.

```powershell
gcloud dataproc clusters delete epi-cluster --region=southamerica-east1
```

`data_sample/` contiene las primeras filas de cada fuente con el mismo formato. Permite revisar la lectura, los separadores y las codificaciones. Los scripts del pipeline leen de Cloud Storage, por lo que para ejecutarlos se necesitan los archivos completos en `raw/`.

---
*Actualizado con la Etapa 06: ranking de dengue y enfermedad de Carrión, vista panel con selector de enfermedad, índice escalado a 0-100 y estados de dato ("Con predicción"/"Sin predicción").*