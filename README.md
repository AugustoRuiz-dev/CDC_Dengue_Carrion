# Proyecto CDC Perú - Big Data y Analítica de Datos

Predicción semanal de casos de dengue y enfermedad de Carrión por distrito y
priorización de las 25 regiones del Perú.

Pipeline cloud sobre Google Cloud: Cloud Storage como data lake, Dataproc con
PySpark para el procesamiento y BigQuery para la priorización. Los datos crudos
y las credenciales no se versionan.

## Estructura

```
CDC_BIG_DATA/
├─ code/01_explore.py
├─ logs/01_explore/01_explore.log
├─ data_sample/
├─ README.md
├─ requirements.txt
└─ .gitignore
```

Los scripts 02 a 06 se agregan por commits, junto con su carpeta de logs.

## Etapas

| Orden | Script | Entorno | Salida |
|---|---|---|---|
| 01 | `01_explore.py` | Dataproc | Log de perfilado |
| 02 | `02_clean.py` | Dataproc | `processed/epi_clean/`, `processed/ipress_dim/` |
| 03 | `03_opt.py` | Dataproc | Log de optimización |
| 04 | `04_scale.py` | Dataproc | Log de escalabilidad |
| 05 | `05_models.py` | Dataproc | `results/model_metrics.csv`, `results/predicciones.csv` |
| 06 | `06_priorizacion.sql` | BigQuery | Tabla `priorizacion_regiones` |

La ingesta a `raw/` y la carga a BigQuery son pasos manuales.

## Requisitos

- Proyecto de Google Cloud con facturación habilitada.
- APIs activas: Cloud Storage, Dataproc y BigQuery.
- `gcloud` instalado, o Cloud Shell.
- Python 3.11 con `pip install -r requirements.txt` para revisar resultados en local.

## Data lake

```powershell
gcloud storage buckets create gs://cdc-epi-lake-2026 --location=southamerica-east1 --uniform-bucket-level-access
```

```
cdc-epi-lake-2026/
├─ raw/          dengue/ carrion/ ipress/ boletines/
├─ processed/    epi_clean/ ipress_dim/ boletines_master/ boletines_detalle/
└─ results/
```

## Ingesta

```powershell
gcloud storage cp "datos_abiertos_vigilancia_dengue_2000_2024.csv" gs://cdc-epi-lake-2026/raw/dengue/
gcloud storage cp "datos_abiertos_vigilancia_enfermedad_carrion_2000_2024.csv" gs://cdc-epi-lake-2026/raw/carrion/
gcloud storage cp "IPRESS.csv" gs://cdc-epi-lake-2026/raw/ipress/
gcloud storage cp .\boletines\*.pdf gs://cdc-epi-lake-2026/raw/boletines/
```

| Fuente | Separador | Codificación | Registros |
|---|---|---|---|
| Dengue | `;` | UTF-8 | 1,029,421 |
| Carrión | `,` | UTF-8 | 46,120 |
| IPRESS | `,` | Latin-1 | 20,819 |

## Clúster

```powershell
gcloud dataproc clusters create epi-cluster `
  --region=southamerica-east1 `
  --zone=southamerica-east1-b `
  --image-version=2.3.39-debian12 `
  --single-node `
  --master-machine-type=n4-standard-4 `
  --master-boot-disk-size=500GB
```

## Ejecutar

También se puede enviar desde la consola de Dataproc, con el script en
`gs://cdc-epi-lake-2026/code/`.

```powershell
gcloud dataproc jobs submit pyspark code/01_explore.py `
  --cluster=epi-cluster --region=southamerica-east1 2>&1 |
  Tee-Object -FilePath logs/01_explore/01_explore.log
```

En Cloud Shell se usa `tee` en lugar de `Tee-Object`.

## BigQuery y priorización

```powershell
bq mk --location=southamerica-east1 epi_analytics
bq load --source_format=PARQUET --replace epi_analytics.epi_clean "gs://cdc-epi-lake-2026/processed/epi_clean/*.parquet"
bq load --source_format=PARQUET --replace epi_analytics.ipress_dim "gs://cdc-epi-lake-2026/processed/ipress_dim/*.parquet"
bq query --use_legacy_sql=false < code/06_priorizacion.sql
```

`epi_clean` conserva `ano` como columna entera, no solo como partición.

## Modelos

Se comparan diez modelos: cero, persistencia, persistencia estacional, lineal
base, lineal ampliada, Ridge, Lasso, Poisson, Random Forest y Gradient Boosting.

| Conjunto | Periodo | Uso |
|---|---|---|
| Entrenamiento | 2000-2021 | Ajuste |
| Validación | 2022-2023 | Selección por MAE |
| Prueba | 2024 | Reporte final |

Horizontes: t+1 y t+2. La prueba no participa en la selección.

## Resultados

- `results/model_metrics.csv`: RMSE, MAE, MAE en semanas con casos, sesgo y tiempo por modelo.
- `results/predicciones.csv`: predicciones del modelo elegido en prueba.
- `results/modelos/`: modelo elegido por horizonte.

## Privacidad y costos

- Sin nombres de personas ni teléfonos en el informe, los logs o el repositorio.
- `02_clean.py` excluye `Teléfono` y `Director Médico` del catálogo IPRESS.
- El costo se concentra en las máquinas del clúster y en el disco de 500 GB. Detener el clúster no borra el disco.

```powershell
gcloud dataproc clusters delete epi-cluster --region=southamerica-east1
```

`data_sample/` contiene las primeras filas de cada fuente para revisar formatos
sin descargar los archivos completos.
