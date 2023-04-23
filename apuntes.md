-- Hay que crear los buckets de Prefect (por ejemplo el de GCS)

1) hay que crear un proyecto de GCP, cambiar nombre en 
2) añadir permisos al service account: Storage Admin, Storage Object Admin, Big Query Admin, Dataproc Admin. Descargar credenciales y crear la env variable GOOGLE_APPLICATION_CREDENTIALS
3) tener instalado: python (instalar requirements), terraform, Google SDK (si en GVM ya lo tiene instalado)
4) autenticar la máquina local con la cuenta de google: 
    ```bash
    gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
    ```
5) ejecutar desde la carpeta de terraform
    ```bash
    terraform init
    terraform plan #poner project ID de GCP
    terraform apply
    ```
6) Copiar el código `transformation_spark.py` al bucket
7) ejecutar 
    ```bash
    prefect start orion
    ```
    y crear 2 blocks: uno de `GCP Credentials` y otro de `GCS Bucket`
8) Crear un cluster Dataproc, single node, y tomar el nombre del bucket temporal
     


----------
''' CODIGO DE BASH PARA SUBMIT A JOB TO DATAPROC

gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=europe-west6 \
    -- jars=gs//spark-lib/bigquey/spark-bigquery-latest_2.12.jar \
    gs://bucket_path/spark_file.py
    --
        --input_green=ksksaka
        --args...
'''

''' CODIGO DE SPARK PARA ESCRIBIR A BIGQUERY

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-europe-west6-828225226997-fckhkym8')

df_result.write.format('bigquery') \
    .option('table', output) \
    .save()
'''

output es un parámetro con el schema.table de bigquery


```
gsutil cp src/transformation_spark.py gs://de_project_bucket_de-zoomcamp-386310/code/transformation_spark.py



*****

gcloud dataproc jobs submit pyspark \
            --cluster=cluster-2f70 \
            --region=europe-west2 \
            --jars=gs://spark-lib/bigquery/spark-3.1-bigquery-0.30.0.jar \
            gs://de_project_bucket_de-zoomcamp-386310/code/transformation_spark.py \
            -- \
                --temp_dataproc_bucket=dataproc-temp-europe-west2-798822437747-927ytg0x \
                --data_bucket=de_project_bucket_de-zoomcamp-386310  \
                --bigquery_table=de_project.report \
                --year=2023 \
                --month=1 \
                --day=16


1. escribir los pasos y objetivo en el README
2. añadir particion (hacer): DONE
3. como se orquestran todos los pasos? 
4. ver si con os.system aparece la log: DONE
5. añadir requirements: DONE
6. generar blocks a través de python