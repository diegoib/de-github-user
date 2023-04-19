-- Hay que crear los buckets de Prefect (por ejemplo el de GCS)

1) hay que crear un proyecto de GCP, cambiar nombre en 
2) añadir permisos al service account: Storage Admin, Storage Object Admin, Big Query Admin
3) tener instalado: python (instalar requirements), terraform
3) autenticar la máquina local con la cuenta de google: 
    ```bash
    gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
    ```
4) ejecutar desde la carpeta de terraform
    ```bash
    terraform init
    terraform plan #poner project ID de GCP
    terraform apply
    ```
5) ejecutar 
    ```bash
    prefect start orion
    ```
    y crear 2 blocks: uno de `GCP Credentials` y otro de `GCS Bucket`
     

