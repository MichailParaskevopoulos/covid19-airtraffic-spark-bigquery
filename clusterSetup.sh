gcloud services enable compute.googleapis.com \
  dataproc.googleapis.com \
  bigquerystorage.googleapis.com

export REGION='us-central1'
export PROJECT_ID='covid19flights'
export CLUSTER_NAME='covid19flights'

gcloud config set project ${PROJECT_ID}

gcloud dataproc clusters create ${CLUSTER_NAME} \
    --worker-machine-type n1-standard-4 \
    --num-workers 0 \
    --image-version 2.0.5-debian10 \
    --region ${REGION} \
    --max-idle=30m \
    --initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
    --metadata gcs-connector-version=2.2.0 \
    --metadata bigquery-connector-version=1.2.0 \
    --metadata spark-bigquery-connector-version=0.19.1

gcloud dataproc jobs submit pyspark \
    gs://pyspark_job_files/pyspark_airtraffic.py \
    --cluster ${CLUSTER_NAME} \
    --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar

gcloud dataproc jobs submit pyspark \
    gs://pyspark_job_files/pyspark_aircraft_types.py \
    --cluster ${CLUSTER_NAME} \
    --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
    
gcloud dataproc jobs submit pyspark \
    gs://pyspark_job_files/pyspark_airlines.py \
    --cluster ${CLUSTER_NAME} \
    --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
