# Cloud-func-load-CSV-to-BQ
Trigger a function to load cloud storage CSV file to big query table

# How to execute through cloud build
cmd: gcloud builds submit --config cloudbuild.yaml function-source.zip

function-source.zip contain main.py  requirements.txt  .gcloudignore  files

cmd: zip function-source.zip main.py requirements.txt .gcloudignore