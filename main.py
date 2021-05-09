import os
from google.cloud import bigquery

def csv_loader(data, context):
        client = bigquery.Client()
        dataset_id = 'ds_bigmart'
        dataset_ref = client.dataset(dataset_id)
        job_config = bigquery.LoadJobConfig()
        job_config.schema = [
                bigquery.SchemaField('Item_Identifier', 'STRING'),
				bigquery.SchemaField('Item_Weight', 'FLOAT'),
				bigquery.SchemaField('Item_Fat_Content', 'STRING'),
				bigquery.SchemaField('Item_Visibility', 'FLOAT'),
				bigquery.SchemaField('Item_Type', 'STRING'),
				bigquery.SchemaField('Item_MRP', 'FLOAT'),
				bigquery.SchemaField('Outlet_Identifier', 'STRING'),
				bigquery.SchemaField('Outlet_Establishment_Year', 'INTEGER'),
				bigquery.SchemaField('Outlet_Size', 'STRING'),
				bigquery.SchemaField('Outlet_Location_Type', 'STRING'),
				bigquery.SchemaField('Outlet_Type', 'STRING'),
				bigquery.SchemaField('Item_Outlet_Sales', 'FLOAT')
                ]
        job_config.skip_leading_rows = 1
        job_config.source_format = bigquery.SourceFormat.CSV

        # get the URI for uploaded CSV in GCS from 'data'
        uri = 'gs://third-campus-303308-func-bq-load/' + data['name']

        # lets do this
        load_job = client.load_table_from_uri(
                uri,
                dataset_ref.table('bigmart'),
                job_config=job_config)

        print('Starting job {}'.format(load_job.job_id))
        print('Function=csv_loader, Version= v14')
        print('File: {}'.format(data['name']))

        load_job.result()  # wait for table load to complete.
        print('Job finished.')

        destination_table = client.get_table(dataset_ref.table('bigmart'))
        print('Loaded {} rows.'.format(destination_table.num_rows))