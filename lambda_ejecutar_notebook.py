import boto3
import os
import sys

# Inicializa los clientes de S3 y Lambda
s3_client = boto3.client('s3')

# Ruta donde se guardará temporalmente el archivo .py
local_file_path = '/tmp/forecast_aws.py'

def lambda_handler(event, context):
    # Nombre del bucket y del archivo .py en S3
    bucket_name = 'exit-bucket-sae'
    s3_file_key = 'python_script/forecast_aws.py'
    
    try:
        # Descargar el archivo .py desde S3 a la instancia Lambda
        s3_client.download_file(bucket_name, s3_file_key, local_file_path)
        
        # Ejecutar el archivo .py descargado
        exec(open(local_file_path).read())
        
        return {
            'statusCode': 200,
            'body': 'Notebook script executed successfully!'
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': f'Error executing the notebook script: {str(e)}'
        }


