import json
import boto3
import time

# Inicializar clientes de boto3 para interactuar con S3 y Athena
s3_client = boto3.client('s3')
athena_client = boto3.client('athena')

# Definir variables globales
BUCKET_NAME = 'exit-bucket-sae'  # Nombre del bucket S3 donde está el archivo CSV
PREFIX = 'mis-datos/'  # Si está en una carpeta específica, indica el prefijo
DATABASE = 'base_datos_final'  # Nombre de la base de datos en Athena
TABLE_NAME = 'datos_historicos'  # Nombre de la tabla en Athena
S3_OUTPUT = 's3://output-results-lambda/query-results/'  # Ubicación para los resultados de la consulta en Athena

def lambda_handler(event, context):
    try:
        # Listar archivos en el bucket con el prefijo dado
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=PREFIX
        )
        
        # Verificar si hay archivos en el bucket
        if 'Contents' in response:
            # Buscar archivos CSV
            csv_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.csv')]
            
            if csv_files:
                print(f'Archivos CSV encontrados: {csv_files}')
                
                # Tomar el primer archivo CSV (o puedes ajustar esto para seleccionar el que desees)
                selected_csv = csv_files[0]
                print(f'Archivo CSV seleccionado: {selected_csv}')
                
                # Ejecutar la consulta en Athena sobre la tabla ya creada
                query = f"SELECT * FROM {TABLE_NAME};"  # Ajusta la consulta según tus necesidades
                
                # Ejecutar la consulta en Athena
                query_execution_id = execute_athena_query(query)
                
                # Esperar que la consulta se complete
                wait_for_query_to_complete(query_execution_id)
                
                # Retornar el éxito de la consulta
                return {
                    'statusCode': 200,
                    'body': json.dumps(f'Consulta en Athena ejecutada con éxito para el archivo: {selected_csv}')
                }
            else:
                return {
                    'statusCode': 200,
                    'body': json.dumps('No se encontraron archivos CSV en el bucket.')
                }
        else:
            return {
                'statusCode': 200,
                'body': json.dumps('El bucket está vacío o no contiene los archivos esperados.')
            }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error en la ejecución: {str(e)}')
        }

# Función para ejecutar una consulta en Athena
def execute_athena_query(query):
    try:
        # Ejecutar la consulta en Athena
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': DATABASE},
            ResultConfiguration={'OutputLocation': S3_OUTPUT}  # Especifica el bucket de resultados
        )
        query_execution_id = response['QueryExecutionId']
        print(f'ID de la ejecución de la consulta: {query_execution_id}')
        return query_execution_id
    except Exception as e:
        print(f'Error al ejecutar la consulta en Athena: {str(e)}')
        raise

# Función para esperar que la consulta de Athena se complete
def wait_for_query_to_complete(query_execution_id):
    try:
        status = 'RUNNING'
        while status == 'RUNNING':
            time.sleep(5)  # Esperar 5 segundos antes de verificar el estado nuevamente
            response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = response['QueryExecution']['Status']['State']
            print(f'Estado de la consulta: {status}')
        
        if status == 'SUCCEEDED':
            print('Consulta ejecutada exitosamente en Athena.')
        else:
            raise Exception(f'Error en la ejecución de la consulta: {status}')
    except Exception as e:
        print(f'Error al esperar la ejecución de la consulta: {str(e)}')
        raise
