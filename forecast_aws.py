import boto3
import zipfile
import sys
import pandas as pd
from io import StringIO
#Importar statsmodels
s3_client = boto3.client('s3')
s3_client.download_file("almacenamiento-primario", 'layers_lambda/statsmodels_package.zip', '/tmp/statsmodels_package.zip')
with zipfile.ZipFile('/tmp/statsmodels_package.zip', 'r') as zip_ref:
    zip_ref.extractall('/tmp/')
sys.path.append('/tmp/python')
#import statsmodels.api as sm
from statsmodels.tsa.arima.model import ARIMA

def main():
    # Conexiones con DynamoDB
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table("my_finance_table")

    # Extracción de datos
    response = table.scan()
    items = response["Items"]
    df_dynamo = pd.DataFrame(items)

    # Arreglo de datos
    df_dynamo["fecha"] = pd.to_datetime(df_dynamo["fecha"])
    df_dynamo.set_index("fecha", inplace=True)
    df_dynamo = df_dynamo.sort_values(by="fecha", ascending=True)

    #FIXME

    # Convertir el DataFrame a CSV
    csv_buffer = StringIO()
    df_dynamo.to_csv(csv_buffer, index=False)

    # Subir a S3
    bucket_name = "almacenamiento-primario"
    s3_key = "datos/forecast_stocks.csv"
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Archivo subido a S3: s3://{bucket_name}/{s3_key}")