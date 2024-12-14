import pandas as pd
import yfinance as yf
import boto3
from io import StringIO
import json

# Cliente para S3
s3 = boto3.client('s3')

# Nombre del bucket de S3 y el nombre del archivo
BUCKET_NAME = 'almacenamiento-primario'
CSV_FILENAME = 'stocks_data.csv'

def lambda_handler(event, context):
    # Datos
    start_time = (pd.Timestamp.today() - pd.DateOffset(years=2)).strftime('%Y-%m-%d')
    end_time = pd.Timestamp.today().strftime('%Y-%m-%d')
    acciones = ['^IBEX','^GSPC','^N225']
    stock_list = []

    # Descargar y procesar los datos
    for i in acciones:
        stock_values = yf.download(i, start=start_time, end=end_time)
        stock_values.columns = stock_values.columns.get_level_values(0)
        stock_values = stock_values.reset_index()
        stock_values = pd.DataFrame({'fecha': stock_values['Date'], 'CLOSE_'+i: stock_values['Close']})
        stock_list.append(stock_values)

    # Fusionar los datos
    final_stock_values = pd.merge(stock_list[0], stock_list[1], how='left', on='fecha')
    final_stock_values = pd.merge(final_stock_values, stock_list[2], how='left', on='fecha')

    # Convertir el DataFrame a CSV
    csv_buffer = StringIO()  # Usamos un buffer en memoria
    final_stock_values.to_csv(csv_buffer, index=False)  # Convertir DataFrame a CSV y guardarlo en el buffer

    #Eliminar el CSV antiguo (nota cambiada!!!!)
    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=CSV_FILENAME)
    except:
        print("No se pudo eliminar el archivo original")
        
    # Subir el CSV al bucket de S3
    s3.put_object(Bucket=BUCKET_NAME, Key=CSV_FILENAME, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps(f'Datos guardados exitosamente en {CSV_FILENAME} dentro del bucket {BUCKET_NAME}')
    }
