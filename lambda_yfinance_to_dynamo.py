import pandas as pd
import yfinance as yf
import boto3
import json
from decimal import Decimal

# Cliente para DynamoDB
dynamodb = boto3.resource('dynamodb')

# Nombre de la tabla de DynamoDB
DYNAMODB_TABLE_NAME = 'my_finance_table'

def lambda_handler(event, context):
    # Datos
    start_time = (pd.Timestamp.today() - pd.DateOffset(years=1)).strftime('%Y-%m-%d')
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

    # Conectar a la tabla de DynamoDB
    table = dynamodb.Table(DYNAMODB_TABLE_NAME)

    # Eliminar los datos previos de la tabla basados en la fecha
    for index, row in final_stock_values.iterrows():
        fecha = str(row['fecha'])[:10]  # Convertir la fecha a formato string
        table.delete_item(
            Key={
                'fecha': fecha
            }
        )

    # Guardar cada fila del DataFrame en DynamoDB
    for index, row in final_stock_values.iterrows():
        # Convertir los valores a tipos compatibles con DynamoDB (Decimal para números)
        item = {
            'fecha': str(row['fecha'])[:10],  # Asegurarse de que la fecha esté en formato de cadena
            'CLOSE_IBEX': Decimal(str(row['CLOSE_^IBEX'])) if pd.notna(row['CLOSE_^IBEX']) else None,
            'CLOSE_GSPC': Decimal(str(row['CLOSE_^GSPC'])) if pd.notna(row['CLOSE_^GSPC']) else None,
            'CLOSE_N225': Decimal(str(row['CLOSE_^N225'])) if pd.notna(row['CLOSE_^N225']) else None
        }

        # Filtrar None antes de insertar en DynamoDB
        item = {k: v for k, v in item.items() if v is not None}

        # Insertar cada fila en la tabla
        table.put_item(Item=item)

    invocar_siguiente()
    
    return {
        'statusCode': 200,
        'body': json.dumps(f'Datos guardados exitosamente en la tabla {DYNAMODB_TABLE_NAME}')
    }

def invocar_siguiente():
    print("Entrando al invocador")
    client = boto3.client('lambda')
    try:
        client.invoke(
            FunctionName="ejecutar_notebook",
            InvocationType='Event'
        )
        print(f"Lambda 'ejecutar_notebook' invocada exitosamente.")
    except Exception as e:
        print(f"Error al invocar Lambda 'ejecutar_notebook': {str(e)}")