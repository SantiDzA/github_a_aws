import lambda_2_modelo
import boto3

def lambda_handler(event, context):
    lambda_2_modelo.main()
    invocar_siguiente()
    return {"statusCode": 200, "body": "Predicción realizada correctamente"}

def invocar_siguiente():
    print("Entrando al invocador")
    client = boto3.client('lambda')
    try:
        client.invoke(
            FunctionName="lambda_3",
            InvocationType='Event'
        )
        print(f"Lambda 'lambda_3' invocada exitosamente.")
    except Exception as e:
        print(f"Error al invocar Lambda 'lambda_3': {str(e)}")