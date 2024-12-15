import forecast_aws


def lambda_handler(event, context):
    forecast_aws.main()
    invocar_siguiente()
    return {"statusCode": 200, "body": "Predicción realizada correctamente"}

def invocar_siguiente():
    print("Entrando al invocador")
    client = boto3.client('lambda')
    try:
        client.invoke(
            FunctionName="s3_a_athena",
            InvocationType='Event'
        )
        print(f"Lambda 's3_a_athena' invocada exitosamente.")
    except Exception as e:
        print(f"Error al invocar Lambda 's3_a_athena': {str(e)}")