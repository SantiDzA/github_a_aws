import forecast_aws


def lambda_handler(event, context):
    forecast_aws.main()
    return {"statusCode": 200, "body": "Predicción realizada correctamente"}
