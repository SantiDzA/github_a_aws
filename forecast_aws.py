import pandas as pd
from pmdarima import auto_arima
from io import StringIO

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

    df_dynamo = df_dynamo.sort_values(by="fecha", ascending=True).reset_index(drop=True)
    df_dynamo = df_dynamo[["fecha", "CLOSE_GSPC", "CLOSE_IBEX", "CLOSE_N225"]]

    # Ajustando el index
    df_dynamo.set_index("fecha", inplace=True)

    # Ajustando la frecuencia
    df_dynamo = df_dynamo.asfreq("b")

    # Valores faltantes
    df_dynamo["CLOSE_IBEX"] = df_dynamo["CLOSE_IBEX"].fillna(method="ffill")
    df_dynamo["CLOSE_GSPC"] = df_dynamo["CLOSE_GSPC"].fillna(method="ffill")
    df_dynamo["CLOSE_N225"] = df_dynamo["CLOSE_N225"].fillna(method="ffill")


    # Modelo
    modelo_ibex = auto_arima(df_dynamo["CLOSE_IBEX"])
    modelo_sp500 = auto_arima(df_dynamo["CLOSE_GSPC"])
    modelo_nikkei = auto_arima(df_dynamo["CLOSE_N225"])


    # Predicciones
    forecast_stocks = pd.concat(
        [
            pd.DataFrame(
                {
                    "fecha": modelo_ibex.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[0].index,
                    "forectast_ibex_inf": modelo_ibex.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[1][:, 0],
                    "forecast_ibex": modelo_ibex.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[0].values,
                    "forecast_ibex_sup": modelo_ibex.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[1][:, 1],
                }
            ),
            pd.DataFrame(
                {
                    "forectast_nikkei_inf": modelo_nikkei.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[1][:, 0],
                    "forecast_nikkei": modelo_nikkei.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[0].values,
                    "forecast_nikkei_sup": modelo_nikkei.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[1][:, 1],
                }
            ),
            pd.DataFrame(
                {
                    "forectast_nikkei_inf": modelo_nikkei.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[1][:, 0],
                    "forecast_nikkei": modelo_nikkei.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[0].values,
                    "forecast_nikkei_sup": modelo_nikkei.predict(
                        n_periods=5, return_conf_int=True, alpha=0.05
                    )[1][:, 1],
                }
            ),
        ],
        axis=1,
    )

    df_dynamo.reset_index(inplace=True)

    # Configurar cliente S3
    s3 = boto3.client("s3")

    # Convertir el DataFrame a CSV
    csv_buffer = StringIO()
    forecast_stocks.to_csv(csv_buffer, index=False)

    # Subir a S3
    bucket_name = "exit-bucket-sae"  # Reemplaza con el nombre de tu bucket
    s3_key = "forecast_stocks.csv"  # Ruta donde se guardará el archivo en S3
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Archivo subido a S3: s3://{bucket_name}/{s3_key}")
