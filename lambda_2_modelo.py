import boto3
import zipfile
import sys
import pandas as pd
from io import StringIO

# Importar statsmodels
s3_client = boto3.client("s3")
s3_client.download_file(
    "almacenamiento-primario",
    "layers_lambda/statsmodels_package.zip",
    "/tmp/statsmodels_package.zip",
)
with zipfile.ZipFile("/tmp/statsmodels_package.zip", "r") as zip_ref:
    zip_ref.extractall("/tmp/")
sys.path.append("/tmp/python")
import statsmodels.api as sm


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

    # Ajustando index y frecuencia
    df_dynamo.set_index("fecha", inplace=True)
    df_dynamo = df_dynamo.asfreq("b")

    # Modelo
    ibex_model = sm.tsa.ARIMA(df_dynamo["CLOSE_IBEX"].astype(float), order=(1, 1, 4))
    ibex_results = ibex_model.fit()
    sp500_model = sm.tsa.ARIMA(df_dynamo["CLOSE_GSPC"].astype(float), order=(1, 1, 4))
    sp500_results = sp500_model.fit()
    nikei_model = sm.tsa.ARIMA(df_dynamo["CLOSE_N225"].astype(float), order=(1, 1, 3))
    nikei_results = nikei_model.fit()
    forecast_stocks = pd.concat(
        [
            pd.DataFrame(
                {
                    "fecha": ibex_results.get_forecast(steps=5).summary_frame().index,
                    "forectast_ibex_inf": ibex_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean_ci_lower"],
                    "forecast_ibex": ibex_results.get_forecast(steps=5).summary_frame()[
                        "mean"
                    ],
                    "forecast_ibex_sup": ibex_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean_ci_upper"],
                }
            ).reset_index(drop=True),
            pd.DataFrame(
                {
                    "forectast_sp500_inf": sp500_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean_ci_lower"],
                    "forecast_sp500": sp500_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean"],
                    "forecast_sp500_sup": sp500_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean_ci_upper"],
                }
            ).reset_index(drop=True),
            pd.DataFrame(
                {
                    "forectast_nikei_inf": nikei_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean_ci_lower"],
                    "forecast_nikei": nikei_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean"],
                    "forecast_nikei_sup": nikei_results.get_forecast(
                        steps=5
                    ).summary_frame()["mean_ci_upper"],
                }
            ).reset_index(drop=True),
        ],
        axis=1,
    )

    # Convertir el DataFrame a CSV
    csv_buffer = StringIO()
    forecast_stocks.to_csv(csv_buffer, index=False)

    # Subir a S3
    bucket_name = "almacenamiento-primario"
    s3_key = "datos/forecast_stocks.csv"
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    print(f"Archivo subido a S3: s3://{bucket_name}/{s3_key}")
