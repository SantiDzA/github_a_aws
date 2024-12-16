import boto3
from time import sleep

ath = boto3.client("athena")
s3 = boto3.resource("s3")
glue = boto3.client("glue")

def lambda_handler(event, context):
    # Arrancar el Crawler (primero nos aseguramos de que esté parado)
    crawler = "crawler-datos"
    parar_crawler(crawler)
    dormir_crawler(crawler)
    crawler_start = glue.start_crawler(Name=crawler)
    dormir_crawler(crawler)

    # Consultas con Athena
    tabla = "datos"
    base_datos = "base_athena"
    nom_s3 = "almacenamiento-primario"
    carpeta = "query_results/"
    destino = "s3://" + nom_s3 + "/" + carpeta
    # El primer elemento es la consulta a realizar
    # El segundo el nombre del archivo para guardar la consulta
    lista_queries = [
        (f"SELECT * FROM {tabla} limit 10;", "general"),
        (f"""SELECT 'forecast_ibex_inf' AS index_name, fecha AS date, 
            forecast_ibex_inf AS value FROM {tabla}
            WHERE forecast_ibex_inf = (SELECT MIN(forecast_ibex_inf) FROM {tabla})
            UNION ALL SELECT 'forecast_sp500_inf' AS index_name, fecha AS {tabla},
            forecast_sp500_inf AS value FROM {tabla}
            WHERE forecast_sp500_inf = (SELECT MIN(forecast_sp500_inf) FROM {tabla})
            UNION ALL SELECT 'forecast_nikei_inf' AS index_name, fecha AS date,
            forecast_nikei_inf AS value FROM {tabla}
            WHERE forecast_nikei_inf = (SELECT MIN(forecast_nikei_inf) FROM {tabla});""", "minimos"),
        (f"""SELECT 'forecast_ibex_sup' AS index_name, fecha AS date, 
            forecast_ibex_sup AS value FROM {tabla}
            WHERE forecast_ibex_sup = (SELECT MAX(forecast_ibex_sup) FROM {tabla})
            UNION ALL SELECT 'forecast_sp500_sup' AS index_name, fecha AS date,
            forecast_sp500_sup AS value FROM {tabla}
            WHERE forecast_sp500_sup = (SELECT MAX(forecast_sp500_sup) FROM {tabla})
            UNION ALL SELECT 'forecast_nikei_sup' AS index_name, fecha AS date,
            forecast_nikei_sup AS value FROM {tabla}
            WHERE forecast_nikei_sup = (SELECT MAX(forecast_nikei_sup) FROM {tabla});""", "maximos")
    ]
    for query in lista_queries:
        queryStart = ath.start_query_execution(
            QueryString = query[0],
            QueryExecutionContext = {
                "Database": base_datos
            },
            ResultConfiguration = {
                "OutputLocation": destino
            }
        )
        query_id = queryStart["QueryExecutionId"]
        dormir_athena(query_id)
        
        # Dejamos un archivo CSV con nombre más legible
        dir_csv = f"{carpeta}{query_id}.csv"
        abs_dir_csv = nom_s3 + "/" + dir_csv
        nuevo_csv = f"{carpeta}{query[1]}.csv"
        s3.Object(nom_s3, nuevo_csv).copy_from(CopySource = abs_dir_csv)
        s3.Object(nom_s3, nuevo_csv).Acl().put(ACL='public-read')
        s3.Object(nom_s3, dir_csv).delete()
        s3.Object(nom_s3, dir_csv + ".metadata").delete()
    return {
        'statusCode': 200,
        'body': "Datos guardados con éxito"
    }

# Parar el Crawler en caso de que esté funcionando
def parar_crawler(name):
    rs = glue.get_crawler(Name=name)
    state = rs['Crawler']['State']
    if state == "RUNNING":
        glue.stop_crawler(Name=name)
    sleep(1) 

# Función para esperar a que el Crawler acabe
def dormir_crawler(name):
    while True:
        rs = glue.get_crawler(Name=name)
        state = rs['Crawler']['State']
        if state == "READY":
            break
        sleep(1) 

# Función para esperar que la consulta de Athena se complete
def dormir_athena(ident):
    while True:
        rs = ath.get_query_execution(QueryExecutionId=ident)
        state = rs['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        sleep(1) 