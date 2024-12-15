import boto3
from time import sleep

client = boto3.client("athena")
s3 = boto3.resource("s3")

def lambda_handler(event, context):
    tabla = "datos"
    base_datos = "bd_prueba"
    nom_s3 = "almacenamiento-primario"
    carpeta = "query_results/"
    destino = "s3://" + nom_s3 + "/" + carpeta
    # El primer elemento es la consulta a realizar
    # El segundo el nombre del archivo para guardar la consulta
    lista_queries = [
        (f"SELECT * FROM {tabla} limit 10;", "prueba")
    ]
    for query in lista_queries:
        queryStart = client.start_query_execution(
            QueryString = query[0],
            QueryExecutionContext = {
                "Database": base_datos
            },
            ResultConfiguration = {
                "OutputLocation": destino
            }
        )
        query_id = queryStart["QueryExecutionId"]
        dormir_hasta_fin_consulta(query_id)
        
        # Dejamos un archivo CSV con nombre más legible
        dir_csv = f"{carpeta}{query_id}.csv"
        abs_dir_csv = nom_s3 + "/" + dir_csv
        nuevo_csv = f"{carpeta}{query[1]}.csv"
        s3.Object(nom_s3, nuevo_csv).copy_from(CopySource = abs_dir_csv)
        s3.Object(nom_s3, dir_csv).delete()
        s3.Object(nom_s3, dir_csv + ".metadata").delete()
    return {
        'statusCode': 200,
        'body': "Datos guardados con éxito"
    }
# Función para esperar que la consulta de Athena se complete
def dormir_hasta_fin_consulta(ident):
    while True:
        rs = client.get_query_execution(QueryExecutionId=ident)
        state = rs['QueryExecution']['Status']['State']
        if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        sleep(1) 