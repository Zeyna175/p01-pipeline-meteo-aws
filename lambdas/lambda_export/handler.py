import csv, io, boto3, os, logging
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from boto3.dynamodb.conditions import Key
logger = logging.getLogger()
logger.setLevel(logging.INFO)
DYNAMO_TABLE = os.environ.get("DYNAMO_TABLE", "meteo-data")
S3_BUCKET = os.environ.get("METEO_BUCKET", "meteo-afrique-ouest")
NB_JOURS = 7
CSV_COLONNES = ["ville","pays","date","heure","temp_min","temp_max","temp_moy",
                "humidite_moy","precipitation","vent_kmh","condition"]
VILLES = ["Dakar","Thies","Saint-Louis","Bamako","Abidjan","Ouagadougou"]
def convertir_decimals(obj):
    return float(obj) if isinstance(obj, Decimal) else obj
def query_ville_date(table, ville, date):
    response = table.query(
        KeyConditionExpression=Key("pk").eq(f"{ville}#{date}"),
        ScanIndexForward=True)
    return response.get("Items", [])
def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    table = boto3.resource("dynamodb").Table(DYNAMO_TABLE)
    s3 = boto3.client("s3")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=CSV_COLONNES,
                            extrasaction="ignore", delimiter=",",
                            quoting=csv.QUOTE_NONNUMERIC)
    writer.writeheader()
    total_lignes = 0
    for i in range(NB_JOURS):
        date = (now - timedelta(days=i)).strftime("%Y-%m-%d")
        for ville in VILLES:
            for item in query_ville_date(table, ville, date):
                writer.writerow({
                    "ville": item.get("ville", ville),
                    "pays": item.get("pays", ""),
                    "date": date, "heure": item.get("sk", ""),
                    "temp_min": convertir_decimals(item.get("temp_min", "")),
                    "temp_max": convertir_decimals(item.get("temp_max", "")),
                    "temp_moy": convertir_decimals(item.get("temp_moy", "")),
                    "humidite_moy": convertir_decimals(item.get("humidite_moy", "")),
                    "precipitation": convertir_decimals(item.get("precipitation", 0)),
                    "vent_kmh": convertir_decimals(item.get("vent_kmh", "")),
                    "condition": item.get("condition", ""),
                })
                total_lignes += 1
    s3_key = f"reports/{now.year}/rapport_hebdo_{now.strftime('%Y-%m-%d')}.csv"
    s3.put_object(Bucket=S3_BUCKET, Key=s3_key,
                  Body=output.getvalue().encode("utf-8"),
                  ContentType="text/csv; charset=utf-8")
    logger.info(f"Rapport genere : {s3_key} — {total_lignes} lignes")
    return {"statusCode": 200, "rapport_s3": f"s3://{S3_BUCKET}/{s3_key}",
            "total_lignes": total_lignes}