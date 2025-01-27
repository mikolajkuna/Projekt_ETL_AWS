import boto3
import json
import datetime
import logging

logger = logging.getLogger()
logger.setLevel("INFO")

stream_name = 'brakpary'
bucket = '668704778226-landing-zone'
key = 'REJ_WEWY_PW_DATA_TABLE4.json'

s3_client = boto3.resource('s3')
kinesis_client = boto3.client('kinesis', region_name='us-east-1')


def lambda_handler(event, context):
    logger.info("Lambda invoked")

    obj = s3_client.Object(bucket, key)
    data = obj.get()['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    
    
    pracownicy = {}

    for record in json_data:
        prac_id = record["prac_id"]
        data_czas = record["data_czas"]
        status_id = record["status_id"]
        
        
        if data_czas not in pracownicy.get(prac_id, {}):
            if prac_id not in pracownicy:
                pracownicy[prac_id] = {}
            pracownicy[prac_id][data_czas] = {"wejscie": None, "wyjscie": None}
        
        # Zapisujemy wejscie (status_id == 1) lub wyjscie (status_id == 2) dla danego dnia
        if status_id == 1:
            pracownicy[prac_id][data_czas]["wejscie"] = data_czas
        elif status_id == 2:
            pracownicy[prac_id][data_czas]["wyjscie"] = data_czas
        
        # Sprawdzamy, czy mamy pelna pare (wejscie i wyjscie) w tym samym dniu
        if (pracownicy[prac_id][data_czas]["wejscie"] and 
            pracownicy[prac_id][data_czas]["wyjscie"]):
            # Wysylamy dane do strumienia, jesli mamy pelna pare
            put_to_stream(prac_id, data_czas, 
                          pracownicy[prac_id][data_czas]["wejscie"], 
                          pracownicy[prac_id][data_czas]["wyjscie"])
            # Resetujemy po wykryciu pary
            pracownicy[prac_id][data_czas]["wejscie"] = None
            pracownicy[prac_id][data_czas]["wyjscie"] = None

    # Sprawdzamy, którzy pracownicy nie maja pelnej pary (brak wejscia lub wyjscia)
    for prac_id, dni in pracownicy.items():
        for data_czas, statusy in dni.items():
            if statusy["wejscie"] is not None and statusy["wyjscie"] is None:
                logger.info(f"Pracownik {prac_id} brak wyjscia w dniu {data_czas}")
                put_to_stream(prac_id, data_czas, statusy["wejscie"], "brak wyjscia")
            elif statusy["wejscie"] is None and statusy["wyjscie"] is not None:
                logger.info(f"Pracownik {prac_id} brak wejscia w dniu {data_czas}")
                put_to_stream(prac_id, data_czas, "brak wejscia", statusy["wyjscie"])

    logger.info("Lambda finished")


def put_to_stream(prac_id, data_czas, wejscie, wyjscie):
    payload = {
        "prac_id": prac_id,
        "data_czas": data_czas,
        "wejscie": wejscie,
        "wyjscie": wyjscie,
        "event_time": datetime.datetime.now().isoformat()
    }

    logger.info(payload)

    response = kinesis_client.put_record(
        StreamName=stream_name,
        Data=json.dumps(payload),
        PartitionKey=data_czas)

    logger.info(response)
