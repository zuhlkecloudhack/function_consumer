import base64
import datetime
import logging
import json
import time
from tempfile import NamedTemporaryFile

from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.bigquery.schema import SchemaField

with open('config.json') as f:
    data = f.read()
config = json.loads(data)

schema = [SchemaField('flight', 'STRING'),
          SchemaField('total_no_of_words', 'INTEGER'),
          SchemaField('processing_time', 'STRING')]

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(config["PROJECT"],
                                  config["PUBSUB_TOPIC"])

storage_client = storage.Client(config["PROJECT"],)
bucket = storage_client.get_bucket(config["BUCKET"])

bigquery_client = bigquery.Client(config["PROJECT"],)
table_ref = bigquery_client.dataset(config["DATASET"]).table(config["TABLE"])
table = bigquery.Table(table_ref, schema=schema)


def consume_flight_message(data, context):
    if 'data' in data:
        payload = base64.b64decode(data['data']).decode('utf-8')

        row = json.loads(payload)
        event_dt = datetime.datetime.strptime(row["timestamp"][:-5], '%Y-%m-%dT%H:%M:%S')
        row["timestamp"] = time.mktime(event_dt.timetuple())
        row["no-of-words"] = len(row["message"].split(" "))

        logging.info(row)

        _publish(row)
        _write_to_gcs(row)
        _write_to_bq(row)


def _write_to_bq(row):
    now = datetime.datetime.now()
    data = {
        "flight": row["flight-number"],
        "total_no_of_words": row["no-of-words"],
        "processing_time": now.strftime('%Y-%m-%dT%H:%M:%S')
    }

    errors = bigquery_client.insert_rows(table, [data])
    assert errors == []


def _write_to_gcs(row):
    with NamedTemporaryFile() as tmp_file:
        with open(tmp_file.name, "w") as fp:
            json.dump(row, fp)

        now = datetime.datetime.utcnow()

        dest_path = "raw/{}/{}.json".format(
            now.strftime("%Y-%m-%d"),
            now.strftime("%H-%M-%S.%f"),
        )
        blob = bucket.blob(dest_path)
        blob.upload_from_filename(tmp_file.name)


def _publish(row):
    data = json.dumps(row).encode('utf-8')
    publisher.publish(topic_path, data=data)