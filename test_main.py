import base64
import json

import mock

import main

@mock.patch.object(main, 'publisher')
@mock.patch.object(main, 'storage_client')
@mock.patch.object(main, 'bigquery_client')
def test_consume_flight_message(mock_bigquery_client, mock_storage_client, mock_publisher):

    data = base64.b64encode(json.dumps({
        "flight-number" : "CH5634",
        "message" : "Fly me to the moon",
        "message-type" : "INFO",
        "timestamp" : "2012-04-23T18:25:43.511Z"
        }).encode('utf-8'))

    event = {
        'data': data
    }

    context = {}
    main.consume_flight_message(event, context)