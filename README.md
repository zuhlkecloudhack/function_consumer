PubSub Cloud function
---------------------

```
virtualenv cloud-function
source cloud-function/bin/activate
pip install -r requirements.txt
```


Deploy function:

```
gcloud beta functions deploy consume_flight_message \
    --source=./  \
    --region=europe-west1 \
    --runtime python37 \
    --trigger-event google.pubsub.topic.publish \
    --trigger-resource flight_messages

```

Publish message into topic:
```
gcloud beta pubsub topics publish flight_messages --message '
{
"flight-number" : "CH5634",
"message" : "Fly me to the moon",
"message-type" : "INFO",
"timestamp" : "2012-04-23T18:25:43.511Z"
}
'
```
