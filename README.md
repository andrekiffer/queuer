# Queuer


```
 ./bin/queuer serve \
     --app-console="$(pwd)/bin/queuer" \
     --queue=sqs \
     --prefix-topic="queuer" \
     --prefix-exclude-topic="dead-queue" \
     --queue-config="{\"version\": \"latest\",\"region\": \"sa-east-1\",\"key\": \"$SQS_KEY\",\"secret\": \"$SQS_SECRET\"}" \
     --topic-list-refresh=60 \
     --logger="elk" \
     --logger-config="{\"host\": \"$ES_HOST\", \"port\" : 443, \"transport\" : \"Https\"}"
```

## Message Example:
```
{
	"version": "v1",
	"source": ["app"],
	"command": "test",
	"parameters": "--sleep=5",
	"occurred_at": "2017-01-14T11:11:11-003",
	"payload": {
		"name": "Test",
		"description": "Some description"
	}
}

```

## Supervisor configuration

```
file: /etc/supervisor/queuer.conf

[program:queuer]
command=/app/bin/queuer serve
    --app-console="/app/bin/console"
    --queue=sqs
    --prefix-topic="my-queue-topic"
    --prefix-exclude-topic="dead-queue"
    --queue-config="{\"version\": \"latest\",\"region\": \"sa-east-1\",\"key\": \"$SQS_KEY\",\"secret\": \"$SQS_SECRET\"}"
    --topic-list-refresh=60
    --logger="elk"
    --logger-config="{\"host\": \"$ES_HOST\", \"port\" : 443, \"transport\" : \"Https\"}"
    --max-queue-processes=10
numprocs=1
process_name=queuer
autostart=true
autorestart=true
stderr_logfile=/var/log/queuer.err.log
stdout_logfile=/var/log/queuer.log
stdout_logfile_maxbytes=10MB
```
