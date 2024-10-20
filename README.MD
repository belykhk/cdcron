# Expected environment variables

## Health check related
1. `HEALTHCHECK_HOSTNAME` : ip \ fqdn of webserver for consul client to connect to check health of service, default `host.docker.internal`
1. `HEALTHCHECK_PORT` : tcp port to run stub webserver on, default `8080`

## Consul connection related
1. `CONSUL_SCHEME` : http or https, default `http`
1. `CONSUL_HOSTNAME` : ip\fqdn of consul client, default `localhost`
1. `CONSUL_PORT` : port of consul client to connect to, default `8500`
1. `CONSUL_TOKEN` : application token if acl is enabled, default is empty
1. `SERVICE_NAME` : name of service, default `cdcron`
1. `SERVICE_ID` : id of service, default is random ASCII uppercase or digits 5 char string

# State machine of consul leader election
![Image of state machine](consul-state-machine.png)
