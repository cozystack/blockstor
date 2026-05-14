# day2-tls-rest-api

## Scenario

Enable HTTPS on the LINSTOR REST API (port 3371) and optionally restrict by client certificate.

## Steps

1. Generate a server keystore (Java keystore .jks) for the controller: `keytool -keyalg rsa -keysize 2048 -genkey -keystore keystore_linstor.jks ...`.
2. Update `/etc/linstor/linstor.toml`:
```
[https]
  keystore = "/path/to/keystore_linstor.jks"
  keystore_password = "linstor"
```
3. Restart the controller. HTTPS REST API listens on 3371.
4. (Optional) For mTLS client cert restriction, generate client keystore, import to controller truststore, add `truststore` / `truststore_password` to `[https]`.
5. From the client, use `--certfile client.pem`: `linstor --certfile client1.pem node list`.

## Expected outcome

- HTTP requests to v1 are redirected to HTTPS.
- mTLS (if configured) rejects clients without a trusted certificate.

## Validations

- `curl -k https://controller:3371/v1/nodes` returns a JSON list of nodes.
- `curl http://controller:3370/v1/nodes` returns a 30x redirect to HTTPS.
- Without `--certfile`, a client gets an authentication error if mTLS is required.

## Doc reference

linstor-administration.adoc: `==== LINSTOR REST API HTTPS` (lines 3811-3848) and `===== LINSTOR REST API HTTPS restricted client access` (lines 3849-3915).

## Notes

- The PEM the client needs is produced by converting the JKS via openssl (`openssl pkcs12 -in client.p12 -out client.pem`).
- After enabling HTTPS, ALL v1 HTTP calls redirect; ensure all automation uses HTTPS.
- For k8s Operator v2 the API TLS recipe uses cert-manager - see linstor-kubernetes.adoc lines 382-583.
