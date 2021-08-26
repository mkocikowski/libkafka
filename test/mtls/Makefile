.PHONY: default
default: client-cert.pem client-key.pem kafka.server.keystore.jks kafka.server.truststore.jks

.PHONY: clean
clean:
	rm *.pem *.srl *.p12 *.jks

ca-key.pem ca-cert.pem:
	openssl req -x509 -newkey rsa:4096 -sha256 -days 3650 -nodes -keyout ca-key.pem -out ca-cert.pem -subj "/CN=ca"

client-key.pem client-csr.pem:
	openssl req -new -newkey rsa:4096 -sha256 -days 3650 -nodes -keyout client-key.pem -out client-csr.pem -subj "/CN=client"

client-cert.pem: client-csr.pem ca-cert.pem ca-key.pem extensions.cnf
	openssl x509 -req -in client-csr.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out client-cert.pem -days 3650 -sha256 -extensions v3_ca -extfile extensions.cnf

broker-key.pem broker-csr.pem:
	openssl req -new -newkey rsa:4096 -sha256 -days 3650 -nodes -keyout broker-key.pem -out broker-csr.pem -subj "/CN=broker"

broker-cert.pem: broker-csr.pem ca-cert.pem ca-key.pem extensions.cnf
	openssl x509 -req -in broker-csr.pem -CA ca-cert.pem -CAkey ca-key.pem -CAcreateserial -out broker-cert.pem -days 3650 -sha256 -extensions v3_ca -extfile extensions.cnf

# https://smallstep.com/hello-mtls/doc/server/kafka

broker.p12: broker-cert.pem broker-key.pem
	openssl pkcs12 -export -in broker-cert.pem -inkey broker-key.pem -name localhost -passout pass:123456 >broker.p12

kafka.server.keystore.jks: broker.p12
	keytool -importkeystore -srckeystore broker.p12 -destkeystore kafka.server.keystore.jks -srcstoretype pkcs12 -alias localhost -srcstorepass 123456 -deststorepass 123456 -noprompt

kafka.server.truststore.jks: ca-cert.pem
	keytool -keystore kafka.server.truststore.jks -alias CARoot -import -file ca-cert.pem -storepass 123456 -noprompt
