# Flink plus the Kafka SQL connector.
#
# The official image ships without connectors. Any jar placed in /opt/flink/lib
# is loaded when Flink starts. The connector version has two parts:
# <connector version>-<Flink version it was built for>, so they must match.

FROM flink:2.2.1-scala_2.12-java17

ADD --chmod=644 \
  https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar \
  /opt/flink/lib/