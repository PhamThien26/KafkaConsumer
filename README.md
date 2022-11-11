# KafkaConsumer

create topic: ~/kafka/bin/kafka-topics.sh --create --topic TutorialTopic2 --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2

check group-id: ~kafka/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group consumer-group-b --describe
