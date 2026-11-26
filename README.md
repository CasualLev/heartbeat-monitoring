# Real-Time Customer Heart Beat Monitoring System

A data pipeline that simulates customer heart-rate data, streams it through Apache Kafka,
validates it, and stores it in PostgreSQL.



## Tech Stack
Python · Apache Kafka · PostgreSQL · Docker Compose · Grafana

Kafka runs in **KRaft mode** (no Zookeeper). Zookeeper was removed in Kafka 4.0,
so KRaft is the current standard and simplifies the setup to a single container.

