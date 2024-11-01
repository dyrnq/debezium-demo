# debezium-demo


- [debezium-embedded/src/test/java/io/debezium/embedded/EmbeddedEngineTest.java](https://github.com/debezium/debezium/blob/main/debezium-embedded/src/test/java/io/debezium/embedded/EmbeddedEngineTest.java)


```sql
-- CREATE USER canal IDENTIFIED BY 'canal';
CREATE USER canal IDENTIFIED WITH mysql_native_password BY 'canal';
GRANT SELECT, LOCK TABLES, RELOAD, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'canal'@'%';
-- GRANT ALL PRIVILEGES ON *.* TO 'canal'@'%' ;
FLUSH PRIVILEGES;
```