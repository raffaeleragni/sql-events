# SQLEvents: competing consumers in SQL

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=raffaeleragni_sql-events&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=raffaeleragni_sql-events)

How to import:

```maven
<dependency>
  <groupId>com.github.raffaeleragni</groupId>
  <artifactId>db-events</artifactId>
  <version>1.0</version>
</dependency>
```

How to setup:

```java
  // have a connection provider
  var connectionProvider = Supplier<Connection>
  // Config: queue name, retry count, timeout, look ahead count
  var config = new SQLEvents.Config("queue_events", 1, 10, 3);
  var events = new SQLEvents(config, connectionProvider);
```

How to use:

```java
  // Add one element: the value is a small string (varchar 50) used for referencing
  events.push("ref_id_aaa");
  // Consume one element
  events.consumeOne(value -> {...});
```
