package com.github.raffaeleragni.queue_on_sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Events/Queue for competing consumers based on SQL.
 * The queue is not meant to store actual messages or objects. The items of the queue are intended to be small strings (varchar(50)) to use as references. UUIDs are possible within 50 chars.
 *
 * Currently supports look ahead count for collision mitigation and retry with timeouts.
 * Usage:
 * <code><pre>
 *  var config = new SQLEvents.Config("my_queue_table", 10);
 *  queue = new SQLEvents(config, () -> %lt;connection supplier%gt;);
 *  ...
 *  queue.push("item1");
 *  queue.pop().ifPresent(System.out::println);
 * </pre></code>
 * @author Raffaele Ragni
 */
public class SQLEvents {

  /**
   * Configuration:
   * - tableName: name for the SQL table to store the queue internals
   * - retries: how many retries to do if exception occurs
   * - timeout: if a message is stuck, it will be reprocessed within this timeout
   * - lookAhead: how many items to look ahead for collision mitigation, suggested value is N+small% where N is number of consumers
   */
  public record Config(String tableName, int retries, long timeout, int lookAhead) {}

  public static class NullInput extends RuntimeException {}
  public static class SQLError extends RuntimeException {
    SQLError(Throwable cause) {
      super(cause);
    }
  }

  private final Supplier<Connection> conSupplier;

  private final String createQueue;
  private final String selectAvailable;
  private final String insertIntoQueue;
  private final String selectNext;
  private final String takeItem;
  private final String releaseItem;
  private final String deleteItem;
  private final String cleanTimeout;
  private final String deleteRetried;

  public SQLEvents(Config properties, Supplier<Connection> conSupplier) {
    this.conSupplier = conSupplier;
    this.createQueue =
      """
      create table if not exists %s (
        id char(36),
        ref_id varchar(50),
        version bigint,
        last_grabbed_at timestamp,
        grabbed int default 0,
        retried int default 0,
        primary key (id)
      )
      """.formatted(properties.tableName());
    this.selectAvailable =
      """
        select count(*) from %s
      """.formatted(properties.tableName());
    this.insertIntoQueue =
      """
        insert into %s
        (id, ref_id, version, last_grabbed_at)
        values
        (?, ?, 1, null)
      """.formatted(properties.tableName());
    this.selectNext =
      """
        select id, ref_id, version from %s where grabbed = 0 limit %d
      """.formatted(properties.tableName(), properties.lookAhead());
    this.takeItem =
      """
        update %s set version = version+1, grabbed = 1, retried = retried+1, last_grabbed_at = current_timestamp where version = ? and id = ? and grabbed = 0
      """.formatted(properties.tableName());
    this.releaseItem =
      """
        update %s set grabbed = 0 where id = ?
      """.formatted(properties.tableName());
    this.deleteItem =
      """
        delete from %s where id = ?
      """.formatted(properties.tableName());
    this.cleanTimeout =
      """
        update %s set grabbed = 0 where grabbed = 1 and current_timestamp > last_grabbed_at + %d
      """.formatted(properties.tableName(), properties.timeout());
    this.deleteRetried =
      """
        delete from %s where retried > %d
      """.formatted(properties.tableName(), properties.retries());

    statement(createQueue, PreparedStatement::execute);
  }

  public long available() {
    return statement(selectAvailable, st -> {
      try (var rs = st.executeQuery()) {
        rs.next();
        return rs.getLong(1);
      }
    });
  }

  public void push(String value) {
    if (value == null)
      throw new NullInput();
    statement(insertIntoQueue, st -> {
      st.setString(1, UUID.randomUUID().toString());
      st.setString(2, value);
      return st.executeUpdate();
    });
  }

  public Optional<String> pop() {
    return returnUnique();
  }

  public void consumeOne(Consumer<String> consumer) {
    consumeUnique(consumer);
  }

  private Optional<String> consumeUnique(Consumer<String> consumer) {
    return connection(con -> {
      try {
        statement(con, cleanTimeout, PreparedStatement::execute);
        return statement(con, selectNext, st -> lookAheadForItems(st, con, (id, value) -> {
          try {
            consumer.accept(value);
            remove(con, id);
          } catch (RuntimeException ex) {
            release(id);
          }
        }));
      } finally {
        statement(con, deleteRetried, PreparedStatement::execute);
      }
    });
  }

  private Optional<String> returnUnique() {
    return connection(con -> {
      statement(con, cleanTimeout, PreparedStatement::execute);
      return statement(con, selectNext, st -> lookAheadForItems(st, con, (id, value) -> remove(con, id)));
    });
  }

  private Optional<String> lookAheadForItems(PreparedStatement stGet, Connection con, BiConsumer<String, String> consumer) throws SQLException {
    try (var rs = stGet.executeQuery()) {
      while (rs.next()) {
        var id = rs.getString(1);
        var value = rs.getString(2);
        var version = rs.getLong(3);
        var result = take(con, version, id, value, consumer);
        if (result.isPresent())
          return result;
      }
      return Optional.empty();
    }
  }

  private Optional<String> take(Connection con, long version, String id, String value, BiConsumer<String, String> consumer) {
    return statement(con, takeItem, st -> {
      st.setLong(1, version);
      st.setString(2, id);
      if (st.executeUpdate() == 0)
        return Optional.empty();

      consumer.accept(id, value);
      return Optional.of(value);
    });
  }

  private void release(String id) {
    statement(releaseItem, st -> {
      st.setString(1, id);
      return st.executeUpdate();
    });
  }

  private void remove(Connection con, String id) {
    statement(con, deleteItem, st -> {
      st.setString(1, id);
      return st.execute();
    });
  }

  private <T> T statement(String statement, StatementConsumer<T> consumer) {
    return connection(con -> statement(con, statement, consumer));
  }

  private <T> T connection(ConnectionConsumer<T> consumer) {
    try (var con = conSupplier.get()) {
      var prev = con.getAutoCommit();
      con.setAutoCommit(true);
      try {
        return consumer.accept(con);
      } finally {
        con.setAutoCommit(prev);
      }
    } catch (SQLException ex) {
      throw new SQLError(ex);
    }
  }

  private <T> T statement(Connection con, String statement, StatementConsumer<T> consumer) {
    try (var st = con.prepareStatement(statement)) {
      return consumer.accept(st);
    } catch (SQLException ex) {
      throw new SQLError(ex);
    }
  }

  @FunctionalInterface
  private interface ConnectionConsumer<T> {
    T accept(Connection con) throws SQLException;
  }

  @FunctionalInterface
  private interface StatementConsumer<T> {
    T accept(PreparedStatement st) throws SQLException;
  }
}
