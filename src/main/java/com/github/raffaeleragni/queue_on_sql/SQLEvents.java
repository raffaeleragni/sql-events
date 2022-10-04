package com.github.raffaeleragni.queue_on_sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Supplier;

/**
 * Events/Queue for competing consumers based on SQL.
 * The queue is not meant to store actual messages or objects. The items of the queue are intended to be small strings (varchar(50)) to use as references. UUIDs are possible within 50 chars.
 *
 * Currently supports look ahead count for collision mitigation, but not retry logic.
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

  private final Supplier<Connection> conSupplier;

  private final String createQueue;
  private final String selectAvailable;
  private final String insertIntoQueue;
  private final String selectNext;
  private final String updateOptimisticLock;
  private final String deleteItem;

  /**
   * Configuration:
   * - tableName: name for the SQL table to store the queue internals
   * - lookAhead: how many items to look ahead for collision mitigation, suggested value is N+small% where N is number of consumers
   */
  public record Config(String tableName, int lookAhead) {}

  public static class NullInput extends RuntimeException {}
  public static class SQLError extends RuntimeException {
    SQLError(Throwable cause) {
      super(cause);
    }
  }

  public SQLEvents(Config properties, Supplier<Connection> conSupplier) {
    this.conSupplier = conSupplier;
    this.createQueue =
      """
      create table if not exists %s (
        id char(36),
        ref_id varchar(50),
        version bigint,
        last_grabbed_at timestamp,
        processed int default 0,
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
        select id, ref_id, version from %s limit %d
      """.formatted(properties.tableName(), properties.lookAhead());
    this.updateOptimisticLock =
      """
        update %s set version = ?, processed = 1, last_grabbed_at = current_timestamp where version = ? and id = ? and processed = 0
      """.formatted(properties.tableName());
    this.deleteItem =
      """
        delete from %s where id = ?
      """.formatted(properties.tableName());

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
    return ensureUniquePop();
  }

  private Optional<String> ensureUniquePop() {
    return connection(con ->
      statement(con, selectNext, st -> lookAheadForItems(st, con))
    );
  }

  private Optional<String> lookAheadForItems(PreparedStatement stGet, Connection con) throws SQLException {
    try (var rs = stGet.executeQuery()) {
      while (rs.next()) {
        var id = rs.getString(1);
        var value = rs.getString(2);
        var version = rs.getLong(3);
        var result = optimisticTakeItem(con, version, id, value);
        if (result.isPresent())
          return result;
      }
      return Optional.empty();
    }
  }

  private Optional<String> optimisticTakeItem(Connection con, long version, String id, String value) {
    return statement(con, updateOptimisticLock, st -> {
      st.setLong(1, version+1);
      st.setLong(2, version);
      st.setString(3, id);
      if (st.executeUpdate() == 0)
        return Optional.empty();

      deleteItem(con, id);
      return Optional.of(value);
    });
  }

  private void deleteItem(Connection con, String id) {
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
