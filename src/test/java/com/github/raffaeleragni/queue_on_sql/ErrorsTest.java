package com.github.raffaeleragni.queue_on_sql;

import com.github.raffaeleragni.queue_on_sql.SQLEvents.SQLError;
import java.sql.Connection;
import java.sql.SQLException;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import tests.connection.DatabaseProvider;

class ErrorsTest {
  @Test
  void testBadConnection() throws SQLException {
    var config = new SQLEvents.Config("queue", 0, 10, 1);
    var c = mock(Connection.class);
    when(c.getAutoCommit()).thenThrow(SQLException.class);
    when(c.prepareStatement(any())).thenThrow(SQLException.class);
    assertThrows(SQLError.class, () -> new SQLEvents(config, () -> c));
  }

  @Test
  void testBadTableName() {
    var config = new SQLEvents.Config("bad name for queue", 0, 10, 1);
    assertThrows(SQLError.class, () -> new SQLEvents(config, () -> DatabaseProvider.connection(this.getClass().getName())));
  }
}
