package com.github.raffaeleragni.queue_on_sql;

import com.github.raffaeleragni.queue_on_sql.SQLEvents.NullInput;
import java.util.Optional;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tests.connection.DatabaseProvider;

class SQLEventsSimpleTest {
  SQLEvents queue;

  @BeforeEach
  void setup()  {
    var config = new SQLEvents.Config("queue", 1);
    queue = new SQLEvents(config, () -> DatabaseProvider.connection(this.getClass().getName()));
  }

  @Test
  void testEmpty() {
    assertThat(queue.available(), is(0L));
  }

  @Test
  void testInsert() {
    queue.push("1");
    assertThat(queue.available(), is(1L));
    queue.push("1");
    assertThat(queue.available(), is(2L));
  }

  @Test
  void testConsumeOne() {
    queue.push("1");
    assertThat(queue.pop(), is(Optional.of("1")));
    assertThat(queue.available(), is(0L));

    queue.push("2");
    assertThat(queue.pop(), is(Optional.of("2")));
    assertThat(queue.available(), is(0L));
  }

  @Test
  void testConsumeInOrder() {
    queue.push("1");
    queue.push("2");
    assertThat(queue.available(), is(2L));

    assertThat(queue.pop(), is(Optional.of("1")));
    assertThat(queue.pop(), is(Optional.of("2")));

    assertThat(queue.available(), is(0L));
  }

  @Test
  void testNoNullInputs() {
    assertThrows(NullInput.class, () -> queue.push(null));
  }

  @Test
  void testPopEmpty() {
    assertThat(queue.pop(), is(Optional.empty()));
  }
}
