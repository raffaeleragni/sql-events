package com.github.raffaeleragni.queue_on_sql;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tests.connection.DatabaseProvider;

class SQLEventsLookAheadTest {
  SQLEvents queue;

  @BeforeEach
  void setup()  {
    var config = new SQLEvents.Config("queue", 0, 10, 10);
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
  void testConsumeWhileMultipleInQueue() {
    queue.push("1");
    queue.push("2");
    queue.push("3");
    queue.push("4");
    queue.push("5");
    queue.push("6");
    queue.push("7");
    queue.push("8");
    queue.push("9");
    assertThat(queue.available(), is(9L));

    var results = new HashSet<String>();
    while (true) {
      var result = queue.pop();
      result.ifPresent(results::add);
      if (result.isEmpty())
        break;
    }
    assertThat(queue.available(), is(0L));
    assertThat(results, is(Set.of("1", "2", "3", "4", "5", "6", "7", "8", "9")));
  }

  @Test
  void testNoNullInputs() {
    assertThrows(SQLEvents.NullInput.class, () -> queue.push(null));
  }

  @Test
  void testPopEmpty() {
    assertThat(queue.pop(), is(Optional.empty()));
  }
}
