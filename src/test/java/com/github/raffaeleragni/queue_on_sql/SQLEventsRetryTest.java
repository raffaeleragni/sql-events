package com.github.raffaeleragni.queue_on_sql;

import java.util.function.Consumer;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tests.connection.DatabaseProvider;

class SQLEventsRetryTest {
  SQLEvents queue;
  Consumer<String> exceptionRetrier = value -> { throw new RuntimeException(); };

  @BeforeEach
  void setup()  {
    var config = new SQLEvents.Config("queue", 5, 10, 1);
    queue = new SQLEvents(config, () -> DatabaseProvider.connection(this.getClass().getName()));
  }

  @Test
  void testRetryThenOK() {
    queue.push("1");
    assertThat(queue.available(), is(1L));

    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(1L));

    queue.consumeOne(value -> assertThat(value, is("1")));
    assertThat(queue.available(), is(0L));
  }

  @Test
  void testRetryExaustion() throws Exception {
    queue.push("1");
    assertThat(queue.available(), is(1L));

    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(1L));
    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(1L));
    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(1L));
    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(1L));
    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(1L));

    queue.consumeOne(exceptionRetrier);
    assertThat(queue.available(), is(0L));
  }

}
