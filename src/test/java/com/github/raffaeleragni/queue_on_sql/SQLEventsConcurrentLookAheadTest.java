package com.github.raffaeleragni.queue_on_sql;

import java.util.Collections;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import static java.util.stream.Collectors.toSet;
import java.util.stream.IntStream;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import tests.connection.DatabaseProvider;

class SQLEventsConcurrentLookAheadTest {
  SQLEvents queue;
  ExecutorService executor;

  private static int THREADS = 10;
  private static int LOOK_AHEAD = THREADS + 5;
  private static Set<String> INPUTS = IntStream.range(0, THREADS * LOOK_AHEAD * 10)
      .boxed()
      .map(String::valueOf)
      .collect(toSet());

  @BeforeEach
  void setup()  {
    executor = Executors.newFixedThreadPool(THREADS);
    var config = new SQLEvents.Config("queue", 0, 10, LOOK_AHEAD);
    queue = new SQLEvents(config, () -> DatabaseProvider.connection(this.getClass().getName()));
  }

  @Test
  void testConsumeConcurrentlyManyItems() throws Exception {
    for (var item: INPUTS)
      queue.push(item);

    assertThat(queue.available(), is(Long.valueOf(INPUTS.size())));

    var results = Collections.synchronizedList(new LinkedList<String>());
    Runnable consumerTask = () -> {
      while (true) {
        var result = queue.pop();
        result.ifPresent(results::add);
        if (result.isEmpty())
          break;
      }
    };

    var tasks = new LinkedList<Future<?>>();

    for (var i = 0; i < THREADS; i++)
      tasks.add(executor.submit(consumerTask));

    for (var task: tasks)
      task.get();

    assertThat(queue.available(), is(0L));

    assertThat(results.size(), is(INPUTS.size()));
    assertThat(Set.copyOf(results), is(INPUTS));
  }
}
