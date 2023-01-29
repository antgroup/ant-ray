package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.EventSeverity;
import io.ray.api.Ray;
import io.ray.api.id.UniqueId;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

// test log reporter, first ray event API would write
// the event log file, then we search the specific event line with the label and
// message, these events located in the different files, should be sorted
// we sort them by the label(we use PIPELINE_1, PIPELINE_2... and so on)
// then we will test the correctness of this message

public class EventTest extends BaseTest {

  public static class Counter {
    private int value;

    public Counter() {
      this.value = 0;
    }

    public int read() {
      return value;
    }

    public void increment() {
      Ray.reportEvent(
          EventSeverity.INFO, "PIPELINE_2", "running increment function in the Counter Actor");
      value += 1;
    }

    public void incrementInPrivateThread() throws Exception {
      Thread thread =
          new Thread(
              Ray.wrapRunnable(
                  () -> {
                    Ray.reportEvent(
                        EventSeverity.INFO,
                        "PIPELINE_2",
                        "running increment function in the Counter Actor in private thread");
                  }));
      thread.start();
      Ray.reportEvent(
          EventSeverity.INFO, "PIPELINE_1", "running increment function in the Counter Actor");
      value += 1;
      thread.join();
    }
  }

  /* use specific label and message to search event in log file */
  public List<String> searchSpecificEvent(List<Map.Entry<String, String>> searchList) {
    List<String> eventList = new ArrayList<>();
    List<File> fileList =
        (List<File>)
            FileUtils.listFiles(
                new File(TestUtils.getRuntime().getRayConfig().logDir, "events"), null, false);
    String encoding = "GBK";
    fileList.stream()
        .forEach(
            fileName -> {
              if (fileName.toString().contains("CORE_WORKER")) {
                File file = new File(String.valueOf(fileName));
                Preconditions.checkState(file.isFile() && file.exists());
                try {
                  InputStreamReader read = null;
                  read = new InputStreamReader(new FileInputStream(file), encoding);

                  BufferedReader bufferedReader = new BufferedReader(read);
                  String lineTxt = null;

                  while ((lineTxt = bufferedReader.readLine()) != null) {
                    boolean ok = false;
                    for (int i = 0, len = searchList.size(); i < len; ++i) {
                      if (lineTxt.contains(searchList.get(i).getKey())
                          && lineTxt.contains(searchList.get(i).getValue())) {
                        ok = true;
                        break;
                      }
                    }
                    if (ok) {
                      eventList.add(lineTxt);
                    }
                  }
                  bufferedReader.close();
                  read.close();
                } catch (Exception e) {
                  throw new RuntimeException(e);
                }
              }
            });
    return eventList;
  }

  @Test(groups = {"cluster"})
  public void testEventFile() {
    List<Map.Entry<String, String>> searchList = new ArrayList<>();
    String message = "start ray worker with unique ID: " + UniqueId.randomId().toString();
    Ray.reportEvent(
        EventSeverity.INFO, "PIPELINE_1", "test for breakline\n\ntest for breakline again\n");
    Ray.reportEvent(EventSeverity.INFO, "PIPELINE_1", message);
    searchList.add(new AbstractMap.SimpleEntry<>("PIPELINE_1", message));

    // Test creating an actor from a constructor
    List<ActorHandle<EventTest.Counter>> actorList = new ArrayList<>();

    for (int i = 0; i < 4; ++i) {
      actorList.add(Ray.actor(EventTest.Counter::new).remote());
    }

    for (int i = 0; i < 4; ++i) {
      actorList.get(i).task(EventTest.Counter::increment).remote();
    }

    for (int i = 0; i < 4; ++i) {
      Assert.assertEquals(
          Integer.valueOf(1), actorList.get(i).task(EventTest.Counter::read).remote().get());
    }

    searchList.add(
        new AbstractMap.SimpleEntry<>(
            "PIPELINE_2", "running increment function in the Counter Actor"));

    message = "End ray worker with unique ID: " + UniqueId.randomId().toString();
    Ray.reportEvent(EventSeverity.INFO, "PIPELINE_3", message);
    searchList.add(new AbstractMap.SimpleEntry<>("PIPELINE_3", message));

    List<String> eventList = searchSpecificEvent(searchList);

    Assert.assertEquals(eventList.size(), 6);
  }

  @Test(groups = {"cluster"})
  public void testEventFileInPrivateThread() {
    List<Map.Entry<String, String>> searchList = new ArrayList<>();

    ActorHandle<EventTest.Counter> counter = Ray.actor(EventTest.Counter::new).remote();

    counter.task(EventTest.Counter::incrementInPrivateThread).remote();
    counter.task(EventTest.Counter::read).remote().get();

    searchList.add(
        new AbstractMap.SimpleEntry<>(
            "PIPELINE_1", "running increment function in the Counter Actor"));
    searchList.add(
        new AbstractMap.SimpleEntry<>(
            "PIPELINE_2", "running increment function in the Counter Actor in private thread"));

    List<String> eventList = searchSpecificEvent(searchList);

    Assert.assertEquals(eventList.size(), 2);
  }
}
