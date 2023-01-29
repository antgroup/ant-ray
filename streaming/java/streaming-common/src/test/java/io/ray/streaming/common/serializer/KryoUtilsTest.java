package io.ray.streaming.common.serializer;

import com.esotericsoftware.kryo.Serializer;
import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class KryoUtilsTest {

  private static final Logger LOG = LoggerFactory.getLogger(KryoUtilsTest.class);
  public static final int LOOP_COUNT = 10000;

  private Tuple t = createTuple();

  class Tuple {

    Long a;
    List<InnerLabelRecord> b;

    public Tuple(Long a, List<InnerLabelRecord> b) {
      this.a = a;
      this.b = b;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Tuple tuple = (Tuple) o;
      return Objects.equal(a, tuple.a) && Objects.equal(b, tuple.b);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(a, b);
    }
  }

  static class Record {

    public int v;

    public Record(int v) {
      this.v = v;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Record record = (Record) o;
      return v == record.v;
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(v);
    }
  }

  static class InnerLabelRecord implements Serializable {

    public Record record;
    public Long triggerTime;

    public InnerLabelRecord(Record record, Long triggerTime) {
      this.record = record;
      this.triggerTime = triggerTime;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      InnerLabelRecord that = (InnerLabelRecord) o;
      return Objects.equal(record, that.record) && Objects.equal(triggerTime, that.triggerTime);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(record, triggerTime);
    }
  }

  @Test
  public void testSerde() {
    Tuple t = createTuple();
    byte[] bytes = KryoUtils.writeToByteArray(t);
    Assert.assertEquals(KryoUtils.readFromByteArray(bytes), t);
  }

  private Tuple createTuple() {
    // use ArrayList will raise java.util.ConcurrentModificationException
    Tuple t = new Tuple(1L, new CopyOnWriteArrayList<>());
    t.b.add(new InnerLabelRecord(new Record(1), System.currentTimeMillis()));
    t.b.add(new InnerLabelRecord(new Record(2), System.currentTimeMillis()));
    t.b.add(new InnerLabelRecord(new Record(2), null));
    t.b.add(new InnerLabelRecord(null, null));
    return t;
  }

  private void modifyTuple() {
    if (t.b.size() > 2) {
      t.b.remove(0);
      return;
    }
    t.b.add(new InnerLabelRecord(null, System.currentTimeMillis()));
  }

  private void runSer() {
    Tuple copied = new Tuple(t.a, t.b);
    Assert.assertTrue(copied.b.size() > 0);
    byte[] bytes = KryoUtils.writeToByteArray(copied);
    Assert.assertTrue(bytes.length > 0);
  }

  private Thread newSerThread() {
    return new Thread(
        new Runnable() {
          @Override
          public void run() {
            int i = 0;
            while (i < LOOP_COUNT) {
              runSer();
              i++;

              if (i % 100 == 0) {
                modifyTuple();
              }
            }
          }
        });
  }

  private Thread newModifyThread() {
    return new Thread(
        new Runnable() {
          @Override
          public void run() {
            int i = 0;
            while (i < LOOP_COUNT) {
              modifyTuple();
              i++;
            }
          }
        });
  }

  @Test
  public void testMultiThreadsSerde() throws Exception {

    Thread t1 = newSerThread();
    t1.start();

    Thread t3 = newModifyThread();
    t3.start();

    Thread t2 = newSerThread();
    t2.start();

    t1.join();
    t2.join();
    t3.join();

    // No exception occurred.
    Assert.assertEquals(1, 1);
  }

  @Test
  public void testLambdaInKryo() {
    Runnable r = (Runnable & Serializable) () -> LOG.info("works");
    byte[] encoded = KryoUtils.writeToByteArray(r);
    Object decoded = KryoUtils.readFromByteArray(encoded);
    Assert.assertNotNull(decoded);
  }

  @Test
  public void testArrayBlockingQueue() throws Exception {

    ArrayBlockingQueue<String> arrayBlockingQueue = new ArrayBlockingQueue<String>(8);
    arrayBlockingQueue.add("test1");
    arrayBlockingQueue.add("test2");
    byte[] bytes = KryoUtils.writeToByteArray(arrayBlockingQueue);
    ArrayBlockingQueue<String> serialieQueue = KryoUtils.readFromByteArray(bytes);
    Assert.assertEquals(serialieQueue.size(), 2);
  }

  @Test
  public void testArrayBlockingQueue1() throws Exception {
    ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(4);
    q.put("a");
    q.put("b");
    q.put("c");

    Assert.assertEquals(q.remainingCapacity(), 1);

    byte[] encoded = KryoUtils.writeToByteArray(q);

    ArrayBlockingQueue<String> q1 = KryoUtils.readFromByteArray(encoded);

    Assert.assertEquals(q1.remainingCapacity(), 1);
  }

  @Test
  public void testLinkedBlockingQueue() throws Exception {
    LinkedBlockingQueue<String> q = new LinkedBlockingQueue<>(4);
    q.put("a");
    q.put("b");
    q.put("c");

    Assert.assertEquals(q.remainingCapacity(), 1);

    byte[] encoded = KryoUtils.writeToByteArray(q);

    LinkedBlockingQueue<String> q1 = KryoUtils.readFromByteArray(encoded);

    Assert.assertEquals(q1.remainingCapacity(), 1);
  }

  enum A {
    S1,
    S2
  }

  @Test
  public void testEnum() {
    Object o = KryoUtils.readFromByteArray(KryoUtils.writeToByteArray(A.S1));
    Assert.assertSame(o, A.S1);
    Serializer serializer = KryoUtils.getInstance().getSerializer(A.class);
    Assert.assertTrue(serializer instanceof KryoUtils.EnumSerializer);
  }
}
