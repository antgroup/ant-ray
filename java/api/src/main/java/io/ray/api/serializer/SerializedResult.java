package io.ray.api.serializer;

import java.util.ArrayList;
import java.util.List;

public class SerializedResult {

  private List<byte[]> allInBandData = new ArrayList<>();

  public void appendInBandData(byte[] data) {
    allInBandData.add(data);
  }

  public byte[] getInBandData() {
    return null;
  }

}
