package io.ray.api;

import java.util.HashMap;
import java.util.Map;

public class UpdateJobTotalResourcesRequest {
  private Map<String, Double> minResourceRequirements = new HashMap<>();
  private Map<String, Double> maxResourceRequirements = new HashMap<>();

  private UpdateJobTotalResourcesRequest(
      Map<String, Double> minResourceRequirements, Map<String, Double> maxResourceRequirements) {
    if (minResourceRequirements != null) {
      this.minResourceRequirements.putAll(minResourceRequirements);
    }
    if (maxResourceRequirements != null) {
      this.maxResourceRequirements.putAll(maxResourceRequirements);
    }
  }

  // This interface will be deleted later.
  public Map<String, Double> getTotalResources() {
    return minResourceRequirements;
  }

  public Map<String, Double> getMinResources() {
    return minResourceRequirements;
  }

  public Map<String, Double> getMaxResources() {
    return maxResourceRequirements;
  }

  public static class Builder {
    private Long totalMemoryMb = null;
    private Long totalCpus = null;
    private Long totalGpus = null;

    private Long maxTotalMemoryMb = null;
    private Long maxTotalCpus = null;
    private Long maxTotalGpus = null;

    public Builder setTotalMemoryMb(long totalMemoryMb) {
      this.totalMemoryMb = totalMemoryMb;
      return this;
    }

    public Builder setTotalCpus(long totalCpus) {
      this.totalCpus = totalCpus;
      return this;
    }

    public Builder setTotalGpus(long totalGpus) {
      this.totalGpus = totalGpus;
      return this;
    }

    public Builder setMaxTotalMemoryMb(long maxTotalMemoryMb) {
      this.maxTotalMemoryMb = maxTotalMemoryMb;
      return this;
    }

    public Builder setMaxTotalCpus(long maxTotalCpus) {
      this.maxTotalCpus = maxTotalCpus;
      return this;
    }

    public Builder setMaxTotalGpus(long maxTotalGpus) {
      this.maxTotalGpus = maxTotalGpus;
      return this;
    }

    public UpdateJobTotalResourcesRequest build() {
      Map<String, Double> minResourceRequirements = new HashMap<>();
      Map<String, Double> maxResourceRequirements = new HashMap<>();
      if (totalMemoryMb != null) {
        if (totalMemoryMb % 50 != 0 || totalMemoryMb <= 0) {
          throw new IllegalArgumentException(
              "totalMemoryMb must be positive integer multiple of 50.");
        }
        minResourceRequirements.put("totalMemoryMb", Double.valueOf(totalMemoryMb));
        if (maxTotalMemoryMb == null) {
          maxResourceRequirements.put("maxTotalMemoryMb", Double.valueOf(totalMemoryMb));
        }
      }
      if (totalCpus != null) {
        if (totalCpus <= 0) {
          throw new IllegalArgumentException("totalCpus must be greater than 0.");
        }
        minResourceRequirements.put("totalCpus", Double.valueOf(totalCpus));
        if (maxTotalCpus == null) {
          maxResourceRequirements.put("maxTotalCpus", Double.valueOf(totalCpus));
        }
      }
      if (totalGpus != null) {
        if (totalGpus <= 0) {
          throw new IllegalArgumentException("totalGpus must be greater than 0.");
        }
        minResourceRequirements.put("totalGpus", Double.valueOf(totalGpus));
        if (maxTotalGpus == null) {
          maxResourceRequirements.put("maxTotalGpus", Double.valueOf(totalGpus));
        }
      }

      if (maxTotalMemoryMb != null) {
        if (totalMemoryMb == null) {
          throw new IllegalArgumentException(
              "It is not allowed to set only the value of maxTotalMemoryMb without setting the "
                  + "value of totalMemoryMb.");
        }
        if (maxTotalMemoryMb < totalMemoryMb) {
          throw new IllegalArgumentException(
              String.format(
                  "maxTotalMemoryMb(%d) must be positive integer multiple of 50 and >= "
                      + "totalMemoryMb(%d)",
                  maxTotalMemoryMb.longValue(), totalMemoryMb.longValue()));
        } else {
          maxResourceRequirements.put("maxTotalMemoryMb", Double.valueOf(maxTotalMemoryMb));
        }
      }
      if (maxTotalCpus != null) {
        if (maxTotalCpus == null) {
          throw new IllegalArgumentException(
              "It is not allowed to set only the value of maxTotalCpus without setting the "
                  + "value of totalCpus.");
        }
        if (maxTotalCpus < totalCpus) {
          throw new IllegalArgumentException(
              String.format(
                  "maxTotalCpus(%d) must >= totalCpus(%d)",
                  maxTotalCpus.longValue(), totalCpus.longValue()));
        } else {
          maxResourceRequirements.put("maxTotalCpus", Double.valueOf(maxTotalCpus));
        }
      }
      if (maxTotalGpus != null) {
        if (maxTotalGpus == null) {
          throw new IllegalArgumentException(
              "It is not allowed to set only the value of maxTotalGpus without setting the "
                  + "value of totalGpus.");
        }
        if (maxTotalGpus < totalGpus) {
          throw new IllegalArgumentException(
              String.format(
                  "maxTotalGpus(%d) must >= totalGpus(%d)",
                  maxTotalGpus.longValue(), totalGpus.longValue()));
        } else {
          maxResourceRequirements.put("maxTotalGpus", Double.valueOf(maxTotalGpus));
        }
      }

      if (minResourceRequirements.isEmpty()) {
        throw new IllegalArgumentException("The request can not be empty.");
      }
      return new UpdateJobTotalResourcesRequest(minResourceRequirements, maxResourceRequirements);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }
}
