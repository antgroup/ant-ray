package io.ray.api.options;

/**
 * This class is used to define rare resource type. This file keep in consistance with
 * rare_resources.py
 */
public class RareResourceType {
  // Used when users need GPU resources and don't care what kind of GPU card.
  public static final String GPU = "GPU";
  // Used when users need physical GPU card.
  public static final String T4_GPU = "nvidia.com/gpu";
  public static final String V100_PCIE_16GB_P = "nvidia.com/V100-PCIE-16GB-P";
  public static final String P100_PCIE_16GB_P = "nvidia.com/P100-PCIE-16GB-P";
  // Used when users need virtual GPU card.
  public static final String T4_1_PCIE_16GB_16GB_V = "nvidia.com/T4-1-PCIE-16GB-16GB-V";
  public static final String T4_1_PCIE_16GB_32GB_V = "nvidia.com/T4-1-PCIE-16GB-32GB-V";
  public static final String T4_1_PCIE_16GB_64GB_V = "nvidia.com/T4-1-PCIE-16GB-64GB-V";
  public static final String T4_2_PCIE_16GB_8GB_V = "nvidia.com/T4-2-PCIE-16GB-8GB-V";
  public static final String T4_2_PCIE_16GB_16GB_V = "nvidia.com/T4-2-PCIE-16GB-16GB-V";
  public static final String T4_4_PCIE_16GB_16GB_V = "nvidia.com/T4-4-PCIE-16GB-16GB-V";
  public static final String T4_4_PCIE_16GB_4GB_V = "nvidia.com/T4-4-PCIE-16GB-4GB-V";
  public static final String T4_4_PCIE_16GB_8GB_V = "nvidia.com/T4-4-PCIE-16GB-8GB-V";
  public static final String P100_1_PCIE_16GB_16GB_V = "nvidia.com/P100-1-PCIE-16GB-16GB-V";
  public static final String P100_1_PCIE_16GB_32GB_V = "nvidia.com/P100-1-PCIE-16GB-32GB-V";
  public static final String P100_1_PCIE_16GB_64GB_V = "nvidia.com/P100-1-PCIE-16GB-64GB-V";
  public static final String P100_2_PCIE_16GB_8GB_V = "nvidia.com/P100-2-PCIE-16GB-8GB-V";
  public static final String P100_2_PCIE_16GB_16GB_V = "nvidia.com/P100-2-PCIE-16GB-16GB-V";
  public static final String P100_4_PCIE_16GB_4GB_V = "nvidia.com/P100-4-PCIE-16GB-4GB-V";
  public static final String P100_4_PCIE_16GB_16GB_V = "nvidia.com/P100-4-PCIE-16GB-16GB-V";
  public static final String P100_8_PCIE_16GB_2GB_V = "nvidia.com/P100-8-PCIE-16GB-2GB-V";
  public static final String P100_8_PCIE_16GB_8GB_V = "nvidia.com/P100-8-PCIE-16GB-8GB-V";
  public static final String P100_8_PCIE_16GB_16GB_V = "nvidia.com/P100-8-PCIE-16GB-16GB-V";
  public static final String V100_1_PCIE_16GB_16GB_V = "nvidia.com/V100-1-PCIE-16GB-16GB-V";
  public static final String V100_1_PCIE_16GB_32GB_V = "nvidia.com/V100-1-PCIE-16GB-32GB-V";
  public static final String V100_1_PCIE_16GB_64GB_V = "nvidia.com/V100-1-PCIE-16GB-64GB-V";
  public static final String V100_2_PCIE_16GB_8GB_V = "nvidia.com/V100-2-PCIE-16GB-8GB-V";
  public static final String V100_2_PCIE_16GB_16GB_V = "nvidia.com/V100-2-PCIE-16GB-16GB-V";
  public static final String V100_4_PCIE_16GB_4GB_V = "nvidia.com/V100-4-PCIE-16GB-4GB-V";
  public static final String V100_4_PCIE_16GB_8GB_V = "nvidia.com/V100-4-PCIE-16GB-8GB-V";
  public static final String V100_4_PCIE_16GB_16GB_V = "nvidia.com/V100-4-PCIE-16GB-16GB-V";
  public static final String V100_1_PCIE_32GB_32GB_V = "nvidia.com/V100-1-PCIE-32GB-32GB-V";
  public static final String V100_1_PCIE_32GB_64GB_V = "nvidia.com/V100-1-PCIE-32GB-64GB-V";
  public static final String V100_1_PCIE_32GB_128GB_V = "nvidia.com/V100-1-PCIE-32GB-128GB-V";
  public static final String V100_2_PCIE_32GB_16GB_V = "nvidia.com/V100-2-PCIE-32GB-16GB-V";
  public static final String V100_2_PCIE_32GB_32GB_V = "nvidia.com/V100-2-PCIE-32GB-32GB-V";
  public static final String V100_2_PCIE_32GB_64GB_V = "nvidia.com/V100-2-PCIE-32GB-64GB-V";
  public static final String V100_4_PCIE_32GB_8GB_V = "nvidia.com/V100-4-PCIE-32GB-8GB-V";
  public static final String V100_4_PCIE_32GB_16GB_V = "nvidia.com/V100-4-PCIE-32GB-16GB-V";
  public static final String V100_4_PCIE_32GB_32GB_V = "nvidia.com/V100-4-PCIE-32GB-32GB-V";
  public static final String V100_8_PCIE_32GB_4GB_V = "nvidia.com/V100-8-PCIE-32GB-4GB-V";
  public static final String V100_8_PCIE_32GB_8GB_V = "nvidia.com/V100-8-PCIE-32GB-8GB-V";
  public static final String V100_8_PCIE_32GB_16GB_V = "nvidia.com/V100-8-PCIE-32GB-16GB-V";
  // Used when users need node with BIG_MEMORY resource label.
  public static final String BIG_MEMORY = "BIG_MEMORY";
  // Used when users need node with ACPU resource label.
  public static final String ACPU = "ACPU";
}
