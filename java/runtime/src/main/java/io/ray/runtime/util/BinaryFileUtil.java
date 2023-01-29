package io.ray.runtime.util;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;

public class BinaryFileUtil {

  public static final String CORE_WORKER_JAVA_LIBRARY = "core_worker_library_java";

  /**
   * Extract a platform-native resource file to <code>destDir</code>. Note that this a process-safe
   * operation. If multi processes extract the file to same directory concurrently, this operation
   * will be protected by a file lock.
   *
   * @param destDir a directory to extract resource file to
   * @param fileName resource file name
   * @return extracted resource file
   */
  public static File getNativeFile(String destDir, String fileName) {
    final File dir = new File(destDir);
    if (!dir.exists()) {
      try {
        FileUtils.forceMkdir(dir);
      } catch (IOException e) {
        throw new RuntimeException("Couldn't make directory: " + dir.getAbsolutePath(), e);
      }
    }
    String lockFilePath = destDir + File.separator + "file_lock";
    try (FileLock ignored = new RandomAccessFile(lockFilePath, "rw").getChannel().lock()) {
      String resourceDir;
      if (SystemUtils.IS_OS_MAC) {
        resourceDir = "native/darwin/";
      } else if (SystemUtils.IS_OS_LINUX) {
        resourceDir = "native/linux/";
      } else {
        throw new UnsupportedOperationException("Unsupported os " + SystemUtils.OS_NAME);
      }
      String resourcePath = resourceDir + fileName;
      File destFile = new File(String.format("%s/%s", destDir, fileName));
      if (destFile.exists()) {
        return destFile;
      }

      /// File doesn't exist. Create a temp file and then rename it.
      final String tempFilePath = String.format("%s/%s.tmp", destDir, fileName);
      // Adding a temporary file here is used to fix the issue that when
      // a java worker crashes during extracting dynamic library file, next
      // java worker will use an incomplete file. The issue link is:
      //
      // https://aone.alipay.com/issue/36836313?spm=a2o8d.corp_prod_issue_list.0.0.3b513e1axAelvM&stat=1.5.6&toPage=1&versionId=1969445
      File tempFile = new File(tempFilePath);
      for (ClassLoader classLoader :
          new ClassLoader[] {
            Thread.currentThread().getContextClassLoader(), BinaryFileUtil.class.getClassLoader()
          }) {
        try (InputStream is = classLoader.getResourceAsStream(resourcePath)) {
          if (is != null) {
            Files.copy(
                is, Paths.get(tempFile.getCanonicalPath()), StandardCopyOption.REPLACE_EXISTING);
            if (!tempFile.renameTo(destFile)) {
              throw new RuntimeException(
                  String.format(
                      "Couldn't rename temp file(%s) to %s",
                      tempFile.getAbsolutePath(), destFile.getAbsolutePath()));
            }
            return destFile;
          }
        } catch (IOException e) {
          throw new RuntimeException("Couldn't get temp file from resource " + fileName, e);
        }
      }
      throw new IllegalArgumentException(String.format("%s doesn't exist", fileName));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
