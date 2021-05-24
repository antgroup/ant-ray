package org.ray.yarn.utils;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.htrace.fasterxml.jackson.annotation.JsonInclude;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.ray.yarn.config.AbstractConfig;
import org.ray.yarn.config.RayClusterConfig;
import org.yaml.snakeyaml.Yaml;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Read YAML file content and convert to objects
 */
public class YamlUtil {

  private static final Log log = LogFactory.getLog(YamlUtil.class);

  public static <T extends AbstractConfig> T loadFile(String filePath, Class<T> clazz)
      throws IOException {
    String protocol = getProtocol(filePath);
    switch (protocol) {
      case "http":
      case "https":
        return loadFromHttpFile(filePath, clazz);
      case "file":
        return loadFromLocalFile(filePath, clazz);
      case "ftp":
      default:
        log.error("Unsupported protocol: " + protocol);
        throw new IllegalArgumentException("Unsupported protocol: " + protocol);
    }
  }

  public static <T extends AbstractConfig> T loadFromHttpFile(String filePath, Class<T> clazz)
      throws IOException {
    throw new NotImplementedException();
  }

  public static <T extends AbstractConfig> T loadFromLocalFile(String filePath, Class<T> clazz)
      throws IOException {
    String content = readFile("test.txt", Charset.defaultCharset());
    return (T) loadFromString(content);
  }

  static RayClusterConfig loadFromString(String yamlContent) {
    Yaml yaml = new Yaml();
    LinkedHashMap map = yaml.load(yamlContent);
    ObjectMapper oMapper = new ObjectMapper();
    oMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    RayClusterConfig rayClusterConfig = oMapper.convertValue(map, RayClusterConfig.class);
    rayClusterConfig.setFullContent(map);
    return rayClusterConfig;
  }

  static String readFile(String path, Charset encoding) throws IOException {
    byte[] encoded = Files.readAllBytes(Paths.get(path));
    return new String(encoded, encoding);
  }

  static String getProtocol(String filePath) {
    try {
      URL fileURL = new URI(filePath).toURL();
      return fileURL.getProtocol();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Unrecognized path " + filePath);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Unrecognized path " + filePath);
    } catch (IllegalArgumentException e) {
      log.info("Path is not absolute");
      File file = new File(filePath);
      return getProtocol(file);
    }
  }

  static String getProtocol(File file) {
    try {
      URL fileURL = file.toURI().toURL();
      return fileURL.getProtocol();
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Unrecognized path " + file.getPath());
    }
  }
}
