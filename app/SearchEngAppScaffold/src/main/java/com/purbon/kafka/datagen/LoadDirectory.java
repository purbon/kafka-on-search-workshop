package com.purbon.kafka.datagen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LoadDirectory {


  private static final String RAW_FILES_TOPIC = "io.files.raw";

  public static void load(Path dir) throws IOException {

    final SimpleKafkaProducer producer = new SimpleKafkaProducer("localhost:9092");

    try {
      Files
          .list(dir)
          .filter(path -> path.endsWith(".txt"))
          .forEach(path -> {
            try {
              byte[] contentInBytes = Files.readAllBytes(path);
              producer.send(RAW_FILES_TOPIC, path.hashCode(), new String(contentInBytes));
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    } finally {
      producer.close();
    }

  }

  public static void main(String [] args) throws Exception {
    String dir = args[1];
    LoadDirectory.load(Paths.get(dir));
  }
}
