package io.unitycatalog.server.service.iceberg;

import org.apache.iceberg.Files;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

public class SimpleLocalFileIO implements FileIO {
  @Override
  public InputFile newInputFile(String path) {
    // Handle file:// URIs by stripping the scheme
    if (path.startsWith("file://")) {
      path = path.substring(7); 
    } else if (path.startsWith("file:")) {
      path = path.substring(5); 
    }
    return Files.localInput(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteFile(String path) {
    throw new UnsupportedOperationException();
  }
}
