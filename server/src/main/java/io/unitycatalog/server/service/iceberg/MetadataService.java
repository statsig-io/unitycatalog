package io.unitycatalog.server.service.iceberg;

import java.net.URI;
import java.util.concurrent.CompletableFuture;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.io.FileIO;

public class MetadataService {

  private final FileIOFactory fileIOFactory;

  public MetadataService(FileIOFactory fileIOFactory) {
    this.fileIOFactory = fileIOFactory;
  }

  public TableMetadata readTableMetadata(String metadataLocation) {
    URI metadataLocationUri = URI.create(metadataLocation);
    // TODO: cache fileIO
    FileIO fileIO = fileIOFactory.getFileIO(metadataLocationUri);

    try {

      String pathForParser = metadataLocation;
      if (metadataLocation.startsWith("file://")) {
        pathForParser = metadataLocation.substring(7);
      } else if (metadataLocation.startsWith("file:")) {
        pathForParser = metadataLocation.substring(5);
      }

      System.err.println("DEBUG: Reading metadata from: " + metadataLocation);
      System.err.println("DEBUG: Path for parser: " + pathForParser);

      final String finalPath = pathForParser;
      return CompletableFuture.supplyAsync(() -> TableMetadataParser.read(fileIO, finalPath))
          .join();
    } catch (Exception e) {
      System.err.println("Error reading metadata from " + metadataLocation + ": " + e.getMessage());
      if (e.getCause() != null) {
        System.err.println("Caused by: " + e.getCause().getMessage());
        e.getCause().printStackTrace();
      }
      throw new IllegalArgumentException(metadataLocation + " is not a valid metadata file", e);
    }
  }
}
