package io.unitycatalog.server.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.ConsumesJson;
import com.linecorp.armeria.server.annotation.ExceptionHandler;
import com.linecorp.armeria.server.annotation.Get;
import com.linecorp.armeria.server.annotation.Head;
import com.linecorp.armeria.server.annotation.Param;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.annotation.ProducesJson;
import io.unitycatalog.server.exception.IcebergRestExceptionHandler;
import io.unitycatalog.server.model.CreateTable;
import io.unitycatalog.server.model.DataSourceFormat;
import io.unitycatalog.server.model.ListSchemasResponse;
import io.unitycatalog.server.model.ListTablesResponse;
import io.unitycatalog.server.model.SchemaInfo;
import io.unitycatalog.server.model.TableInfo;
import io.unitycatalog.server.model.TableType;
import io.unitycatalog.server.persist.Repositories;
import io.unitycatalog.server.persist.TableRepository;
import io.unitycatalog.server.service.iceberg.MetadataService;
import io.unitycatalog.server.service.iceberg.TableConfigService;
import io.unitycatalog.server.utils.JsonUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;

@ExceptionHandler(IcebergRestExceptionHandler.class)
public class IcebergRestCatalogService {

  private static final String PREFIX_BASE = "catalogs/";

  private static final List<Endpoint> ENDPOINTS =
      List.of(
          Endpoint.V1_LIST_NAMESPACES,
          Endpoint.V1_LOAD_NAMESPACE,
          Endpoint.V1_TABLE_EXISTS,
          Endpoint.V1_LOAD_TABLE,
          Endpoint.V1_LOAD_VIEW,
          Endpoint.V1_REPORT_METRICS,
          Endpoint.V1_LIST_TABLES);

  private final CatalogService catalogService;
  private final SchemaService schemaService;
  private final TableService tableService;
  private final TableConfigService tableConfigService;
  private final MetadataService metadataService;
  private final TableRepository tableRepository;

  public IcebergRestCatalogService(
      CatalogService catalogService,
      SchemaService schemaService,
      TableService tableService,
      TableConfigService tableConfigService,
      MetadataService metadataService,
      Repositories repositories) {
    this.catalogService = catalogService;
    this.schemaService = schemaService;
    this.tableService = tableService;
    this.tableConfigService = tableConfigService;
    this.metadataService = metadataService;
    this.tableRepository = repositories.getTableRepository();
  }

  // Config APIs

  @Get("/v1/config")
  @ProducesJson
  public ConfigResponse config(@Param("warehouse") Optional<String> catalogOpt) {
    String catalog =
        catalogOpt.orElseThrow(
            () -> new BadRequestException("Must supply a proper catalog in warehouse property."));

    // TODO: check catalog exists
    // set catalog prefix
    return ConfigResponse.builder()
        .withOverride("prefix", PREFIX_BASE + catalog)
        .withEndpoints(ENDPOINTS)
        .build();
  }

  // Namespace APIs

  @Get("/v1/catalogs/{catalog}/namespaces")
  @ProducesJson
  public ListNamespacesResponse listNamespaces(
      @Param("catalog") String catalog, @Param("parent") Optional<String> parent)
      throws JsonProcessingException {
    List<Namespace> namespaces;
    if (parent.isPresent() && !parent.get().isEmpty()) {
      // nested namespaces is not supported, so child namespaces will be empty
      namespaces = Collections.emptyList();
    } else {
      String respContent =
          schemaService
              .listSchemas(catalog, Optional.of(Integer.MAX_VALUE), Optional.empty())
              .aggregate()
              .join()
              .contentUtf8();
      ListSchemasResponse resp =
          JsonUtils.getInstance().readValue(respContent, ListSchemasResponse.class);
      assert resp.getSchemas() != null;
      namespaces =
          resp.getSchemas().stream()
              .map(schemaInfo -> Namespace.of(schemaInfo.getName()))
              .collect(Collectors.toList());
    }

    return ListNamespacesResponse.builder().addAll(namespaces).build();
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}")
  @ProducesJson
  public GetNamespaceResponse getNamespace(
      @Param("catalog") String catalog, @Param("namespace") String namespace)
      throws JsonProcessingException {

    String schemaFullName = String.join(".", catalog, namespace);
    String resp = schemaService.getSchema(schemaFullName).aggregate().join().contentUtf8();
    return GetNamespaceResponse.builder()
        .withNamespace(Namespace.of(namespace))
        .setProperties(JsonUtils.getInstance().readValue(resp, SchemaInfo.class).getProperties())
        .build();
  }

  // Table APIs

  @Head("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  public HttpResponse tableExists(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table) {
    try {
      TableInfo tableInfo = tableRepository.getTable(catalog + "." + namespace + "." + table);
      // For Iceberg tables, storageLocation contains the metadata location
      if (tableInfo.getDataSourceFormat() == DataSourceFormat.ICEBERG
          && tableInfo.getStorageLocation() != null) {
        return HttpResponse.of(HttpStatus.OK);
      } else {
        throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
      }
    } catch (Exception e) {
      throw new NoSuchTableException("Table does not exist: %s", namespace + "." + table);
    }
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}")
  @ProducesJson
  public LoadTableResponse loadTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      @Param("table") String table) {
    TableInfo tableInfo = tableRepository.getTable(catalog + "." + namespace + "." + table);

    // For Iceberg tables, storageLocation contains the metadata location
    if (tableInfo.getDataSourceFormat() != DataSourceFormat.ICEBERG) {
      throw new NoSuchTableException("Not an Iceberg table: %s", namespace + "." + table);
    }

    String metadataLocation = tableInfo.getStorageLocation();
    if (metadataLocation == null) {
      throw new NoSuchTableException("No metadata location for table: %s", namespace + "." + table);
    }

    TableMetadata tableMetadata = metadataService.readTableMetadata(metadataLocation);
    Map<String, String> config = tableConfigService.getTableConfig(tableMetadata);

    return LoadTableResponse.builder()
        .withTableMetadata(tableMetadata)
        .addAllConfig(config)
        .build();
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/views/{view}")
  @ProducesJson
  public LoadViewResponse loadView(
      @Param("namespace") String namespace, @Param("view") String view) {
    // this is not supported yet, but Iceberg REST client tries to load
    // a table with given path name and then tries to load a view with that
    // name if it didn't find a table, so for now, let's just return a 404
    // as that should be expected since it didn't find a table with the name
    throw new NoSuchViewException("View does not exist: %s", namespace + "." + view);
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/tables/{table}/metrics")
  public HttpResponse reportMetrics(
      @Param("namespace") String namespace, @Param("table") String table) {
    return HttpResponse.of(HttpStatus.OK);
  }

  @Get("/v1/catalogs/{catalog}/namespaces/{namespace}/tables")
  @ProducesJson
  public org.apache.iceberg.rest.responses.ListTablesResponse listTables(
      @Param("catalog") String catalog, @Param("namespace") String namespace)
      throws JsonProcessingException {
    // Log the request for debugging
    System.err.println(
        "Iceberg REST listTables called - catalog: " + catalog + ", namespace: " + namespace);

    try {
      // Use the tableRepository to get ICEBERG tables
      ListTablesResponse unityResponse =
          tableRepository.listTables(
              catalog,
              namespace,
              Optional.of(Integer.MAX_VALUE),
              Optional.empty(),
              true,
              true 
              );

      // Filter for ICEBERG tables and convert to Iceberg format
      List<TableIdentifier> icebergTables = new ArrayList<>();
      for (TableInfo table : unityResponse.getTables()) {
        if (table.getDataSourceFormat() == DataSourceFormat.ICEBERG) {
          icebergTables.add(
              TableIdentifier.of(Namespace.of(table.getSchemaName()), table.getName()));
        }
      }

      System.err.println(
          "Found " + icebergTables.size() + " Iceberg tables in " + catalog + "." + namespace);

      return org.apache.iceberg.rest.responses.ListTablesResponse.builder()
          .addAll(icebergTables)
          .build();
    } catch (Exception e) {
      System.err.println("Error listing tables: " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException("Failed to list tables", e);
    }
  }

  @Post("/v1/catalogs/{catalog}/namespaces/{namespace}/register")
  @ProducesJson
  @ConsumesJson
  public LoadTableResponse registerTable(
      @Param("catalog") String catalog,
      @Param("namespace") String namespace,
      RegisterTableRequest request)
      throws IOException {

    TableMetadata tableMetadata = parseTableMetadataFromLocation(request.metadataLocation());

    List<io.unitycatalog.server.model.ColumnInfo> columns =
        extractColumnsFromIcebergSchema(tableMetadata);

    System.err.println("REGISTER TABLE: Table name: " + request.name());

    CreateTable createTable =
        new CreateTable()
            .name(request.name())
            .catalogName(catalog)
            .schemaName(namespace)
            .properties(tableMetadata.properties())
            .tableType(TableType.EXTERNAL)
            .dataSourceFormat(DataSourceFormat.ICEBERG)
            .storageLocation(request.metadataLocation())
            .columns(columns); 

    tableRepository.createTable(createTable);

    System.err.println("REGISTER TABLE: Table created successfully");

    return LoadTableResponse.builder().withTableMetadata(tableMetadata).build();
  }

  private TableMetadata parseTableMetadataFromLocation(String metadataLocation) {
    return metadataService.readTableMetadata(metadataLocation);
  }

  private List<io.unitycatalog.server.model.ColumnInfo> extractColumnsFromIcebergSchema(
      TableMetadata tableMetadata) {
    List<io.unitycatalog.server.model.ColumnInfo> columns = new ArrayList<>();

    org.apache.iceberg.Schema icebergSchema = tableMetadata.schema();

    // Convert each Iceberg field to Unity Catalog column
    for (org.apache.iceberg.types.Types.NestedField field : icebergSchema.columns()) {
      String columnType = convertIcebergTypeToUnityCatalog(field.type());
      io.unitycatalog.server.model.ColumnTypeName typeName = getColumnTypeName(columnType);

      io.unitycatalog.server.model.ColumnInfo column =
          new io.unitycatalog.server.model.ColumnInfo()
              .name(field.name())
              .typeText(columnType)
              .typeJson(generateTypeJson(field.name(), columnType, !field.isRequired()))
              .typeName(typeName)
              .position(field.fieldId())
              .nullable(!field.isRequired())
              .comment(field.doc());

      // Set precision and scale for DECIMAL types
      if (field.type().typeId() == org.apache.iceberg.types.Type.TypeID.DECIMAL) {
        org.apache.iceberg.types.Types.DecimalType decimal =
            (org.apache.iceberg.types.Types.DecimalType) field.type();
        column.typePrecision(decimal.precision());
        column.typeScale(decimal.scale());
      }

      System.err.println(
          "COLUMN EXTRACTION: Added column - name: "
              + field.name()
              + ", type: "
              + columnType
              + ", typeName: "
              + typeName
              + ", typeJson: "
              + column.getTypeJson()
              + ", position: "
              + field.fieldId()
              + ", nullable: "
              + !field.isRequired());

      columns.add(column);
    }

    System.err.println("COLUMN EXTRACTION: Successfully extracted " + columns.size() + " columns");
    return columns;
  }

  private String generateTypeJson(String name, String type, boolean nullable) {
    // Generate JSON representation of the type
    // Based on examples from Unity Catalog tests
    String baseType = type;

    // Extract base type for complex types
    if (type.startsWith("ARRAY<")) {
      baseType = "array";
    } else if (type.startsWith("MAP<")) {
      baseType = "map";
    } else if (type.startsWith("STRUCT")) {
      baseType = "struct";
    } else if (type.startsWith("DECIMAL")) {
      // Extract precision and scale for DECIMAL types
      if (type.matches("DECIMAL\\(\\d+,\\d+\\)")) {
        String[] parts = type.substring(8, type.length() - 1).split(",");
        return String.format(
            "{\"name\":\"%s\",\"type\":\"decimal\",\"precision\":%s,\"scale\":%s,\"nullable\":%s,\"metadata\":{}}",
            name, parts[0], parts[1], nullable);
      }
      baseType = "decimal";
    } else {
      // Map Unity Catalog types to JSON types
      switch (type) {
        case "BOOLEAN":
          baseType = "boolean";
          break;
        case "INT":
          baseType = "integer";
          break;
        case "LONG":
          baseType = "long";
          break;
        case "FLOAT":
          baseType = "float";
          break;
        case "DOUBLE":
          baseType = "double";
          break;
        case "DATE":
          baseType = "date";
          break;
        case "TIMESTAMP":
          baseType = "timestamp";
          break;
        case "STRING":
          baseType = "string";
          break;
        case "BINARY":
          baseType = "binary";
          break;
        case "TIME":
          baseType = "time";
          break;
        default:
          baseType = type.toLowerCase();
      }
    }

    return String.format(
        "{\"name\":\"%s\",\"type\":\"%s\",\"nullable\":%s,\"metadata\":{}}",
        name, baseType, nullable);
  }

  private io.unitycatalog.server.model.ColumnTypeName getColumnTypeName(String typeText) {
    // Map the type text to the ColumnTypeName enum
    // Handle complex types by extracting the base type
    if (typeText.startsWith("ARRAY<")) {
      return io.unitycatalog.server.model.ColumnTypeName.ARRAY;
    } else if (typeText.startsWith("MAP<")) {
      return io.unitycatalog.server.model.ColumnTypeName.MAP;
    } else if (typeText.startsWith("STRUCT")) {
      return io.unitycatalog.server.model.ColumnTypeName.STRUCT;
    } else if (typeText.startsWith("DECIMAL")) {
      return io.unitycatalog.server.model.ColumnTypeName.DECIMAL;
    } else if (typeText.equals("TIME")) {
      // Unity Catalog doesn't have TIME type, map to STRING
      return io.unitycatalog.server.model.ColumnTypeName.STRING;
    }

    try {
      return io.unitycatalog.server.model.ColumnTypeName.fromValue(typeText);
    } catch (IllegalArgumentException e) {
      // If type is not found, default to STRING
      System.err.println(
          "COLUMN EXTRACTION: Warning - Unknown type '" + typeText + "', defaulting to STRING");
      return io.unitycatalog.server.model.ColumnTypeName.STRING;
    }
  }

  private String convertIcebergTypeToUnityCatalog(org.apache.iceberg.types.Type icebergType) {
    // Map Iceberg types to Unity Catalog SQL types
    switch (icebergType.typeId()) {
      case BOOLEAN:
        return "BOOLEAN";
      case INTEGER:
        return "INT";
      case LONG:
        return "LONG";
      case FLOAT:
        return "FLOAT";
      case DOUBLE:
        return "DOUBLE";
      case DATE:
        return "DATE";
      case TIME:
        return "TIME";
      case TIMESTAMP:
        // Iceberg TIMESTAMP is with timezone, while TIMESTAMPTZ is without
        // But Unity Catalog doesn't have TIMESTAMPTZ, so we use TIMESTAMP for both
        return "TIMESTAMP";
      case STRING:
        return "STRING";
      case BINARY:
        return "BINARY";
      case DECIMAL:
        org.apache.iceberg.types.Types.DecimalType decimal =
            (org.apache.iceberg.types.Types.DecimalType) icebergType;
        return String.format("DECIMAL(%d,%d)", decimal.precision(), decimal.scale());
      case LIST:
        org.apache.iceberg.types.Types.ListType list =
            (org.apache.iceberg.types.Types.ListType) icebergType;
        return String.format("ARRAY<%s>", convertIcebergTypeToUnityCatalog(list.elementType()));
      case MAP:
        org.apache.iceberg.types.Types.MapType map =
            (org.apache.iceberg.types.Types.MapType) icebergType;
        return String.format(
            "MAP<%s,%s>",
            convertIcebergTypeToUnityCatalog(map.keyType()),
            convertIcebergTypeToUnityCatalog(map.valueType()));
      case STRUCT:
        return "STRUCT";
      default:
        // Default to STRING for unknown types
        return "STRING";
    }
  }
}
