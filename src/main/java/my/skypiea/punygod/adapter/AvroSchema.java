package my.skypiea.punygod.adapter;

import org.apache.avro.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Schema mapped onto a directory of CSV files. Each table in the schema
 * is a CSV file in that directory.
 */
public class AvroSchema extends AbstractSchema {
    private Map<String, Table> tableMap;

    public AvroSchema() {
    }

    @Override
    protected Map<String, Table> getTableMap() {
        if(tableMap != null) {
          return tableMap;
        }
         String demo1 = "{\n" +
                 "  \"type\": \"record\",\n" +
                 "  \"name\": \"PersonInfo\",\n" +
                 "  \"namespace\": \"chana\",\n" +
                 "  \"fields\": [\n" +
                 "    {\n" +
                 "      \"name\": \"name\",\n" +
                 "      \"type\": \"string\"\n" +
                 "    },\n" +
                 "    {\n" +
                 "      \"name\": \"age\",\n" +
                 "      \"type\": \"int\"\n" +
                 "    },\n" +
                 "    {\n" +
                 "      \"name\": \"gender\",\n" +
                 "      \"type\": {\n" +
                 "        \"type\": \"enum\",\n" +
                 "        \"name\": \"GenderType\",\n" +
                 "        \"symbols\": [\n" +
                 "          \"Female\",\n" +
                 "          \"Male\",\n" +
                 "          \"Unknown\"\n" +
                 "        ]\n" +
                 "      },\n" +
                 "      \"default\": \"Unknown\"\n" +
                 "    },\n" +
                 "    {\n" +
                 "      \"name\": \"emails\",\n" +
                 "      \"type\": {\n" +
                 "        \"type\": \"array\",\n" +
                 "        \"items\": \"string\"\n" +
                 "      }\n" +
                 "    }\n" +
                 "  ]\n" +
                 "}\n";
        Schema schema =  new Schema.Parser().parse(demo1);
        AvroTable avroTable = new AvroTable(schema);
        // Build a map from table name to table; each file becomes a table.
        tableMap = new HashMap<>();
        tableMap.put("PERSONINFO", avroTable);

        return tableMap;
    }
}
