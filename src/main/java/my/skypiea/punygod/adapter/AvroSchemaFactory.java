package my.skypiea.punygod.adapter;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.Map;

/**
 * Factory that creates a {@link AvroSchema}.
 * <p>
 * <p>Allows a custom schema to be included in a <code><i>model</i>.json</code>
 * file.
 */
public class AvroSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {
        System.out.println(map.get("directory"));
        return new AvroSchema();
    }
}
