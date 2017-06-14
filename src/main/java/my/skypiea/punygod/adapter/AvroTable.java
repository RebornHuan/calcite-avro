package my.skypiea.punygod.adapter;


import my.skypiea.punygod.AvroSchemaConverter;
import org.apache.avro.Schema;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/**
 * Created by wanghuan on 2017/6/13.
 */
public class AvroTable extends AbstractTable implements ScannableTable {
    private final Schema rootSchema;

    AvroTable(Schema schema) {
        this.rootSchema = schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return new AvroSchemaConverter(typeFactory, rootSchema).convert();
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<Object[]>() {
            public Enumerator<Object[]> enumerator() {
                return new AvroEnumerator();
            }
        };
    }
}
