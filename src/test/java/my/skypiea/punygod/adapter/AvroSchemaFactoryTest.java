package my.skypiea.punygod.adapter;

import com.google.common.base.Function;
import com.google.common.collect.Ordering;
import junit.framework.Assert;
import junit.framework.TestCase;
import org.apache.calcite.util.Util;
import org.junit.Test;

import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.*;
import java.util.*;

/**
 * Created by wanghuan on 2017/6/13.
 */
public class AvroSchemaFactoryTest extends TestCase {


    /**
     * Tests an inline schema with a non-existent directory.
     */
    @Test
    public void testBadDirectory() throws SQLException {
        Properties info = new Properties();
        info.put("model",
                "inline:"
                        + "{\n"
                        + "  version: '1.0',\n"
                        + "   schemas: [\n"
                        + "     {\n"
                        + "       type: 'custom',\n"
                        + "       name: 'bad',\n"
                        + "       factory: 'my.skypiea.punygod.adapter.AvroSchemaFactory',\n"
                        + "       operand: {\n"
                        + "         directory: '/does/not/exist'\n"
                        + "       }\n"
                        + "     }\n"
                        + "   ]\n"
                        + "}");

        Connection connection =
                DriverManager.getConnection("jdbc:calcite:", info);
        // must print "directory ... not found" to stdout, but not fail
        ResultSet tables =
                connection.getMetaData().getTables(null, null, null, null);
        tables.next();
        tables.close();
        connection.close();
    }

    /**
     * Reads from a table.
     */
    @Test
    public void testSelect() throws SQLException {
        sql("model", "select count(name) from PersonInfo").ok();
    }


    private Fluent sql(String model, String sql) {
        return new Fluent(model, sql, output());
    }

    private Function<ResultSet, Void> output() {
        return new Function<ResultSet, Void>() {
            public Void apply(ResultSet resultSet) {
                try {
                    output(resultSet, System.out);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        };
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

    private void close(Connection connection, Statement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                // ignore
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void checkSql(String sql, String model, Function<ResultSet, Void> fn)
            throws SQLException {
        Connection connection = null;
        Statement statement = null;
        try {
            Properties info = new Properties();
            info.put("model", jsonPath(model));
            connection = DriverManager.getConnection("jdbc:calcite:", info);
            statement = connection.createStatement();
            final ResultSet resultSet =
                    statement.executeQuery(
                            sql);
            fn.apply(resultSet);
        } finally {
            close(connection, statement);
        }
    }

    private String jsonPath(String model) {
        return resourcePath(model + ".json");
    }

    private String resourcePath(String path) {
        final URL url = AvroSchemaFactoryTest.class.getResource("/" + path);
        // URL converts a space to %20, undo that.
        try {
            String s = URLDecoder.decode(url.toString(), "UTF-8");
            if (s.startsWith("file:")) {
                s = s.substring("file:".length());
            }
            return s;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Fluent API to perform test actions.
     */
    private class Fluent {
        private final String model;
        private final String sql;
        private final Function<ResultSet, Void> expect;

        Fluent(String model, String sql, Function<ResultSet, Void> expect) {
            this.model = model;
            this.sql = sql;
            this.expect = expect;
        }

        /**
         * Runs the test.
         */
        Fluent ok() {
            try {
                checkSql(sql, model, expect);
                return this;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Assigns a function to call to test whether output is correct.
         */
        Fluent checking(Function<ResultSet, Void> expect) {
            return new Fluent(model, sql, expect);
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query.
         */
        Fluent returns(String... expectedLines) {
            return checking(expect(expectedLines));
        }

        /**
         * Sets the rows that are expected to be returned from the SQL query,
         * in no particular order.
         */
        Fluent returnsUnordered(String... expectedLines) {
            return checking(expectUnordered(expectedLines));
        }
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private static Function<ResultSet, Void> expectUnordered(String... expected) {
        final List<String> expectedLines =
                Ordering.natural().immutableSortedCopy(Arrays.asList(expected));
        return new Function<ResultSet, Void>() {
            public Void apply(ResultSet resultSet) {
                try {
                    final List<String> lines = new ArrayList<>();
                    AvroSchemaFactoryTest.collect(lines, resultSet);
                    Collections.sort(lines);
                    Assert.assertEquals(expectedLines, lines);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        };
    }

    private static void collect(List<String> result, ResultSet resultSet)
            throws SQLException {
        final StringBuilder buf = new StringBuilder();
        while (resultSet.next()) {
            buf.setLength(0);
            int n = resultSet.getMetaData().getColumnCount();
            String sep = "";
            for (int i = 1; i <= n; i++) {
                buf.append(sep)
                        .append(resultSet.getMetaData().getColumnLabel(i))
                        .append("=")
                        .append(resultSet.getString(i));
                sep = "; ";
            }
            result.add(Util.toLinux(buf.toString()));
        }
    }

    /**
     * Returns a function that checks the contents of a result set against an
     * expected string.
     */
    private static Function<ResultSet, Void> expect(final String... expected) {
        return new Function<ResultSet, Void>() {
            public Void apply(ResultSet resultSet) {
                try {
                    final List<String> lines = new ArrayList<>();
                    AvroSchemaFactoryTest.collect(lines, resultSet);
                    Assert.assertEquals(Arrays.asList(expected), lines);
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
                return null;
            }
        };
    }

}