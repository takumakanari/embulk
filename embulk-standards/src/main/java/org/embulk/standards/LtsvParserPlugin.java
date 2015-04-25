package org.embulk.standards;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.embulk.config.*;
import org.embulk.spi.*;
import org.embulk.spi.time.TimestampParseException;
import org.embulk.spi.time.TimestampParser;
import org.embulk.spi.type.TimestampType;
import org.embulk.spi.util.LineDecoder;
import org.slf4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class LtsvParserPlugin
        implements ParserPlugin
{

    private static final ImmutableSet<String> TRUE_STRINGS =
            ImmutableSet.of(
                    "true", "True", "TRUE",
                    "yes", "Yes", "YES",
                    "y", "Y",
                    "on", "On", "ON",
                    "1");

    public interface PluginTask
            extends Task, LineDecoder.DecoderTask, TimestampParser.ParserTask
    {
        @Config("columns")
        public SchemaConfig getSchemaConfig();

        @Config("null_string")
        @ConfigDefault("null")
        public Optional<String> getNullString();

        @Config("allow_irregular_column_order")
        @ConfigDefault("false")
        public boolean getAllowIrregularColumnOrder();
    }

    private final Logger logger;

    public LtsvParserPlugin()
    {
        logger = Exec.getLogger(CsvParserPlugin.class);
    }

    @Override
    public void transaction(ConfigSource config, Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump(), task.getSchemaConfig().toSchema());
    }

    private Map<String, TimestampParser> newTimestampParsers(
            TimestampParser.ParserTask task, Schema schema)
    {
        Map<String, TimestampParser> parsers = new HashMap<>();
        for (Column column : schema.getColumns()) {
            if (column.getType() instanceof TimestampType) {
                TimestampType tt = (TimestampType) column.getType();
                parsers.put(column.getName(), new TimestampParser(tt.getFormat(), task));
            }
        }
        return parsers;
    }

    @Override
    public void run(TaskSource taskSource, final Schema schema,
                    FileInput input, PageOutput output)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final Map<String, TimestampParser> timestampFormatters = newTimestampParsers(task, schema);

        LineDecoder lineDecoder = new LineDecoder(input, task);
        final LtsvTokenizer tokenizer = new LtsvTokenizer(lineDecoder);

        final String nullStringOrNull = task.getNullString().orNull();
        final boolean allowIrregularColumnOrder = task.getAllowIrregularColumnOrder();

        try (final PageBuilder pageBuilder = new PageBuilder(Exec.getBufferAllocator(), schema, output)) {
            while (tokenizer.nextFile()) {
                while (true) {
                    if (!tokenizer.nextRecord()) {
                        break;
                    }

                    schema.visitColumns(new ColumnVisitor() {
                        public void booleanColumn(Column column)
                        {
                            String v = nextColumn(column);
                            if (v == null) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setBoolean(column, TRUE_STRINGS.contains(v));
                            }
                        }

                        public void longColumn(Column column)
                        {
                            String v = nextColumn(column);
                            if (v == null) {
                                pageBuilder.setNull(column);
                            } else {
                                try {
                                    pageBuilder.setLong(column, Long.parseLong(v));
                                } catch (NumberFormatException e) {
                                    throw new LtsvRecordValidateException(e);
                                }
                            }
                        }

                        public void doubleColumn(Column column)
                        {
                            String v = nextColumn(column);
                            if (v == null) {
                                pageBuilder.setNull(column);
                            } else {
                                try {
                                    pageBuilder.setDouble(column, Double.parseDouble(v));
                                } catch (NumberFormatException e) {
                                    throw new LtsvRecordValidateException(e);
                                }
                            }
                        }

                        public void stringColumn(Column column)
                        {
                            String v = nextColumn(column);
                            if (v == null) {
                                pageBuilder.setNull(column);
                            } else {
                                pageBuilder.setString(column, v);
                            }
                        }

                        public void timestampColumn(Column column)
                        {
                            String v = nextColumn(column);
                            if (v == null) {
                                pageBuilder.setNull(column);
                            } else {
                                try {
                                    pageBuilder.setTimestamp(column, timestampFormatters.get(column.getName()).parse(v));
                                } catch (TimestampParseException e) {
                                    throw new LtsvRecordValidateException(e);
                                }
                            }
                        }

                        Map<String, String> columnMap = null;

                        private String nextColumn(Column column)
                        {
                            if (!tokenizer.hasNextColumn()) {
                                return null;
                            }
                            String v;
                            if (allowIrregularColumnOrder) {
                                if (columnMap == null) {
                                    columnMap = tokenizer.nextAllColumns();
                                }
                                v = columnMap.get(column.getName());
                            } else {
                                v = tokenizer.nextColumn()[1];
                            }
                            if (!v.isEmpty()) {
                                if (v.equals(nullStringOrNull)) {
                                    return null;
                                }
                                return v;
                            } else {
                                return null;
                            }
                        }
                    });
                    pageBuilder.addRecord();
                }

                pageBuilder.finish();
            }
        }
    }

    static class LtsvRecordValidateException
            extends RuntimeException
    {
        LtsvRecordValidateException(Throwable cause)
        {
            super(cause);
        }
    }
}
