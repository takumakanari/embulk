package org.embulk.standards;

import org.embulk.spi.util.LineDecoder;

import java.util.*;
import java.util.regex.Pattern;

public class LtsvTokenizer
{
    private static final String TAB = "\t";
    private static final Pattern LABEL_VALUE_SEPARATOR = Pattern.compile(":");

    private final LineDecoder input;

    private StringTokenizer lineTokenizer = null;

    public LtsvTokenizer(LineDecoder input)
    {
        this.input = input;
    }

    public boolean nextFile()
    {
        return input.nextFile();
    }

    public boolean nextRecord()
    {
        return nextLine(true);
    }

    public boolean hasNextColumn()
    {
        return lineTokenizer.hasMoreTokens();
    }

    public String[] nextColumn()
    {
        if (!hasNextColumn()) {
            throw new StandardException.TooFewColumnsException("Too few columns");
        }
        final String labelValue = lineTokenizer.nextToken();
        final String[] values = LABEL_VALUE_SEPARATOR.split(labelValue, 2);
        if (values.length != 2) {
            throw new StandardException.InvalidFormatException(String.format("Can not format field %s by separator %s",
                    labelValue, LABEL_VALUE_SEPARATOR));
        }
        return values;
    }

    public Map<String, String> nextAllColumns()
    {
        final Map<String, String> dest = new HashMap<>();
        while(hasNextColumn()) {
            final String[] labelValue = nextColumn();
            dest.put(labelValue[0], labelValue[1]);
        }
        return dest;
    }

    private boolean nextLine(boolean ignoreEmptyLine)
    {
        final String line = input.poll();
        if (line == null) {
            return false;
        }
        if (!line.isEmpty() || !ignoreEmptyLine) {
            lineTokenizer = new StringTokenizer(line, TAB);
            return true;
        }
        return false;
    }

}
