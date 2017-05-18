import java.util.Properties;

import org.voltdb.importer.formatter.Formatter;

import au.com.bytecode.opencsv_voltpatches.CSVParser;

import org.voltdb.importer.formatter.FormatException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class CustomFormatterB implements Formatter {
    final CSVParser m_parser;
    public CustomFormatterB (String formatName, Properties prop) {
        char separator = ',';
        char quotechar = CSVParser.DEFAULT_QUOTE_CHARACTER;
        char escape = CSVParser.DEFAULT_ESCAPE_CHARACTER;
        boolean strictQuotes = CSVParser.DEFAULT_STRICT_QUOTES;
        boolean ignoreLeadingWhiteSpace = CSVParser.DEFAULT_IGNORE_LEADING_WHITESPACE;
        m_parser = new CSVParser(separator, quotechar, escape, strictQuotes, ignoreLeadingWhiteSpace);
    }

    @Override
    public Object[] transform(ByteBuffer payload) throws FormatException {
        String line = null;
        try {
            if (payload == null) {
                return null;
            }
            line = new String(payload.array(), payload.arrayOffset(), payload.limit(), StandardCharsets.UTF_8);
            Object list[] = m_parser.parseLine(line);
            if (list != null) {
                for (int i = 0; i < list.length; i++) {
                    if ("NULL".equals(list[i])
                            || "\\N".equals(list[i])
                            || "\"\\N\"".equals(list[i])) {
                        list[i] = null;
                    }
                }
            }
            return list;
        } catch (IOException e) {
            throw new FormatException("failed to format " + line, e);
        }
    }
}
