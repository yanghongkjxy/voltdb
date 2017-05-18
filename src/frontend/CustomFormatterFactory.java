
import java.util.Properties;

import org.voltdb.importer.formatter.AbstractFormatterFactory;
import org.voltdb.importer.formatter.Formatter;

public class CustomFormatterFactory  extends AbstractFormatterFactory {
    /**
     * Creates and returns the formatter object.
     */

    @Override
    public Formatter create(String m_formatName, Properties m_formatProps) {
        Formatter formatter = null;
        if (m_formatName.contains("CustomFormatterA")) {
                formatter = new CustomFormatterA(m_formatName, m_formatProps);
        } else {
            formatter = new CustomFormatterB(m_formatName, m_formatProps);
        }

        return formatter;
    }
}
