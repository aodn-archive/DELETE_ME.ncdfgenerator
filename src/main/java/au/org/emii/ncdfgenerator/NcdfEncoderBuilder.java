package au.org.emii.ncdfgenerator;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;

import org.geoserver.catalog.DataStoreInfo;

import au.org.emii.ncdfgenerator.cql.ExprParser;
import au.org.emii.ncdfgenerator.cql.IDialectTranslate;
import au.org.emii.ncdfgenerator.cql.IExprParser;
import au.org.emii.ncdfgenerator.cql.PGDialectTranslate;


public class NcdfEncoderBuilder {
    // assemble the NcdfEncoder

    private String layerConfigDir;
    private String tmpCreationDir;

    public NcdfEncoderBuilder() {
    }

    public final NcdfEncoder create(String typename, String filterExpr, Connection conn) throws Exception {
        // TODO move args into builder methods

        InputStream config = null;
        try {
            IExprParser parser = new ExprParser();
            IDialectTranslate translate = new PGDialectTranslate();
            ICreateWritable createWritable = new CreateWritable(tmpCreationDir);
            IAttributeValueParser attributeValueParser = new AttributeValueParser();

            config = new FileInputStream(layerConfigDir + "/" + typename + ".xml");

            Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(config);
            Node node = document.getFirstChild();
            NcdfDefinition definition = new NcdfDefinitionXMLParser().parse(node);

            return new NcdfEncoder(parser, translate, conn, createWritable, attributeValueParser, definition, filterExpr);
        }
        finally {
            if (config != null) {
                config.close();
            }
            // conn.close();
        }
    }

    public final void setLayerConfigDir(String layerConfigDir) {
        this.layerConfigDir = layerConfigDir;
    }

    public final void setTmpCreationDir(String tmpCreationDir) {
        this.tmpCreationDir = tmpCreationDir;
    }
}

