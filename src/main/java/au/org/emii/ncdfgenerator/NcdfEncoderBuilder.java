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

// import au.org.emii.ncdfgenerator.NcdfDefinition;


public class NcdfEncoderBuilder {
    // assemble the NcdfEncoder

//    private String layerConfigDir;
    private String tmpCreationDir;
    private NcdfDefinition definition;
    private String filterExpr;
    private Connection conn;

    public NcdfEncoderBuilder() {
    }

/*
    - It's almost certainly better to avoid handling resources in here
    as much as possible.

    - eg. the actual definition should be a definition node ....
*/
    public final NcdfEncoder create() throws Exception {
        // TODO move args into builder methods

        IExprParser parser = new ExprParser();
        IDialectTranslate translate = new PGDialectTranslate();
        ICreateWritable createWritable = new CreateWritable(tmpCreationDir);
        IAttributeValueParser attributeValueParser = new AttributeValueParser();

/*
        config = new FileInputStream(layerConfigDir + "/" + typename + ".xml");

        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(config);
        Node node = document.getFirstChild();
        NcdfDefinition definition = new NcdfDefinitionXMLParser().parse(node);
*/

        if(tmpCreationDir == null) {
           throw new IllegalArgumentException("tmpCreationDir not set");
        }
        else if(definition == null) {
           throw new IllegalArgumentException("definition not set");
        }
        else if(filterExpr == null || filterExpr.equals("")) {
           throw new IllegalArgumentException("filterExpr not set");
        }
        else if(conn == null) {
           throw new IllegalArgumentException("conn not set");
        } 

        return new NcdfEncoder(parser, translate, conn, createWritable, attributeValueParser, definition, filterExpr);
    }

/*
    public final void setLayerConfigDir(String layerConfigDir) {
        this.layerConfigDir = layerConfigDir;
    }
*/
    public final void setTmpCreationDir(String tmpCreationDir) {
        this.tmpCreationDir = tmpCreationDir;
    }

    public final void setDefinition(NcdfDefinition ncdfDefinition) {
        this.definition = definition;
    }

    public final void setFilterExpr(String filterExpr) {
        this.filterExpr = filterExpr;
    }

    public final void setConnection(Connection conn) {
        this.conn = conn;
    }
}

