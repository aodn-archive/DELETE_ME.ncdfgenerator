
package au.org.emii.ncdfgenerator;

import au.org.emii.ncdfgenerator.cql.CQLException;
import org.postgresql.util.PSQLException;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Properties;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.lang.System;

import java.sql.*;

import au.org.emii.ncdfgenerator.IOutputFormatter;

import au.org.emii.ncdfgenerator.cql.ExprParser;
import au.org.emii.ncdfgenerator.cql.IExprParser;
import au.org.emii.ncdfgenerator.cql.IDialectTranslate;
import au.org.emii.ncdfgenerator.cql.PGDialectTranslate;
import au.org.emii.ncdfgenerator.cql.PGDialectTranslate;
import au.org.emii.ncdfgenerator.IOutputFormatter;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import javax.xml.parsers.DocumentBuilderFactory;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;


class MockOutputterCounter implements IOutputFormatter
{
    int count;
    public final void prepare(OutputStream os) {
        this.count = 0;
    }

    public final void write(String filename, InputStream is) {
        ++count;
    }

    public final void finish() {
    }

    public int getCount() {
        return count;
    }
};


public class GenerationIT {

    static final String TMPDIR = "./tmp";

    @Before
    public void setup() {

        // TODO factor this name into var
        new File(TMPDIR).mkdirs();
    }

    private NcdfEncoder getEncoder( InputStream config, String filterExpr, Connection conn, IOutputFormatter outputGenerator ) throws Exception {

        // we can't use the builder for this, becuase config is a stream...

        IExprParser parser = new ExprParser();
        IDialectTranslate translate = new PGDialectTranslate();
        ICreateWritable createWritable = new CreateWritable(TMPDIR);
        IAttributeValueParser attributeValueParser = new AttributeValueParser();

        Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(config);
        Node node = document.getFirstChild();
        NcdfDefinition definition = new NcdfDefinitionXMLParser().parse(node);

        return new NcdfEncoder(parser, translate, conn, createWritable, attributeValueParser, definition, filterExpr, outputGenerator, System.out);
    }

    private Connection getConn() throws Exception {
        Map<String, String> env = System.getenv();

        String opts [] = { "POSTGRES_USER", "POSTGRES_PASS", "POSTGRES_JDBC_URL" } ;
        for(String opt : opts) {
            if(env.get(opt) == null)
                throw new Exception("Environment var '" + opt + "' not set");
        }

        Properties props = new Properties();
        props.setProperty("user", env.get("POSTGRES_USER"));
        props.setProperty("password", env.get("POSTGRES_PASS"));
        props.setProperty("ssl","true");
        props.setProperty("sslfactory","org.postgresql.ssl.NonValidatingFactory");
        props.setProperty("driver","org.postgresql.Driver");

        return DriverManager.getConnection(env.get("POSTGRES_JDBC_URL"), props);
    }

    private InputStream getAnmnConfig() {
        return getClass().getResourceAsStream("/anmn_ts.xml");
    }
 


    @Test
    public void testAnmnNrsCtdProfiles() throws Exception {
        String layerConfigDir = getClass().getResource("/").getFile();
        String tmpCreationDir = TMPDIR;
        NcdfGenerator generator = new NcdfGenerator(layerConfigDir, tmpCreationDir);
        OutputStream os = new FileOutputStream(TMPDIR + "/output.zip");
        String cql = "TIME < '2013-6-29T00:40:01Z' ";
        generator.write("anmn_nrs_ctd_profiles", cql, getConn(), os);
    }

    @Test
    public void testSoopSSTTrajectory() throws Exception {
        String layerConfigDir = getClass().getResource("/").getFile();
        String tmpCreationDir = TMPDIR;
        NcdfGenerator generator = new NcdfGenerator(layerConfigDir, tmpCreationDir);
        OutputStream os = new FileOutputStream(TMPDIR + "/output.zip");
        String cql = "TIME >= '2013-6-27T00:35:01Z' AND TIME <= '2013-6-29T00:40:01Z' ";
        generator.write("soop_sst_trajectory", cql, getConn(), os);
    }

     @Test
    public void testAnmnTs() throws Exception {
        String layerConfigDir = getClass().getResource("/").getFile();
        String tmpCreationDir = TMPDIR;
        NcdfGenerator generator = new NcdfGenerator(layerConfigDir, tmpCreationDir);
        OutputStream os = new FileOutputStream(TMPDIR + "/output.zip");
        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' ";
        generator.write("anmn_ts", cql, getConn(), os);
    }

    @Test
    public void testCqlWithValidSpatialTemporalSubset() throws Exception {
        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";
        MockOutputterCounter outputter = new MockOutputterCounter();
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), outputter );
        encoder.write();
        assertEquals(11, outputter.getCount());
    }

    @Test
    public void testCqlWithNoDataInSpatialSubset() throws Exception {
        String cql = "INTERSECTS(geom,POLYGON((163.7841796875 -15.9970703125,163.7841796875 -3.0771484375,173.8037109375 -3.077148437499999,173.8037109375 -15.9970703125,163.7841796875 -15.9970703125))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";
        MockOutputterCounter outputter = new MockOutputterCounter();
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), outputter);
        encoder.write();
        assertEquals(0, outputter.getCount());
    }

    @Test
    public void testCqlWithTemporalExtentOutOfAllowedRange() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.33 -33.09,113.33 -30.98,117.11 -30.98,117.11 -33.09,113.33 -33.09))) AND TIME >= '1949-01-01T23:00:00Z' AND TIME <= '1951-01-01T00:00:00Z'";
        MockOutputterCounter outputter = new MockOutputterCounter();
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), outputter);
        encoder.write();
        assertEquals(0, outputter.getCount());
    }

    @Test(expected = PSQLException.class)
    public void testCqlLongitudeOutsideAllowedRange() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((182 -33.09,113.33 -30.98,117.11 -30.98,117.11 -33.09,113.33 -33.09))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), new MockOutputterCounter());
        encoder.write();
    }

    @Test(expected = PSQLException.class)
    public void testCqlLatitudeOutsideAllowedRange() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.33 -95,113.33 -30.98,117.11 -30.98,117.11 -33.09,113.33 -33.09))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), new MockOutputterCounter());
        encoder.write();
    }

    @Test
    public void testCqlWithFloatEqualityValid() throws Exception {
        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND (NOMINAL_DEPTH = 125.0 OR NOMINAL_DEPTH = 150.0)";
        MockOutputterCounter outputter = new MockOutputterCounter();
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), outputter);
        encoder.write();
        assertEquals(2, outputter.getCount());
    }

    @Test(expected = CQLException.class)
    public void testCqlWithFloatEqualityInvalid() throws Exception {
        // eg. 5000. is not valid cql float
        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND NOMINAL_DEPTH = 5000.";
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), new MockOutputterCounter());
        encoder.write();
    }

    @Test
    public void testCqlWithFloatInequalityValid() throws Exception {
        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND NOMINAL_DEPTH <> 125.0";
        MockOutputterCounter outputter = new MockOutputterCounter();
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), outputter);
        encoder.write();
        assertEquals(10, outputter.getCount());
    }

    @Test
    public void testCqlWithStringEqualityValid() throws Exception {
        // QC flag is represented as a string in the db, so must quote
        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND TEMP_quality_control = '4'";
        
        MockOutputterCounter outputter = new MockOutputterCounter();
        NcdfEncoder encoder = getEncoder(getAnmnConfig(), cql, getConn(), outputter);
        encoder.write();
        assertEquals(10, outputter.getCount());
        // TODO, should check that we only include obs with temp_qc = 4 etc, not just that instances are constrained.
    }
}

