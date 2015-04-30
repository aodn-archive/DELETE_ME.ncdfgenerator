
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


public class GenerationIT {

    public InputStream config;
    public Connection testDatabaseConnection;

    public static Connection getConn() throws Exception {
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

    @Before
    public void setup() throws Exception {

        config = getClass().getResourceAsStream("/anmn_timeseries_gg.xml");
        testDatabaseConnection = getConn();
        new File("./tmp").mkdirs();
    }

    private void streamData(INcdfEncoder encoder) throws Exception {
        InputStream writer;
        do {
            // should try and get lots...
            writer = encoder.get();
        } while(writer != null);
    }


    @Test
    public void anmn_timeseries_IT() throws Exception {

        InputStream config = getClass().getResourceAsStream("/anmn_timeseries.xml");

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    @Test
    public void anmn_nrs_ctd_profiles_IT() throws Exception {

        // exception handling needs to be improved a lot...

        InputStream config = getClass().getResourceAsStream("/anmn_nrs_ctd_profiles.xml");
        String cql = "TIME < '2013-6-29T00:40:01Z' ";
        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);

        streamData(encoder);
    }

    @Test
    public void soop_sst_trajectory_IT() throws Exception {

        InputStream config = getClass().getResourceAsStream("/soop_sst_trajectory.xml");

        String cql = "TIME >= '2013-6-27T00:35:01Z' AND TIME <= '2013-6-29T00:40:01Z' ";
        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);

        streamData(encoder);
    }


    // test zip streaming of data using builder...
    @Test
    public void anmn_timeseries2_IT() throws Exception {
        InputStream config = getClass().getResourceAsStream("/anmn_timeseries.xml");

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        INcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        ZipCreator zipCreator = new ZipCreator(encoder);

        OutputStream os = new FileOutputStream("./myoutput2.zip");
        zipCreator.doStreaming(os);
        os.close();
    }


    // test zip streaming using NcdfGenerator
    @Test
    public void ncdfGenerator_IT() throws Exception {
        String layerConfigDir = "./src/test/resources/"; // TODO URL url = getClass().getResource("/")  ; url.toString()...
        String tmpCreationDir = "./tmp";
        NcdfGenerator generator = new NcdfGenerator(layerConfigDir, tmpCreationDir);

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        OutputStream os = new FileOutputStream("./tmp/output.zip");

        generator.write("anmn_timeseries", cql, testDatabaseConnection, os);
    }


    @Test
    public void cql_with_valid_spatial_temporal_subset() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    /* TODO: catch empty downloads */
    @Test
    public void cql_with_no_data_in_spatial_subset() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((163.7841796875 -15.9970703125,163.7841796875 -3.0771484375,173.8037109375 -3.077148437499999,173.8037109375 -15.9970703125,163.7841796875 -15.9970703125))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    /* TODO: catch empty downloads */
    @Test
    public void cql_with_temporal_extent_out_of_allowed_range() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.33 -33.09,113.33 -30.98,117.11 -30.98,117.11 -33.09,113.33 -33.09))) AND TIME >= '1949-01-01T23:00:00Z' AND TIME <= '1951-01-01T00:00:00Z'";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    /* TODO: handle longitude outside of range earlier than PSQL exception */
    @Test(expected = PSQLException.class)
    public void cql_longitude_outside_allowed_range() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((182 -33.09,113.33 -30.98,117.11 -30.98,117.11 -33.09,113.33 -33.09))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    /* TODO: handle longitude outside of range earlier than PSQL exception */
    @Test(expected = PSQLException.class)
    public void cql_latitude_outside_allowed_range() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.33 -95,113.33 -30.98,117.11 -30.98,117.11 -33.09,113.33 -33.09))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    @Test
    public void cql_with_float_equality_valid() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND NOMINAL_DEPTH = 5000.50";


        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

    @Test(expected = CQLException.class)
    public void cql_with_float_equality_invalid() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND NOMINAL_DEPTH = 5000.";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }


    /* TODO: Implement not parsing for floats and ints */
    @Test
    public void cql_with_float_not_statement_valid() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND NOMINAL_DEPTH <> 5000.50";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }


    /* TODO: having problems parsing ints (or bytes) */
    @Test
    public void cql_with_int_equality_valid() throws Exception {

        String cql = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND TEMP_quality_control = 5";

        NcdfEncoder encoder = new NcdfEncoderBuilder().create(config, cql, testDatabaseConnection);
        streamData(encoder);
    }

}

