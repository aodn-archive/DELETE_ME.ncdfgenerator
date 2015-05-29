package au.org.emii.wps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.IOUtils;

import org.geotools.data.Transaction;
import org.geotools.data.DefaultTransaction;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geoserver.wps.process.RawData;
import org.geoserver.wps.process.RawData;
import org.geoserver.wps.resource.WPSResourceManager;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.DataStoreInfo;
import org.geotools.jdbc.JDBCDataStore;

import au.org.emii.ncdfgenerator.NcdfEncoder;
import au.org.emii.ncdfgenerator.NcdfEncoderBuilder;
import au.org.emii.wps.StreamAdaptor;
import au.org.emii.wps.StreamAdaptorSource;

import javax.servlet.ServletContext;
import org.springframework.beans.factory.annotation.Autowired;



@DescribeProcess(title="NetCDF download", description="Subset and download collection as NetCDF files")
public class NetcdfOutputProcess implements GeoServerProcess {

    private static final Logger logger = LoggerFactory.getLogger(NetcdfOutputProcess.class);
    private final WPSResourceManager resourceManager;
    private final Catalog catalog;
    private final String workingDir;

    @Autowired
    private ServletContext context;

    public NetcdfOutputProcess(WPSResourceManager resourceManager, Catalog catalog, ServletContext context) {

        System.out.println( "\n@@@@@@@@@@@@@@@@@@ NetcdfOutputProcess " + context );

        logger.info("constructor, catalog " + catalog);

        this.resourceManager = resourceManager;
        this.catalog = catalog;
        this.workingDir = getWorkingDir(resourceManager);
    }

    @DescribeResult(name="result", description="Zipped netcdf files", meta={"mimeTypes=application/zip"})

    public RawData execute(
        @DescribeParameter(name="typeName", description="Collection to download")
        String typeName,
        @DescribeParameter(name="cqlFilter", description="CQL Filter to apply")
        String cqlFilter
    ) throws ProcessException {

        System.out.println( "\n@@@@@@@@@@@@@@@@@@ execute - context " + context );

        Transaction transaction = null;
        Connection conn = null;

        try {
            _writeTemplateToWorkingDir(typeName);

            DataStoreInfo dsinfo = catalog.getDataStoreByName("imos", "JNDI_anmn_ts");
            JDBCDataStore store = (JDBCDataStore)dsinfo.getDataStore(null);
            transaction = new DefaultTransaction("handle");
            conn = store.getConnection(transaction);

            NcdfEncoderBuilder encoderBuilder = new NcdfEncoderBuilder();
            encoderBuilder.setLayerConfigDir(workingDir); // TODO should be removed
            encoderBuilder.setTmpCreationDir(workingDir);

            // TODO args as builder methods
            NcdfEncoder encoder = encoderBuilder.create(typeName, cqlFilter, conn);
            StreamAdaptorSource source = new NetcdfAdaptorSource(encoder, transaction, conn);
            InputStream is = new StreamAdaptor(source);

            return new NetcdfData(is);

        } catch (Exception e) {
            if (transaction != null) {
                try {
                    transaction.close();
                } catch (IOException e_) {
                    logger.info("problem closing transaction");
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e_) {
                    logger.info("problem closing connection");
                }
            }
            throw new ProcessException(e);
        }
    }

   private void _writeTemplateToWorkingDir(String typeName) throws IOException {
       ClassLoader loader = this.getClass().getClassLoader();
       String templateFilename = String.format("%s.xml", typeName);
       URL url = loader.getResource(String.format("templates/%s", templateFilename));

       if (url == null) {
           throw new IllegalArgumentException(String.format("Template file not found: %s", templateFilename));
       }

       try (InputStream templateIn = url.openStream();
            OutputStream templateOut = new FileOutputStream(
               new File(workingDir, templateFilename),
               false)
       ) {
           IOUtils.copy(templateIn, templateOut);
       }
   }

   private String getWorkingDir(WPSResourceManager resourceManager) {
       try {
            // Use WPSResourceManager to create a temporary directory that will get cleaned up
            // when the process has finished executing (Hack! Should be a method on the resource manager)
            return resourceManager.getTemporaryResource("").dir().getAbsolutePath();
       } catch (Exception e) {
            logger.info("Exception accessing working directory: \n" + e);
            return System.getProperty("java.io.tmpdir");
       }
   }
}

