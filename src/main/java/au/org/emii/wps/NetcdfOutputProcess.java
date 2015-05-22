package au.org.emii.wps;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.geotools.process.ProcessException;
import org.geotools.process.factory.DescribeParameter;
import org.geotools.process.factory.DescribeProcess;
import org.geotools.process.factory.DescribeResult;
import org.geoserver.wps.gs.GeoServerProcess;
import org.geoserver.wps.process.FileRawData;
import org.geoserver.wps.process.RawData;
import org.geoserver.wps.resource.WPSResourceManager;

import au.org.emii.ncdfgenerator.NcdfGenerator;


// import org.geoserver.catalog.LayerInfoProperties;
import org.geoserver.catalog.Catalog;
import org.geoserver.catalog.LayerInfo;
import org.geoserver.catalog.DataStoreInfo;
import org.geoserver.catalog.NamespaceInfo;

import org.geotools.feature.NameImpl;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.VirtualTable;




@DescribeProcess(title="NetCDF download", description="Subset and download collection as NetCDF files")
public class NetcdfOutputProcess implements GeoServerProcess {

    private WPSResourceManager resourceManager;
    private String workingDir;


    // there's both a catalog and a service that are injected ...

    /// private ServletContext context;

    ///////////////////
    private Catalog catalog;
/*
    public void setCatalog(Catalog catalog) {

        System.out.println( "\n******* NetcdfOutputProcess **** setCatalog " + catalog ) ;

        this.catalog = catalog;
    }

    public Catalog getCatalog() {
        return catalog;
    }
*/
    ///////////////////

/*
    private LayerInfo getLayerInfo(String workspaceName, String layerName) {
        //return LayerInfoProperties.getLayer(getCatalog(), workspace, layer);


         if (workspaceName != null) {
            NamespaceInfo ns = catalog.getNamespaceByPrefix(workspaceName);
            if (ns == null) {
                throw new RuntimeException("Could not find workspace " + workspaceName);
            }
            String nsURI = ns.getURI();

            return catalog.getLayerByName(new NameImpl(nsURI, layerName));

        }

        // return LayerInfoProperties.getLayer(catalog, workspace, layer);
        return null;
    }
*/

    public NetcdfOutputProcess(WPSResourceManager resourceManager, Catalog catalog ) {

        System.out.println( "\n******* NetcdfOutputProcess constructor 4 " + catalog ) ;

        this.resourceManager = resourceManager;
        this.workingDir = getWorkingDir(resourceManager);

        this.catalog = catalog; 
    }

    @DescribeResult(name="result", description="Zipped netcdf files", meta={"mimeTypes=application/zip"})

    public RawData execute(
        @DescribeParameter(name="typeName", description="Collection to download")
        String typeName,
        @DescribeParameter(name="cqlFilter", description="CQL Filter to apply")
        String cqlFilter
    ) throws ProcessException {
        try {

/*
            String workspaceName = "x" ; 
            String layerName = "y"; 
*/
            // the layer type ...
            // the filter extension uses an existing wms layer... but we want a wps layer...
//            LayerInfo layerInfo = getLayerInfo( "x", "y");

/*
            DataStoreInfo a = catalog.getDataStoreByName( workspaceName, 
                getLayerInfo(workspaceName, layerName).getResource().getStore().getName());
            DataStoreInfo dataStoreInfo = null; 
            JDBCDataStore store = (JDBCDataStore)dataStoreInfo.getDataStore(null);

            22 May 14:16:12 INFO [org.geoserver] - Loaded store 'JNDI_anmn_ts', enabled
            22 May 14:16:12 INFO [org.geoserver] - Loaded data store 'JNDI_anmn_ts'
            22 May 14:16:12 INFO [org.geoserver] - Loaded feature type 'anmn_ts_timeseries_map', enabled
            22 May 14:16:12 INFO [org.geoserver] - Loaded feature type 'JNDI_anmn_ts'

*/

            // returns null if missing...
            DataStoreInfo dsinfo = catalog.getDataStoreByName( "imos", "anmn_ts");
            System.out.println( "\n******* NetcdfOutputProcess dsInfo " + dsinfo ) ;


            dsinfo = catalog.getDataStoreByName( "imos", "JNDI_anmn_ts");
            System.out.println( "\n******* NetcdfOutputProcess dsInfo " + dsinfo ) ;


            //////////////////////


            _writeTemplateToWorkingDir(typeName);

            // Use WPS resource manager to create temporary file so it gets cleaned up
            // when execution is complete
            File output = resourceManager.getTemporaryResource("zip").file();

            try (Connection conn = getConnection()) {
                NcdfGenerator generator = new NcdfGenerator(workingDir, workingDir);;
                generator.write(typeName, cqlFilter, conn, new FileOutputStream(output));
            }

            //Perhaps change ncdfgenerator to return an input stream then 
            //could use StreamRawData (only want to create a file once!)
            return new FileRawData(output, "application/zip", "zip");
        } catch (Exception e) {
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

   public Connection getConnection() throws SQLException, ClassNotFoundException {


        System.out.println( "\n******* NetcdfOutputProcess getConnection() " ) ;

       Class.forName("org.postgresql.Driver");
       String url = "jdbc:postgresql://localhost/harvest";
       Properties props = new Properties();
       props.setProperty("user","anmn_ts");
       props.setProperty("password","anmn_ts");
       return DriverManager.getConnection(url, props);
   }

   private String getWorkingDir(WPSResourceManager resourceManager) {
       try {
           // Use WPSResourceManager to create a temporary directory that will get cleaned up 
           // when the process has finished executing (Hack! Should be a method on the resource manager) 
           return resourceManager.getTemporaryResource("").dir().getAbsolutePath();
       } catch (Exception e) {
           // TODO: Use logger
           System.err.println("Exception accessing working directory: \n" + e);
           return System.getProperty("java.io.tmpdir");
       }
   }
}
