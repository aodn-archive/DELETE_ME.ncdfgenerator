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

import org.geotools.data.Transaction;
import org.geotools.data.DefaultTransaction;



import com.gc.iotools.stream.os.OutputStreamToInputStream;



import org.geoserver.wps.process.RawData;


class MyData implements RawData 
{

    /**
     * Returns the mime type of the stream's contents
     * 
     * @return
     */
    public String getMimeType()
    {
        return "xxx";
    }

    /**
     * Gives access to the raw data contents. TODO: decide if this one may be called only once, or
     * if the code should make it possible to extract the stream multiple times
     * 
     * @return
     * @throws FileNotFoundException
     */
    public InputStream getInputStream() throws IOException
    {
        return null;
    }

    /**
     * Optional field for output raw data, used by WPS to generate a file extension
     * 
     * @return
     */
    public String getFileExtension()
    {
        return "zip";
    }

}

/*
// implements or extends 
class MyStream extends InputStream 
// class MyStream extenimplements InputStream,    Closeable, AutoCloseable  
{
    int     available() { } 
    // Returns an estimate of the number of bytes that can be read (or skipped over) from this input stream without blocking by the next invocation of a method for this input stream.
    void    close() { } 
    // Closes this input stream and releases any system resources associated with the stream.
    void    mark(int readlimit) { } 
    // Marks the current position in this input stream.
    boolean     markSupported() { } 


    // Tests if this input stream supports the mark and reset methods.
    //abstract int    read() { } 

    // Reads the next byte of data from the input stream.
    int     read(byte[] b) { } 
    // Reads some number of bytes from the input stream and stores them into the buffer array b.
    int     read(byte[] b, int off, int len) { } 
    // Reads up to len bytes of data from the input stream into an array of bytes.
    void    reset() { } 
    // Repositions this stream to the position at the time the mark method was last called on this input stream.
    long    skip(long n) { } 
    // Skips over and discards n bytes of data from this input stream.
};
*/

// public abstract class java.io.InputStream extends java.lang.Object {
class MyStream extends java.io.InputStream { //extends java.lang.Object {
  // Instance Methods
  public int available() { return 1000; } ;
  public void close() { } ;
  public synchronized void mark(int readlimit) { } 
  public boolean markSupported() { return false; } 
  // public abstract int read() { } 
  public int read() { return 0; } 
  public int read(byte[] b) { return 0; } 
  public int read(byte[] b, int off, int len) { return 0; } 
  public synchronized void reset() { } 
  public long skip(long n) { return 0; } 
}



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

        Transaction t  = null;
        Connection conn1 = null;

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

            /*
                Need to understand 
                    - what the geotools transaction class is doing.
                    - whether the schema is set  
                    - what all the Connection wrapping classes do . 
            */
            DataStoreInfo dsinfo = catalog.getDataStoreByName( "imos", "JNDI_anmn_ts");
            System.out.println( "\n******* NetcdfOutputProcess dsInfo " + dsinfo ) ;


            JDBCDataStore store = (JDBCDataStore)dsinfo.getDataStore(null);
            System.out.println( "\n******* store " + store ) ;


            t = new DefaultTransaction("handle");
            System.out.println( "\n******* transaction " + store ) ;


            // returns a conn ProxyConnection[PooledConnection[org.postgresql.jdbc3.Jdbc3Connection@1d002268]]
            conn1 = store.getConnection( t ); 
            System.out.println( "\n******* conn " + conn1 ) ;


            // is the schema set in the connection?  it should be...

            // createConnection() is protected ... 
            // Connection conn1 = store.createConnection(); 


            //////////////////////


            final OutputStreamToInputStream<Void> out = new OutputStreamToInputStream<Void>() {
                @Override
                protected Void doRead(final InputStream istream) throws Exception {
                      /*
                       * Read the data from the InputStream "istream" passed as parameter. 
                       */

                        System.out.println( "doRead " + istream ); 

                       // LibraryClass2.processDataFromInputStream(in);
                       return null;
                }
            };
/*
            try {   
                 LibraryClass1.writeDataToTheOutputStream(out);
            } finally {
                 // don't miss the close (or a thread would not terminate correctly).
                 out.close();
            }
*/
            _writeTemplateToWorkingDir(typeName);

            // Use WPS resource manager to create temporary file so it gets cleaned up
            // when execution is complete
//            File output = resourceManager.getTemporaryResource("zip").file();

//            try (Connection conn = getConnection()) {
                NcdfGenerator generator = new NcdfGenerator(workingDir, workingDir);;
                generator.write(typeName, cqlFilter, conn1, out /*new FileOutputStream(output) */ );
 //           }

            //Perhaps change ncdfgenerator to return an input stream then 
            //could use StreamRawData (only want to create a file once!)
//            return new FileRawData(output, "application/zip", "zip");

            return new MyData () ; 


        } catch (Exception e) {

            System.out.println( "\n my exception " + e.getMessage()  );

            throw new ProcessException(e);
        }
        finally { 
            // these can throw ??
            try { 
                if( t != null) 
                    t.close(); // throws IOException
            } catch( IOException e ) { } 

            try { 
                if( conn1 != null) 
                    conn1.close(); // throws SQLException
            } catch( SQLException e ) { } 
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
