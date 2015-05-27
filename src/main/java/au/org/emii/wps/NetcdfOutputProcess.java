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



// import com.gc.iotools.stream.os.OutputStreamToInputStream;



import org.geoserver.wps.process.RawData;


import java.io.ByteArrayOutputStream; 

import java.io.ByteArrayInputStream;

import java.nio.charset.StandardCharsets;  



import java.io.ByteArrayOutputStream;
import java.io.PrintWriter ;

import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import au.org.emii.ncdfgenerator.NcdfEncoder;
import au.org.emii.ncdfgenerator.NcdfEncoderBuilder;

import au.org.emii.ncdfgenerator.IOutputFormatter; 
import au.org.emii.ncdfgenerator.ZipFormatter; 



/*
    VERY IMPORTANT
        need to be able to abandon everything if user disconnects. 

    threading approach isn't really going to play nice.
*/




class StreamAdaptorSource
{
    NcdfEncoder encoder;
    // OutputStream os;
    // IOutputFormatter outputFormatter;

    StreamAdaptorSource( NcdfEncoder encoder)
    {   
        this.encoder = encoder;
        // os = null;
        // outputFormatter = null;
    }

    void prepare( OutputStream os ) throws Exception 
    {   
        // this.os = os;
        // outputFormatter = new ZipFormatter( os ); 
        // why not pass once to the encoder ?

        encoder.prepare( new ZipFormatter( os ) );
    }

    boolean update() throws Exception
    {
        System.out.println( "update()" ); 
        return encoder.writeNext();
    }

/*
    void close()
    {
        System.out.println( "StreamAdaptorSource close called" ); 
    }
*/
}



class StreamAdaptor extends InputStream
{
    class MyByteArrayOutputStream extends ByteArrayOutputStream
    {   
        byte [] internalBuffer()
        {   
            return buf;
        }
    }

    // System.arraycopy(Object src, int srcPos, Object dest, int destPos, int length)
    StreamAdaptorSource source;
    MyByteArrayOutputStream b;
    int readIdx;

    StreamAdaptor( StreamAdaptorSource source ) throws Exception {
        System.out.println( "My stream constructor " ) ;
        // source should potentially be something else,
        this.source = source;
        this.b = new MyByteArrayOutputStream();
        source.prepare( b ); // might throw...
        readIdx = 0;
     }

    // Instance Methods
    public int available() {
        // hint only
        // return b.size() - readIdx; 

        System.out.println( "available called " ) ; 

        return 8192;
    }

    public void close()
    {
        // call close... on source ?

        System.out.println( "\n@@@@ StreamAdaptor close()" ) ;

        // source.close();
        // source = null;
    }

    public int read() throws IOException {
        // inefficient. but we can rely on sane clients using read(buf,off,len) instead
        byte [] buf = new byte [1];
        if( read( buf, 0, 1) > 0 ) {
            return buf[ 0];
        } else {
            return -1;
        }
    }

    public int read(byte[] dst, int off, int len) throws IOException
    {   
        try { 
            System.out.println( "read() off " + off + " len " + len + " b.size() " + b.size() + " readIdx " + readIdx );

            if( readIdx + len >= b.size() ) { 
        
                // maybe clear the ByteArray internal buffer, by discarding what's already been read
                if( readIdx != 0) {
                    int remainingSize = b.size() - readIdx; 
                    System.out.println( "recentering "+ remainingSize );
                    byte[] remaining = new byte [remainingSize ];
                    System.arraycopy( b.internalBuffer(), readIdx, remaining, 0, remainingSize);
                    b.reset();
                    b.write( remaining, 0, remainingSize);
                    readIdx = 0;
                }   
                // read more
                if( source.update()) { 
                    return read( dst, off, len );
                } else {
                    // none then adjust len to what remains in the buffer
                    len = b.size() - readIdx;
                    if(len == 0) {
                        // should call close() here?
                        System.out.println( "finished... " ); 
                        // call close here? no, because we have an output stream, and there's  
                        // nothing to do
                        return -1; 
                    }   
                }   
            }   
            // write buf and adjust read position
            System.arraycopy( b.internalBuffer(), readIdx, dst, off, len );
            readIdx += len;
            return len;
        } catch( Exception e ) { 
            throw new IOException( e ); 
        }
    }   
}



class MyData implements RawData 
{
    InputStream is; 

    MyData ( InputStream is ) 
    {
        this.is = is;
    }


    public InputStream getInputStream() // throws Exception
    {

        System.out.println( "\n@@@@ MyData getInputStream() " ) ;


        // InputStream stream = new ByteArrayInputStream( "whoot".getBytes(StandardCharsets.UTF_8));
        // return new ByteArrayInputStream( "whoot".getBytes(StandardCharsets.UTF_8));

        return is;
    }

    public String getMimeType()
    {
        return "application/zip";
    }

    public String getFileExtension() {
        return "zip";
    }
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


            _writeTemplateToWorkingDir(typeName);
/*
            NcdfGenerator generator = new NcdfGenerator(workingDir, workingDir);;
            generator.write(typeName, cqlFilter, conn1, out  );
*/

            /*
                VERY IMPORTANT. We can actually get rid of the prepare(). By extracting the 
                MyByteArray thing. and passing it into 

                MyByteArrayStream   a;
                MYAdaptor( a, source) ; 
                MyOutputZipper ...
                // 

            */

            NcdfEncoderBuilder encoderBuilder = new NcdfEncoderBuilder();
            encoderBuilder.setLayerConfigDir(workingDir); // this should be removed...
            encoderBuilder.setTmpCreationDir(workingDir);

//            encoderBuilder.setOutputType(new ZipFormatter());

/*    public void write(String typename, String filterExpr, Connection conn, OutputStream os) throws Exception {
        try {
            NcdfEncoder encoder = encoderBuilder.create(typename, filterExpr, conn, os);
            encoder.write();
*/ 

            // should set these args as builder methods ...
            // or something else

            NcdfEncoder encoder = encoderBuilder.create(typeName, cqlFilter, conn1, null );

            // change name to Adatpr     
            StreamAdaptorSource mysrc = new StreamAdaptorSource(encoder ) ; 

            InputStream mystream = new StreamAdaptor( mysrc ); 
            return new MyData ( mystream) ; 


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

/*
   public Connection getConnection() throws SQLException, ClassNotFoundException {

        System.out.println( "\n******* NetcdfOutputProcess getConnection() " ) ;

       Class.forName("org.postgresql.Driver");
       String url = "jdbc:postgresql://localhost/harvest";
       Properties props = new Properties();
       props.setProperty("user","anmn_ts");
       props.setProperty("password","anmn_ts");
       return DriverManager.getConnection(url, props);
   }
*/

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
