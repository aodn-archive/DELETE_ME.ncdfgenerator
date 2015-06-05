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
import org.deegree.services.wps.Processlet;
import org.deegree.services.wps.ProcessletException;
import org.deegree.services.wps.ProcessletExecutionInfo;
import org.deegree.services.wps.ProcessletInputs;
import org.deegree.services.wps.ProcessletOutputs;
import org.deegree.services.wps.input.LiteralInput;
import org.deegree.services.wps.output.ComplexOutput;

import au.org.emii.ncdfgenerator.NcdfEncoder;
import au.org.emii.ncdfgenerator.NcdfEncoderBuilder;

import au.org.emii.ncdfgenerator.NcdfDefinitionXMLParser;
import au.org.emii.ncdfgenerator.NcdfDefinition;
import au.org.emii.ncdfgenerator.ZipFormatter;
// import au.org.emii.ncdfgenerator.NcdfGenerator;

import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Node;

import java.io.StringWriter;
import java.io.PrintWriter;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;



public class NetcdfGeneratorProcesslet implements Processlet {

    @Override
    public void process(ProcessletInputs in, ProcessletOutputs out,
        ProcessletExecutionInfo info) throws ProcessletException
    {   
        try {
            System.out.println( "\n\n***** process()" ) ; 

            LiteralInput cqlFilter = (LiteralInput) in.getParameter("cqlFilter");
            System.out.println( "\n\n***** x got cqlFilter " +  cqlFilter) ; 

            LiteralInput typeName = (LiteralInput) in.getParameter("typeName");
            System.out.println( "\n\n***** x got typeName " + typeName) ; 

            ComplexOutput output = (ComplexOutput) out.getParameter("output");
            System.out.println( "\n\n***** x output " + output ) ;

            try {
                // _writeTemplateToLayerConfigDir(typeName.getValue());

                // TODO decode the definition
                // get the conn

                // load the definition file
                ClassLoader loader = this.getClass().getClassLoader();
                String templateFilename = String.format("%s.xml", "anmn_ts");
                URL url = loader.getResource(String.format("templates/%s", templateFilename));

                InputStream config = url.openStream();
                // NcdfDefinition definition = null;
                // config = new FileInputStream(filePath);
                Document document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(config);
                Node node = document.getFirstChild();
                NcdfDefinition definition = new NcdfDefinitionXMLParser().parse(node);


                Connection conn = getConnection();
                String cqlFilter_ = cqlFilter.getValue();
                String schema = "anmn_ts";
                String workingDir = "/tmp";

                System.out.println("\n\n***** whoot here " ); 


                OutputStream outputStream = output.getBinaryOutputStream(); 

                // create the netcdf encoder
                NcdfEncoderBuilder encoderBuilder = new NcdfEncoderBuilder();

                // what do we need the workingDIr

                encoderBuilder.setTmpCreationDir(workingDir)
                    .setDefinition(definition)
                    .setFilterExpr(cqlFilter_)
                    .setConnection(conn)
                    .setSchema(schema)
                ;

                NcdfEncoder encoder = encoderBuilder.create();

                System.out.println("\n\n***** outputStream is " + outputStream); 

                encoder.prepare(new ZipFormatter(outputStream));

                while (encoder.writeNext()); 

            } catch (Exception e) {

                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                String stacktrace = sw.toString();

                System.out.println( "\n\n***** exception " + e.getMessage() + "\n" + stacktrace  ) ; 

                throw new ProcessletException("Exception thrown generating zip file " + e + stacktrace ); 
            }   
        } catch(Exception e) {

            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            String stacktrace = sw.toString();

            System.out.println( "\n\n***** exception " + e.getMessage() + "\n" + stacktrace  ) ; 

            throw e ; 
        }   
    }   

    @Override
    public void init() {
        // nothing to initialize
        System.out.println( "\n\n***** init()" ) ; 
    }   

    @Override
    public void destroy() {
        // nothing to destroy
        System.out.println( "\n\n***** destroy()" ) ; 
    }   


    public Connection getConnection() throws SQLException, ClassNotFoundException, NamingException, Exception {
        System.out.println( "\n\n***** getConnection()" ) ; 
 

        InitialContext cxt = new InitialContext();
        if ( cxt == null ) {
           throw new Exception("Uh oh -- no context!");
        }
        System.out.println( "got context" + cxt );

        DataSource ds = (DataSource) cxt.lookup( "java:/comp/env/jdbc/harvest_read" );
        if ( ds == null ) {
           throw new Exception("Data source not found!");
        }
        System.out.println( "got ds " + ds );

        return ds.getConnection();
    }
}

