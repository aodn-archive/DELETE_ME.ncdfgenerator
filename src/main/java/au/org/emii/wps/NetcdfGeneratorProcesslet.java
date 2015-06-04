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



public class NetcdfGeneratorProcesslet implements Processlet {

    @Override
    public void process(ProcessletInputs in, ProcessletOutputs out,
        ProcessletExecutionInfo info) throws ProcessletException
    {   
        try {
            System.out.println( "\n\n***** process here0 whoot" ) ; 
            LiteralInput cqlFilter = (LiteralInput) in.getParameter("CQLFilter");
            System.out.println( "\n\n***** got cqlFilter" ) ; 
            LiteralInput typeName = (LiteralInput) in.getParameter("TypeName");
            System.out.println( "\n\n***** got typeName " ) ; 
            ComplexOutput output = (ComplexOutput) out.getParameter("Result");
            System.out.println( "\n\n***** filter " + cqlFilter  + " typeName " + typeName) ;
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

                encoder.prepare(new ZipFormatter(outputStream));

                while (encoder.writeNext()); 


/*
                try (Connection conn = getConnection()) {

                    NcdfGenerator generator = getGenerator();
                    generator.write(typeName.getValue(), cqlFilter.getValue(), conn, output.getBinaryOutputStream());

                }   
*/
            } catch (Exception e) {

                System.out.println( "\n\n***** problem going to throw" ) ; 

                throw new ProcessletException("Exception thrown generating zip file " + e); 
            }   
        } catch( Exception e) {
            System.out.println( "\n\n***** exception" ) ; 
            throw e ; 
        }   
    }   

    @Override
    public void init() {
        // nothing to initialize

        System.out.println( "\n\n***** init" ) ; 
    }   

    @Override
    public void destroy() {
        // nothing to destroy

        System.out.println( "\n\n***** destroy" ) ; 
    }   

/*
    private NcdfGenerator getGenerator() {
        //TODO: use configured deegree work directory
        return new NcdfGenerator(
            System.getProperty("java.io.tmpdir"),
            System.getProperty("java.io.tmpdir")
       );
   }
*/

/*
   private void _writeTemplateToLayerConfigDir(String typeName) throws IOException {
       ClassLoader loader = this.getClass().getClassLoader();
       String templateFilename = String.format("%s.xml", typeName);
       URL url = loader.getResource(String.format("templates/%s", templateFilename));

       if (url == null) {
           throw new IllegalArgumentException(String.format("Template file not found: %s", templateFilename));
       }

       try (InputStream templateIn = url.openStream();
            OutputStream templateOut = new FileOutputStream(
               //TODO: use configured deegree work directory
               new File(System.getProperty("java.io.tmpdir"), templateFilename),
               false)
       ) { 
           IOUtils.copy(templateIn, templateOut);
       }
   }
*/
   public Connection getConnection() throws SQLException, ClassNotFoundException {
       Class.forName("org.postgresql.Driver");
       String url = "jdbc:postgresql://localhost/harvest";
       Properties props = new Properties();
       props.setProperty("user","anmn_ts");
       props.setProperty("password","anmn_ts");
       return DriverManager.getConnection(url, props);
   }
}

