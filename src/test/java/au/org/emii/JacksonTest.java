
package au.org.emii.ncdfgenerator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import java.io.InputStream ;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector; 






class Simple {


    public int x = 1;
    public int y = 2;
}


public class JacksonTest 
{

	@Before
	public void merge()
	{

		System.out.println( "\n@@@@@\n\n Jackson merge" ); 
	}


	@Test
	public void testIntegerLiteral() throws Exception
	{

		ObjectMapper xmlMapper = new XmlMapper();
		Simple value = xmlMapper.readValue("<?xml version=\"1.0\"?> <Simple><x>1</x><y>2</y></Simple>", Simple.class);


		System.out.println( "**** fuck " + value   );
	}




}	

