
package au.org.emii.ncdfgenerator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;


import au.org.emii.ncdfgenerator.DimensionImpl;



// https://github.com/FasterXML/jackson-annotations
// http://www.cowtowncoder.com/blog/archives/2011/07/entry_457.html



public class Jackson2Test
{
	@Before
	public void prepare()
	{ }

	@Test
	public void test01() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		IDimension value = xmlMapper.readValue("<?xml version=\"1.0\"?> <DimensionImpl><name>TIME</name></DimensionImpl>", DimensionImpl.class);
		assertTrue( value != null ); 
		System.out.println( "**** DIMESNION " + value   );
	}

	@Test
	public void test02() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		IDimension value = xmlMapper.readValue("<?xml version=\"1.0\"?> <DimensionImpl name=\"TIME\"/>", DimensionImpl.class);
		assertTrue( value != null ); 
		System.out.println( "**** DIMESNION " + value   );
	}

}	

