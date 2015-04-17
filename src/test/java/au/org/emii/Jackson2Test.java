
package au.org.emii.ncdfgenerator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import java.util.ArrayList;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;


import au.org.emii.ncdfgenerator.IDimension;
import au.org.emii.ncdfgenerator.DimensionImpl;



// https://github.com/FasterXML/jackson-annotations
// http://www.cowtowncoder.com/blog/archives/2011/07/entry_457.html

/*
   @JsonCreator
   public Simple2(@JsonProperty("x") int x, @JsonProperty("y") int y) {
*/ 

class VariableEncoder 
{
	final String				variableName;
	final ArrayList<DimensionImpl>	dimensions; // change name childDimensions


   @JsonCreator
	public VariableEncoder(
		@JsonProperty("name") String variableName,
		@JsonProperty("dimensions") ArrayList< DimensionImpl> dimensions
	) {
		this.variableName = variableName;
		this.dimensions = dimensions;
	}

}




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

	// what about multiple dimensions...
	// needs to be like this ... 

	// final ArrayList<IDimension>	dimensions; // change name childDimensions
	
	@Test
	public void test03() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		VariableEncoder value = xmlMapper.readValue("<?xml version=\"1.0\"?><VariableEncoder><dimensions> <DimensionImpl name=\"TIME\"/> </dimensions>  </VariableEncoder>", VariableEncoder.class);
		assertTrue( value != null ); 
		System.out.println( "**** VAR ENCODER " + value   );
	}




}	

