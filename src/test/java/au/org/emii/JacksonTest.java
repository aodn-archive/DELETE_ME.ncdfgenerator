
package au.org.emii.ncdfgenerator;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Properties;

import java.io.InputStream ;



import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.databind.ObjectMapper;


// import com.fasterxml.jackson.annotate.JsonProperty;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;

/*
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlRootElement;

*/

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty ;


// jackson annotations not recognized...

// https://github.com/FasterXML/jackson-annotations

// http://www.cowtowncoder.com/blog/archives/2011/07/entry_457.html





class Simple 
{
	//public Simple( int x1, int x2)
	public Simple( )
	{
		System.out.println( "my constructor" ); 
	}

    public int x = 1;
    public int y = 2;
}

class Simple2 
{
   final int x, y;

   @JsonCreator
   public Simple2(@JsonProperty("x") int x, @JsonProperty("y") int y) {
      this.x = x;
      this.y = y;
   }
}


public class JacksonTest 
{

	@Before
	public void prepare()
	{
	}

	@Test
	public void test01() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		Simple value = xmlMapper.readValue("<?xml version=\"1.0\"?> <Simple><x>1</x><y>2</y></Simple>", Simple.class);
		assertTrue( value != null ); 
		System.out.println( "**** value " + value   );
	}

	@Test
	public void test02() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		Simple2 value = xmlMapper.readValue("<?xml version=\"1.0\"?> <Simple2><x>1</x><y>2</y></Simple2>", Simple2.class);
		assertTrue( value != null ); 
		System.out.println( "**** value " + value   );
	}

	@Test
	public void test03() throws Exception
	{
		// initialization using xml attributes
		ObjectMapper xmlMapper = new XmlMapper();
		Simple2 value = xmlMapper.readValue("<?xml version=\"1.0\"?> <Simple2 x=\"1\" y=\"2\"/>", Simple2.class);
		assertTrue( value != null ); 
		System.out.println( "**** value " + value + " y is " + value.y  );
	}

}	



