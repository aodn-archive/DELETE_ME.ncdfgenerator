
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



// jackson annotations not recognized...

// https://github.com/FasterXML/jackson-annotations


class CtorPOJO 
{
   private final int _x, _y;

   @JsonCreator
   public CtorPOJO(@JsonProperty("x") int x, @JsonProperty("y") int y) {
      _x = x;
      _y = y;
   }
}



class Simple 
{
	//public Simple( int x1, int x2)
	public Simple( )
	{
		System.out.println( "my constructor" ); 
	}


//	@XmlElement(required = true)
//	@JsonProperty("fuck")
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
	public void test01() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		Simple value = xmlMapper.readValue("<?xml version=\"1.0\"?> <Simple><x>1</x><y>2</y></Simple>", Simple.class);
		System.out.println( "**** fuck " + value   );
	}

	@Test
	public void test02() throws Exception
	{
		ObjectMapper xmlMapper = new XmlMapper();
		CtorPOJO value = xmlMapper.readValue("<?xml version=\"1.0\"?> <CtorPOJO><x>1</x><y>2</y></CtorPOJO>", CtorPOJO.class);
		System.out.println( "**** fuck " + value   );
	}


}	



