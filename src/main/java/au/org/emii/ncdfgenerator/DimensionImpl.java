
package au.org.emii.ncdfgenerator;

import ucar.nc2.Dimension;
import ucar.nc2.NetcdfFileWriteable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeName;
// import com.fasterxml.jackson.annotation.JsonDeserialize;

// @JsonDeserialize(as = IDimension.class)


@JsonTypeName("DimensionImpl")
@JsonIgnoreProperties({ "name", "size", "dimension" })
class DimensionImpl implements IDimension
{

	 final String name;
	 int size;
	 Dimension dimension;

	@JsonCreator
	public DimensionImpl( @JsonProperty("name") String name )
	{
		this.name = name; // required to encode dimension
		this.size = 0;
		this.dimension = null;
	}

	public Dimension getDimension( )  // bad naming
	{
		// throw if not defined...
		return dimension;
	}

	public int getLength()
	{
		return size;
	}

	public void define( NetcdfFileWriteable writer)
	{
		dimension = writer.addDimension( name, size );
	}

	public void addValueToBuffer( Object value )
	{
		++size;
	}

	public String getName()
	{
		return name ;

	}
}

