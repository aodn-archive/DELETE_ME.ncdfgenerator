
package au.org.emii.ncdfgenerator;

import ucar.nc2.NetcdfFileWriteable;
import ucar.nc2.Dimension;

// import au.org.emii.ncdfgenerator.DimensionImpl;



import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonSubTypes;
// import com.fasterxml.jackson.annotation.Type;

/*

@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, include=As.WRAPPER_OBJECT)
@JsonSubTypes({
    @Type(name="sub1", value=Sub1.class), 
    @Type(name="sub2", value=Sub2.class)})
public interface MyInt
{
}

@JsonTypeName("sub1")
public Sub1 implements MyInt
{
}

@JsonTypeName("sub2")
public Sub2 implements MyInt
{
}

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")  
@JsonSubTypes({  
    @Type(value = ResourceQueryValue.class, name = "ResourceQueryValue"),  
    @Type(value = NumericQueryValue.class, name= "NumericQueryValue")
    })  

*/

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY,
    property = "type",
	defaultImpl = DimensionImpl.class
	)
@JsonSubTypes({
	 @JsonSubTypes.Type(value = DimensionImpl.class, name = "DimensionImp") 
})

interface IDimension extends IAddValue
{
	public void define( NetcdfFileWriteable writer) ;
	public Dimension getDimension( ) ; // horrible to expose this...
										// can't the caller create the dimension?
	public int getLength();
	public void addValueToBuffer( Object value );
	public String getName();
}
