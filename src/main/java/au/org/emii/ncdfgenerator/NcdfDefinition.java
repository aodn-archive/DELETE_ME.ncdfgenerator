
package au.org.emii.ncdfgenerator;

import java.util.List;

class NcdfDefinition
{
	final DataSource dataSource;
	final List< IDimension> dimensions;
	final List< IVariableEncoder> variables;

	NcdfDefinition(
		DataSource dataSource,
		List< IDimension> dimensions,
		List< IVariableEncoder> variables
	) {
		this.dataSource = dataSource;
		this.dimensions = dimensions;
		this.variables = variables;
	}
}


