
package au.org.emii.ncdfgenerator;

import au.org.emii.ncdfgenerator.cql.IExpression;
import au.org.emii.ncdfgenerator.cql.IExprParser;
import au.org.emii.ncdfgenerator.cql.IDialectTranslate;


import java.util.Map;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;

import org.w3c.dom.Document;

import ucar.nc2.NetcdfFileWriteable;


class NcdfEncoder
{
	final IExprParser exprParser;
	final IDialectTranslate translate ;
	final Connection conn;
	final ICreateWritable createWritable;
	final NcdfDefinition definition ;
	final String filterExpr;

	final int fetchSize;

	IExpression selectionExpr;
	String selectionSql;
	ResultSet featureInstancesRS;

	public NcdfEncoder(
		IExprParser exprParser,
		IDialectTranslate translate,
		Connection conn,
		ICreateWritable createWritable,
		NcdfDefinition definition,
		String filterExpr
	) {
		this.exprParser = exprParser;
		this.translate = translate;
		this.conn = conn;
		this.createWritable = createWritable;
		this.definition = definition;
		this.filterExpr = filterExpr;

		fetchSize = 1000;
		featureInstancesRS = null;
		selectionExpr = null;
		selectionSql = null;
	}

	public void prepare() throws Exception
	{
		System.out.println( "encoder prepare " + filterExpr );
		System.out.println( "done parsing expression" );
		System.out.println( "setting search_path to " + definition.schema );

		// do not quote search path!.
		PreparedStatement s = conn.prepareStatement( "set search_path=" + definition.schema + ", public");
		s.execute();
		s.close();

		selectionExpr = exprParser.parseExpression( filterExpr );
		selectionSql = translate.process( selectionExpr);

		// ok, so if we combine both tables in a join, then it's actually simpler,
		// we always have to hit both queries in the initial and instance selection queries

		String query =
			"select distinct data.instance_id" +
			" from (" + definition.virtualDataTable + ") as data" +
			" left join (" + definition.virtualInstanceTable + ") instance" +
			" on instance.id = data.instance_id" +
			" where " + selectionSql + ";" ;

		System.out.println( "first query " + query  );

		PreparedStatement stmt = conn.prepareStatement( query );
		stmt.setFetchSize(fetchSize);

		// change name featureInstancesRSToProcess ?
		featureInstancesRS = stmt.executeQuery();

		System.out.println( "****** done determining feature instances " );
	}


	public NetcdfFileWriteable get() throws Exception
	{
		// TODO should just return a readable IStream, client shouldn't care that it's netcdf type.

		try {
			if( featureInstancesRS.next())
			{
				// munge
				long instanceId = -1234;
				Object o = featureInstancesRS.getObject(1);
				Class clazz = o.getClass();
				if( clazz.equals( Integer.class )) {
					instanceId = (long)(Integer)o;
				}
				else if( clazz.equals( Long.class )) {
					instanceId = (long)(Long)o;
				} else {
					throw new NcdfGeneratorException( "Can't convert intance_id type to integer" );
				}

				System.out.println( "instanceId is " + instanceId );

				String orderClause = "";
				for( IDimension dimension : definition.dimensions.values() )
				{
					if( !orderClause.equals("")){
						orderClause += ",";
					}
					orderClause += "\"" + dimension.getName() + "\"" ;
				}

				String query =
					"select *" +
					" from (" + definition.virtualDataTable + ") as data" +
					" left join (" + definition.virtualInstanceTable + ") instance" +
					" on instance.id = data.instance_id" +
					" where " + selectionSql +
					" and data.instance_id = " + Long.toString( instanceId) +
					" order by " + orderClause +
					";" ;

				System.out.println( "*****\nsecond query " + query );

				populateValues( query, definition.dimensions, definition.encoders );

				NetcdfFileWriteable writer = createWritable.create();


				for ( IDimension dimension: definition.dimensions.values()) {
					dimension.define(writer);
				}

				for ( IVariableEncoder encoder: definition.encoders.values()) {
					encoder.define( writer );
				}
				// finish netcdf definition
				writer.create();

				for ( IVariableEncoder encoder: definition.encoders.values()) {
					// maybe change name writeValues
					encoder.finish( writer );
				}
				// write the file
				writer.close();

				// TODO we should be returning a filestream here...
				// the caller doesn't care that it's a netcdf
				return writer;
			}
			else {
				// no more netcdfs
				conn.close();
				return null;
			}
		} catch ( Exception e ) {
			System.out.println( "Opps " + e.getMessage() );
			conn.close();
			return null;
		}
	}

	public void populateValues(
		String query,
		Map< String, IDimension> dimensions,
		Map< String, IVariableEncoder> encoders
		)  throws Exception
	{
		// System.out.println( "query " + query  );

		// sql stuff
		PreparedStatement stmt = conn.prepareStatement( query );
		stmt.setFetchSize(fetchSize);
		ResultSet rs = stmt.executeQuery();

		// now we loop the main attributes
		ResultSetMetaData m = rs.getMetaData();
		int numColumns = m.getColumnCount();

		// pre-map the encoders by index according to the column name
		ArrayList< IAddValue> [] processing = (ArrayList< IAddValue> []) new ArrayList [numColumns + 1];

		for ( int i = 1 ; i <= numColumns ; i++ ) {

			processing[i] = new ArrayList< IAddValue> ();

			IDimension dimension = dimensions.get( m.getColumnName(i));
			if( dimension != null)
				processing[i].add( dimension );

			IAddValue encoder = encoders.get(m.getColumnName(i));
			if( encoder != null)
				processing[i].add( encoder );
		}

		// process result set rows
		while ( rs.next() ) {
			for ( int i = 1 ; i <= numColumns ; i++ ) {
				for( IAddValue p : processing[ i] ) {
					p.addValueToBuffer( rs.getObject( i));
				}
			}
		}
	}


}


