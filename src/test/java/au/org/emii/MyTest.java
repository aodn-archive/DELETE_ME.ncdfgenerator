
// package au.org.emii;
package au.org.emii.ncdfgenerator;


import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import au.org.emii.ncdfgenerator.cql.*;

public class MyTest {

    @Before
    public void merge() 
	{

    }

	private IExpression doExprTest( String s ) throws Exception
	{
		IExprParser p =	new ExprParser() ;
		IExpression expr = p.parseExpression( s);
		assertFalse( expr == null );
		System.out.println( "****" );
		expr.accept( new PrettyPrinterVisitor( System.out ) );
		System.out.println( "" );
		return expr;
	}

	@Test
    public void testIntegerLiteral() throws Exception
	{
		IExpression expr = doExprTest( "1234" ) ;
		assertTrue( expr instanceof ExprIntegerLiteral );
    }

	@Test
    public void testStringLiteral() throws Exception
	{

    }

	@Test
    public void testTimestampLiteral() throws Exception
	{
		IExpression expr = doExprTest( " '2015-01-13T23:00:00Z' " ) ;
		// will be an ExprProc nop, due to the way we parse this thing, parsing the quotes separately
		ExprProc expr0 = (ExprProc)expr;
		IExpression expr1 = expr0.children.get( 0 );
		assertTrue( expr1 instanceof ExprTimestampLiteral );
    }

	@Test
    public void testWKT() throws Exception
	{
		IExpression expr = doExprTest( "POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))" );
		assertTrue( expr instanceof ExprWKTLiteral );
    }


	@Test
    public void testSymbol() throws Exception
	{
		IExpression expr = doExprTest( "TIME" ) ;
		assertTrue( expr instanceof ExprSymbol);
    }

	@Test
    public void testPrecedenceBinding01() throws Exception
	{
		IExpression expr = doExprTest( " TIME>='2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z' AND 777= 999" ) ;
		assertTrue( expr instanceof ExprProc);
		ExprProc expr0 = (ExprProc) expr;
		assertTrue( expr0.symbol.equals( "AND" ));
		// TODO check lhs for precendence
    }

	@Test
    public void testPrecedenceBinding02() throws Exception
	{
		IExpression expr = doExprTest( " 123 <= SYM AND 666 > 777 OR 888 = '2015-01-13T23:00:00Z' " );
		assertTrue( expr instanceof ExprProc);
		// TODO check lhs for precendence
    }


	 @Test
    public void testFunction() throws Exception
	{
		IExpression expr = doExprTest( " myname( 123 , sym )  " );
		assertTrue( expr instanceof ExprProc);
	}
	
    @Test
    public void test05() throws Exception
	{
		// example cql filter query expression from 
		// https://github.com/aodn/netcdf-subset-service
		String s = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";
		IExpression expr = doExprTest( s ); 
    }

/*
	syntax...
	
 	select ST_GeomFromText( 'POINT( 1, 1 )' );

	select st_intersects( ST_GeomFromText( 'POINT( 1 1 )' ),  ST_GeomFromText( 'POINT( 1 1 )' ) );
*/

    @Test
    public void test06() throws Exception
	{
		// example cql filter query expression from 
		// https://github.com/aodn/netcdf-subset-service
		String s = "INTERSECTS(geom,POLYGON((113.3349609375 -33.091796875,113.3349609375 -30.982421875,117.1142578125 -30.982421875,117.1142578125 -33.091796875,113.3349609375 -33.091796875))) AND TIME >= '2015-01-13T23:00:00Z' AND TIME <= '2015-04-14T00:00:00Z'";
		IExpression expr = doExprTest( s ); 

/*
		StringBuilder b = new StringBuilder();

		// should use the actual class that's there to create the builder.
		PGDialectSelectionGenerator sg = new PGDialectSelectionGenerator( b );
		// public PGDialectSelectionGenerator( StringBuilder b )
*/

		IDialectTranslate dt = new PGDialectTranslate(); 

		String s2 = dt.process( expr ); 

		System.out.println( "whoot " + s2 ) ; 

	
    }





}




