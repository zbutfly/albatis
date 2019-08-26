package net.butfly.albatis.cassandra;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import net.butfly.albacore.io.URISpec;
import net.butfly.albatis.Connection;
import net.butfly.albatis.io.Input;
import net.butfly.albatis.io.Rmap;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
        
        try {
        	System.out.println("##");
        	//CassandraConnection oconn=new CassandraConnection(new URISpec("cassandra://10.19.120.97:9042"));
        	Connection oconn = Connection.connect(new URISpec("cassandra://10.19.120.97:9042/test_cassandra"));
        	Input<Rmap> in = oconn.input("Student");
        
			
			System.out.println(oconn);
			System.out.println(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
        
        
        
    }
    

    
}
