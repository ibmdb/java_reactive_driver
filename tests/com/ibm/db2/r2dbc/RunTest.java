package com.ibm.db2.r2dbc;

import org.junit.runner.JUnitCore;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import org.junit.runner.Description;

public class RunTest extends RunListener
{
    public void testFailure(Failure failure) 
    {
       System.err.println(failure.getMessage());
    }

    public void testStarted(Description description)
    {
    	System.out.println(description.getDisplayName()+" started");
    }
    
    public void testFinished(Description description)
    {
    	System.out.println(description.getDisplayName()+" finished");
    }
    
	public static void main(String[] args) 
	{
		JUnitCore core = new JUnitCore();
	    core.addListener(new RunTest());
	    core.run(TestSuite.class);
	}
}
