package com.ibm.db2.r2dbc;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({
		ConnectionTests.class, 
		DDLTests.class,
		PreparedStatementTests.class,
		TransactionIsolationTests.class,
		SmallIntTypeTests.class,
		IntegerTypeTests.class,
		BigIntTypeTests.class,
		CharTypeTests.class,
		CharEbcdicTypeTests.class,
		CharAsciiTypeTests.class,
		CharUnicodeTypeTests.class,
		VarCharTypeTests.class,
		VarCharEbcdicTypeTests.class,
		VarCharAsciiTypeTests.class,
		VarCharUnicodeTypeTests.class,
		DateTypeTests.class,
		TimeTypeTests.class,
		TimestampTypeTests.class,
		DecimalTypeTests.class,
		RealTypeTests.class,
		DoubleTypeTests.class,
		DecFloat16TypeTests.class,
		DecFloat34TypeTests.class,
		CLOBTests.class,
		CLOBUnicodeTests.class, 
		BLOBTests.class,
		NamedParameterMarkerTests.class,
		DataTransformationTests.class
	})
public class TestSuite
{
	
}