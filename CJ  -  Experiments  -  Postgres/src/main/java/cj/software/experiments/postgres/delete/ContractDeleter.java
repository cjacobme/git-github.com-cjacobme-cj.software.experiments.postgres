package cj.software.experiments.postgres.delete;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

public class ContractDeleter
{
	public long delete(Connection pConnection, long pContractId) throws SQLException
	{
		try (CallableStatement lStmt = pConnection.prepareCall("{ ? = call delete_contract( ? ) }"))
		{
			lStmt.registerOutParameter(1, Types.BIGINT);
			lStmt.setLong(2, pContractId);
			lStmt.execute();
			long lResult = lStmt.getLong(1);
			return lResult;
		}
	}
}
