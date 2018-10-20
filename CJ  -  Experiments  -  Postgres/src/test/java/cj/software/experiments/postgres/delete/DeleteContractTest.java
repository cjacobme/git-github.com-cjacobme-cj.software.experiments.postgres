package cj.software.experiments.postgres.delete;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Test;

import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.SingleInstancePostgresRule;

/**
 * hier möchten wir testen, wie man Daten zum Teil mit temporären Tabellen löscht.
 */
public class DeleteContractTest
{
	private Logger logger = Logger.getLogger(DeleteContractTest.class);

	@ClassRule
	public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

	@Test
	public void connect() throws SQLException
	{
		DataSource lPostgresDatabase = pg.getEmbeddedPostgres().getPostgresDatabase();
		try (Connection pConnection = lPostgresDatabase.getConnection())
		{
			this.logger.info("connected");
			Statement lStmt = pConnection.createStatement();
			ResultSet lRS = lStmt.executeQuery("select 1");
			assertThat(lRS.next()).as("ResultSet").isTrue();
			long lReturned = lRS.getLong(1);
			assertThat(lReturned).as("returned").isEqualTo(1l);
		}
	}

	@Test
	public void deleteSingleContract()
	{

	}
}
