package cj.software.experiments.postgres.delete;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
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

import cj.software.experiments.postgres.delete.util.StatementsLoader;

/**
 * hier möchten wir testen, wie man Daten zum Teil mit temporären Tabellen löscht. Das Datenmodell:
 * <ul>
 * <li>Contract ist die Wurzel von allem</li>
 * <li>zu einem Contract gehören Deals (N-zu-1-Relation Deal->Contract)</li>
 * <li>Es gibt eine Basistabelle Messages.</li>
 * <li>Es gibt Tabelle MsgContract mit N-zu-1-Relationen zu Messages und Contract.</li>
 * <li>Analog MsgDeal</li>
 * </ul>
 * Aufgabenstellung ist eine Postgres-Funktion, die einen gesamten Objektgrafen ausgehend von einem
 * Contract löscht.
 */
public class DeleteContractTest
{
	private Logger logger = Logger.getLogger(DeleteContractTest.class);

	@ClassRule
	public static SingleInstancePostgresRule pg = EmbeddedPostgresRules.singleInstance();

	StatementsLoader statementsLoader = new StatementsLoader();

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

	/**
	 * erster Schritt: wir können einen einzelnen Contract löschen, der ohne jede Beziehung zu
	 * weiteren
	 * 
	 * @throws SQLException
	 *             Fehler bei SQL-Zugriff
	 * @throws URISyntaxException
	 *             Falscher Aufbau Dateiname
	 * @throws IOException
	 *             Fehler beim Lesen einer Datei
	 */
	@Test
	public void deleteSingleContract() throws SQLException, URISyntaxException, IOException
	{
		this.statementsLoader.loadStatements(
				pg.getEmbeddedPostgres().getPostgresDatabase(),
				DeleteContractTest.class,
				"/CreateSchema.ddl",
				"/InsertContracts.sql");
		try (Connection lConnection = pg
				.getEmbeddedPostgres()
				.getPostgresDatabase()
				.getConnection())
		{
			this.assertCounts(lConnection, 2, 0, 0, 0, 0);
			ContractDeleter lContractDeleter = new ContractDeleter();
			lContractDeleter.delete(lConnection, 1);
			this.assertCounts(lConnection, 0, 0, 0, 0, 0);
			lContractDeleter.delete(lConnection, 2);
			this.assertCounts(lConnection, 0, 0, 0, 0, 0);
		}
	}

	private void assertCounts(
			Connection pConnection,
			long pExpectedContracts,
			long pExpectedDeals,
			long pExpectedMsgs,
			long pExpectedMsgsForContracts,
			long pExpectedMsgsForDeals) throws SQLException
	{
		try (Statement lStmt = pConnection.createStatement())
		{
			try (ResultSet lRS = lStmt.executeQuery("select count(*) from contracts"))
			{
				this.assertCount(lRS, pExpectedContracts, "contracts");
			}
			try (ResultSet lRS = lStmt.executeQuery("select count(*) from deals"))
			{
				this.assertCount(lRS, pExpectedDeals, "Deals");
			}
			try (ResultSet lRS = lStmt.executeQuery("select count(*) from messages"))
			{
				this.assertCount(lRS, pExpectedMsgs, "messages");
			}
			try (ResultSet lRS = lStmt.executeQuery("select count(*) from msgcontracts"))
			{
				this.assertCount(lRS, pExpectedDeals, "messages for contracts");
			}
			try (ResultSet lRS = lStmt.executeQuery("select count(*) from msgdeals"))
			{
				this.assertCount(lRS, pExpectedDeals, "messages for deals");
			}
		}
	}

	private void assertCount(ResultSet pRS, long pExpected, String pName) throws SQLException
	{
		assertThat(pRS.next()).as("resultset next").isTrue();
		long lActual = pRS.getLong(1);
		assertThat(lActual).as("number of rows in %s", pName).isEqualTo(pExpected);
	}
}
