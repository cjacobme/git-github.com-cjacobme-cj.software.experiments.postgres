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
	 * weiteren Objekten steht
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
		DataSource lDataSource = pg.getEmbeddedPostgres().getPostgresDatabase();
		this.statementsLoader.loadStatements(
				lDataSource,
				DeleteContractTest.class,
				"/CreateSchema.ddl",
				"/InsertContracts.sql");
		this.statementsLoader.loadComplete(
				lDataSource,
				DeleteContractTest.class,
				"/CreateFunction.sql");
		try (Connection lConnection = lDataSource.getConnection())
		{
			this.assertCounts(lConnection, 2, 0, 0, 0, 0);
			ContractDeleter lContractDeleter = new ContractDeleter();
			long lDelete = lContractDeleter.delete(lConnection, 31);
			assertThat(lDelete).as("number of rows").isEqualTo(1l);
			this.assertCounts(lConnection, 1, 0, 0, 0, 0);
			lContractDeleter.delete(lConnection, 32);
			this.assertCounts(lConnection, 0, 0, 0, 0, 0);
		}
	}

	@Test
	public void deleteContractsAndDeals() throws SQLException, URISyntaxException, IOException
	{
		DataSource lDataSource = pg.getEmbeddedPostgres().getPostgresDatabase();
		this.statementsLoader.loadStatements(
				lDataSource,
				DeleteContractTest.class,
				"/CreateSchema.ddl",
				"/InsertContracts.sql",
				"/InsertDeals.sql");
		this.statementsLoader.loadComplete(
				lDataSource,
				DeleteContractTest.class,
				"/CreateFunction.sql");
		try (Connection lConnection = lDataSource.getConnection())
		{
			this.assertCounts(lConnection, 2, 9, 0, 0, 0);
			ContractDeleter lDeleter = new ContractDeleter();
			long lDeleted = lDeleter.delete(lConnection, 32);
			assertThat(lDeleted).as("number of deleted rows").isEqualTo(6);
			this.assertCounts(lConnection, 1, 4, 0, 0, 0);
			lDeleted = lDeleter.delete(lConnection, 31);
			assertThat(lDeleted).as("number of deleted rows").isEqualTo(5);
			this.assertCounts(lConnection, 0, 0, 0, 0, 0);
		}
	}

	@Test
	public void deleteContractsWithMessages() throws SQLException, URISyntaxException, IOException
	{
		DataSource lDataSource = pg.getEmbeddedPostgres().getPostgresDatabase();
		this.statementsLoader.loadStatements(
				lDataSource,
				DeleteContractTest.class,
				"/CreateSchema.ddl",
				"/InsertContracts.sql",
				"/InsertMsgContracts.sql");
		this.statementsLoader.loadComplete(
				lDataSource,
				DeleteContractTest.class,
				"/CreateFunction.sql");
		try (Connection lConnection = lDataSource.getConnection())
		{
			this.assertCounts(lConnection, 2, 0, 3, 3, 0);
			ContractDeleter lDeleter = new ContractDeleter();

			long lDeleted = lDeleter.delete(lConnection, 31);
			assertThat(lDeleted).as("number of deleted rows").isEqualTo(5);
			this.assertCounts(lConnection, 1, 0, 1, 1, 0);

			lDeleted = lDeleter.delete(lConnection, 32);
			assertThat(lDeleted).as("number of deleted rows").isEqualTo(3);
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
			this.assertCount(lStmt, "contracts", pExpectedContracts);
			this.assertCount(lStmt, "deals", pExpectedDeals);
			this.assertCount(lStmt, "messages", pExpectedMsgs);
			this.assertCount(lStmt, "msgcontracts", pExpectedMsgsForContracts);
			this.assertCount(lStmt, "msgdeals", pExpectedMsgsForDeals);
		}
	}

	private void assertCount(Statement pStmt, String pTableName, long pExcpectedCount)
			throws SQLException
	{
		try (ResultSet lRS = pStmt.executeQuery("select count(*) from " + pTableName))
		{
			assertThat(lRS.next()).as("next for %s", pTableName).isTrue();
			long lActual = lRS.getLong(1);
			assertThat(lActual).as("number of rows in %s", pTableName).isEqualTo(pExcpectedCount);
		}
	}
}
