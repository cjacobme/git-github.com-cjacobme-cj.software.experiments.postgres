package cj.software.experiments.postgres.delete.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

public class StatementsLoader
{
	private Logger logger = Logger.getLogger(StatementsLoader.class);

	public void loadStatements(
			DataSource pDataSource,
			Class<?> pClass,
			String pFile,
			String... pAddtlFiles) throws SQLException,
			URISyntaxException,
			IOException
	{
		try (Connection lConnection = pDataSource.getConnection())
		{
			this.loadStatements(lConnection, pClass, pFile);
			if (pAddtlFiles != null)
			{
				for (String bFile : pAddtlFiles)
				{
					this.loadStatements(lConnection, pClass, bFile);
				}
			}
		}
	}

	private void loadStatements(Connection pConnection, Class<?> pClass, String pFile)
			throws URISyntaxException,
			IOException,
			SQLException
	{
		this.logger.info(String.format("process %s...", pFile));
		URI lURI = pClass.getResource(pFile).toURI();
		Path lPath = Paths.get(lURI);
		StringBuilder lSB = new StringBuilder();
		try (Statement lStatement = pConnection.createStatement())
		{
			Files.lines(lPath).forEach(pLine ->
			{
				lSB.append(pLine).append(System.lineSeparator());
				if (pLine.endsWith(";"))
				{
					String lToBeExecuted = lSB.toString();
					try
					{
						lStatement.executeUpdate(lToBeExecuted);
					}
					catch (SQLException e)
					{
						throw new RuntimeException(e);
					}
					lSB.setLength(0);
				}
			});
		}

	}
}
