package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class LastnamesDbIngest {

    private final static Logger logger = LoggerFactory.getLogger(LastnamesDbIngest.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection dbConnection = DriverManager.getConnection("jdbc:derby:../../derbydb/lastnames;");
        dbConnection.setAutoCommit(false);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            boolean gotSQLExc = false;
            try {
                logger.info("Shutting database down...");
                DriverManager.getConnection("jdbc:derby:;shutdown=true");
            } catch (SQLException se) {
                if (se.getSQLState().equals("XJ015")) {
                    gotSQLExc = true;
                }
            }
            if (!gotSQLExc) {
                logger.warn("Database did not shut down normally");
            } else {
                logger.info("Database shut down normally");
            }
        }));


        PreparedStatement psInsert = dbConnection.prepareStatement("insert into LASTNAMES values (?)");

        BufferedReader lnbr = new BufferedReader(new FileReader("lastnames.txt"));
        String name;
        int counter = 0;
        while ((name = lnbr.readLine()) != null) {
            if (name.length() < 4)
                continue;

            psInsert.setString(1, name);
            psInsert.addBatch();

            if (++counter % 2000 == 0) {
                psInsert.executeBatch();
                dbConnection.commit();
                logger.debug("Added {} names", counter);
            }
        }
        lnbr.close();

        psInsert.executeBatch();
        dbConnection.commit();
        logger.debug("Added {} names", counter);


    }

}
