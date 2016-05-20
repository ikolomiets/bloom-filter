package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

public class DbNameSuggest {

    private final static Logger logger = LoggerFactory.getLogger(DbNameSuggest.class);

    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
        Connection connection = DriverManager.getConnection("jdbc:derby:../../derbydb/lastnames;");
        connection.setAutoCommit(false);

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

        
        NameLookup nameLookup = new DbNameLookup(connection);

        logger.debug("Ready to suggest. Enter the prefix...");

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.length() > 3) {
                long start = System.currentTimeMillis();
                List<String> suggestions = nameLookup.lookup(line, 10);
                long elapsed = System.currentTimeMillis() - start;
                logger.debug("Found {} suggestions in {}ms", suggestions.size(), elapsed);
                for (String suggestion : suggestions) {
                    System.out.println("> " + suggestion);
                }
            } else {
                System.out.println("> " + line + " is too short");
            }
        }
    }



}
