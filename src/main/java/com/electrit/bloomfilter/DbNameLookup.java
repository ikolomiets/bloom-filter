package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DbNameLookup implements NameLookup {

    private final static Logger logger = LoggerFactory.getLogger(DbNameLookup.class);

    private final PreparedStatement psSelect;

    public DbNameLookup(Connection connection) throws SQLException {
        psSelect = connection.prepareStatement("SELECT NAME FROM LASTNAMES WHERE NAME LIKE ? ORDER BY NAME",
                ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
    }

    private final static class PrefixWords {
        private final String prefix;
        private final List<PrefixWords> children = new ArrayList<>();

        public PrefixWords(String prefix) {
            this.prefix = prefix;
        }

        public int size() {
            int size = 1;
            for (PrefixWords child : children)
                size += child.size();
            return size;
        }

        public int words() {
            int words = prefix.endsWith(".") ? 1 : 0;
            for (PrefixWords child : children)
                words += child.words();
            return words;
        }

        public List<String> getWordsAndPrefixes(int max) {
            List<String> result = new ArrayList<>();
            if (prefix.endsWith(".")) {
                result.add(prefix);
            }

            Iterator<PrefixWords> iterator = children.iterator();
            while (iterator.hasNext()) {
                PrefixWords next = iterator.next();
                if (next.prefix.endsWith(".") && next.children.isEmpty()) {
                    result.add(next.prefix);
                    iterator.remove();
                }
            }
            
            while (result.size() < max && !children.isEmpty()) {
                iterator = children.iterator();
                PrefixWords minPrefixWords = null;
                int minWords = 0;
                while (iterator.hasNext()) {
                    PrefixWords next = iterator.next();
                    int words = next.words();
                    if (minPrefixWords == null) {
                        minPrefixWords = next;
                        minWords = words;
                    } else if (words < minWords) {
                        minPrefixWords = next;
                        minWords = words;
                    }
                }
                
                // todo
            }

            return result;
        }
        
        private PrefixWords pullOnlyChildUp() {
            if (!prefix.endsWith(".") && children.size() == 1) {
                PrefixWords onlyChild = children.get(0);
                PrefixWords next = onlyChild.pullOnlyChildUp();
                return next != null ? next : onlyChild;
            }
            return null;
        }

        public void compact() {
            for (int i = 0, childrenSize = children.size(); i < childrenSize; i++) {
                PrefixWords child = children.get(i);
                child.compact();
                PrefixWords onlyChild = child.pullOnlyChildUp();
                if (onlyChild != null) {
                    children.set(i, onlyChild);
                }
            }
        }

        @Override
        public String toString() {
            return toString("");
        }

        private String toString(String indent) {
            StringBuilder sb = new StringBuilder(indent).append(prefix);
            String nextIndent = indent + ".";
            for (PrefixWords child : children) {
                sb.append("\n").append(indent).append(child.toString(nextIndent));
            }
            return sb.toString();
        }
    }

    @Override
    public List<String> lookup(String prefix, int max) {
        PrefixWords prefixWords;
        try {
            psSelect.setString(1, prefix + "%");
            prefixWords = groupByAbc(psSelect.executeQuery(), prefix);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to execute db query for " + prefix, e);
        }

        if (prefixWords == null) {
            logger.warn("prefixWords is null");
            return new ArrayList<>();
        }

        prefixWords.compact();
        logger.debug("prefixWords size={}, words={}:\n{}", prefixWords.size(), prefixWords.words(), prefixWords);

        return prefixWords.getWordsAndPrefixes(max);
    }

    private static PrefixWords groupByAbc(ResultSet resultSet, String prefix) throws SQLException {
        if (!resultSet.next())
            return null;

        String name = resultSet.getString(1);
        PrefixWords result;
        if (name.equals(prefix)) {
            result = new PrefixWords(prefix + ".");
            name = null;
        } else {
            result = new PrefixWords(prefix);
        }

        for (char extension = 'a'; extension <= 'z'; extension++) {
            if (name == null) {
                if (!resultSet.next())
                    break;
                name = resultSet.getString(1);
            }

            if (name.startsWith(prefix)) {
                String extendedPrefix = prefix + extension;
                if (name.startsWith(extendedPrefix)) {
                    resultSet.previous();
                    PrefixWords extPrefixWords = groupByAbc(resultSet, extendedPrefix);
                    if (extPrefixWords != null)
                        result.children.add(extPrefixWords);
                    name = null;
                }
            } else {
                resultSet.previous();
                break;
            }
        }

        return result;
    }

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
        List<String> result = nameLookup.lookup("kolomi", 10);
        for (String s : result) {
            logger.debug("> {}", s);
        }

    }

}
