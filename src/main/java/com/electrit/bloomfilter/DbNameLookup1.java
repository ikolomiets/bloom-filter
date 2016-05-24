package com.electrit.bloomfilter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public class DbNameLookup1 implements NameLookup {

    private final static Logger logger = LoggerFactory.getLogger(DbNameLookup1.class);

    private final PreparedStatement psSelect;

    public DbNameLookup1(Connection connection) throws SQLException {
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

        public List<String> getWordsAndPrefixes(int max) {
            List<String> result = new ArrayList<>();
            result.add(prefix);

            Iterator<PrefixWords> iterator = children.iterator();
            while (iterator.hasNext()) {
                PrefixWords next = iterator.next();
                if (next.size() == 1) {
                    result.add(next.prefix);
                    iterator.remove();
                }
            }

            // todo

            return result;
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
        PrefixWords prefixWords = new PrefixWords(prefix);
        try {
            psSelect.setString(1, prefix + "%");
            groupByPrefix(psSelect.executeQuery(), prefixWords);
        } catch (SQLException e) {
            throw new RuntimeException("Unable to execute db query for " + prefix, e);
        }

        logger.debug("prefixWords size={}:\n{}", prefixWords.size(), prefixWords);

        return Collections.emptyList();
        //return prefixWords.getWordsAndPrefixes(max);
    }

    private static void groupByPrefix(ResultSet resultSet, PrefixWords parent) throws SQLException {
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            if (name.startsWith(parent.prefix)) {
                PrefixWords child = new PrefixWords(name);
                groupByPrefix(resultSet, child);

                if (!parent.children.isEmpty()) {
                    PrefixWords last = parent.children.get(parent.children.size() - 1);

                    // if last and current share the common prefix longer than parent's - extract it and re-group
                    String common = commonPrefix(last.prefix, name);
                }
                parent.children.add(child);
            } else {
                resultSet.previous();
                return;
            }
        }
    }

    private static String commonPrefix(String first, String second) {
        for (int i = 0; i < Math.min(first.length(), second.length()); i++)
            if (first.charAt(i) != second.charAt(i))
                if (i > 0)
                    return first.substring(0, i);
                else
                    return null;
        return first.substring(0, Math.min(first.length(), second.length()));
    }

    public static void main(String[] args) {
        String x = commonPrefix("goshax", "goshaz");
        System.out.println(x);
    }

}
