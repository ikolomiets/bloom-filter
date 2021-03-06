package com.electrit.bloomfilter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("WeakerAccess")
public class DbNameLookup implements NameLookup {

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

        public void addChild(PrefixWords child) {
            children.add(child);
        }

        public List<PrefixWords> getChildren() {
            return children;
        }

        public int size() {
            int size = 0;
            for (PrefixWords child : children)
                size += child.size();

            return size > 0 ? size : 1;
        }

        private static List<String> getWordsAndPrefixes(List<PrefixWords> children) {
            List<String> result = new ArrayList<>();
            //noinspection Convert2streamapi
            for (PrefixWords child : children)
                result.add(child.prefix + (child.size() > 1 ? "" : "."));

            return result;
        }

        public List<String> getWordsAndPrefixes(int max) {
            List<String> result = new ArrayList<>();

            List<PrefixWords> optimalChildren = new ArrayList<>(this.children);

            while (result.size() + optimalChildren.size() < max) {
                List<String> minWordsAndPrefixes = null;
                int minChildIdx = -1;
                for (int i = 0; i < optimalChildren.size(); i++) {
                    PrefixWords child = optimalChildren.get(i);
                    if (child.getChildren().isEmpty())
                        continue;

                    List<String> wordsAndPrefixes = child.getWordsAndPrefixes(max - optimalChildren.size() + 1);
                    if (minWordsAndPrefixes == null || wordsAndPrefixes.size() < minWordsAndPrefixes.size()) {
                        minWordsAndPrefixes = wordsAndPrefixes;
                        minChildIdx = i;
                    }
                }

                if (minWordsAndPrefixes != null && optimalChildren.size() - 1 + result.size() + minWordsAndPrefixes.size() <= max) {
                    optimalChildren.remove(minChildIdx);
                    result.addAll(minWordsAndPrefixes);
                } else {
                    break;
                }
            }

            result.addAll(getWordsAndPrefixes(optimalChildren));

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
                sb.append("\n").append(child.toString(nextIndent));
            }
            return sb.toString();
        }
    }

    @Override
    public List<String> lookup(String prefix, int max) {
        PrefixWords root = new PrefixWords(prefix);
        try {
            psSelect.setString(1, prefix + "%");
            ResultSet resultSet = psSelect.executeQuery();
            if (resultSet.next()) {
                String name = resultSet.getString(1);
                root.addChild(new PrefixWords(name));
                groupByPrefix(resultSet, root);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Unable to execute db query for " + prefix, e);
        }

        return root.getWordsAndPrefixes(max);
    }

    private static void groupByPrefix(ResultSet resultSet, PrefixWords parent) throws SQLException {
        while (resultSet.next()) {
            String name = resultSet.getString(1);
            if (name.startsWith(parent.prefix)) {
                PrefixWords child = new PrefixWords(name);
                if (parent.getChildren().isEmpty()) {
                    parent.addChild(new PrefixWords(parent.prefix));
                } else {
                    int lastIndex = parent.getChildren().size() - 1;
                    PrefixWords lastChild = parent.getChildren().get(lastIndex);
                    String commonPrefix = commonPrefix(lastChild.prefix, child.prefix);
                    if (commonPrefix == null) {
                        throw new RuntimeException("child=" + child + "\n\nparent=" + parent);
                    }

                    if (!commonPrefix.equals(parent.prefix)) {
                        PrefixWords newParent = new PrefixWords(commonPrefix);
                        newParent.addChild(lastChild);
                        newParent.addChild(child);
                        parent.getChildren().set(lastIndex, newParent);
                        groupByPrefix(resultSet, newParent);
                        continue;
                    }
                }
                parent.addChild(child);
                groupByPrefix(resultSet, child);
            } else {
                resultSet.previous();
                return;
            }
        }
    }

    private static String commonPrefix(String first, String second) {
        int min = Math.min(first.length(), second.length());
        for (int i = 0; i < min; i++)
            if (first.charAt(i) != second.charAt(i))
                if (i > 0)
                    return first.substring(0, i);
                else
                    return null;

        return first.substring(0, min);
    }

}
