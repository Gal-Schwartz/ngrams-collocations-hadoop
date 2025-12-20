package com.collocations;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;

public class NGramUtils {
    public static int toDecade(int year) { return (year / 10) * 10; }

    // SequenceFile value usually: "year\toccurrences\t..." (>=2 fields)
    public static YearCount parseYearCount(Text v) {
        try {
            String[] parts = v.toString().split("\t");
            if (parts.length < 2) return null;
            int year = Integer.parseInt(parts[0].trim());
            long occ = (long) Double.parseDouble(parts[1].trim());
            return new YearCount(year, occ);
        } catch (Exception e) {
            return null;
        }
    }

    public static class YearCount {
        final int year;
        final long count;
        YearCount(int y, long c) { year = y; count = c; }
    }

    public static String cleanToken(String t) {
        if (t == null) return "";
        t = t.trim();
        if (t.isEmpty()) return "";
        int idx = t.indexOf('_'); // remove POS tag if exists
        if (idx > 0) t = t.substring(0, idx);
        // trim punctuation at ends (keep letters/numbers inside)
        t = t.replaceAll("^[^\\p{L}\\p{N}]+", "");
        t = t.replaceAll("[^\\p{L}\\p{N}]+$", "");
        return t.trim().toLowerCase();
    }

    public static String inferLangFromPath(InputSplit split) {
        try {
            String p = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) split).getPath().toString().toLowerCase();
            return p.contains("heb") ? Constants.LANG_HE : Constants.LANG_EN;
        } catch (Exception e) {
            return Constants.LANG_EN;
        }
    }
}
