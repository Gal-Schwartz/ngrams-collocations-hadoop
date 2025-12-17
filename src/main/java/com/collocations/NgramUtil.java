package collocations;

public final class NgramUtil {
    private NgramUtil() {}

    // Decade of year: 1995 -> 1990
    public static int decadeOfYear(int year) {
        return (year / 10) * 10;
    }

    // Google ngrams line looks like:
    // "<ngram>\t<year>\t<match_count>\t<volume_count>"
    // But in SequenceFile sometimes key=ngram and value="year\tmatch\tvol".
    // This parser supports both formats.
    public static Parsed parseKeyValue(String keyText, String valueText) {
        String line;
        if (keyText != null && keyText.contains("\t")) {
            line = keyText;                  // full line in key
        } else if (valueText != null && valueText.contains("\t")) {
            line = keyText + "\t" + valueText; // split between key and value
        } else {
            return null;
        }

        String[] parts = line.split("\t");
        if (parts.length < 3) return null;

        String gram = parts[0];
        int year;
        long matchCount;
        try {
            year = Integer.parseInt(parts[1]);
            matchCount = Long.parseLong(parts[2]);
        } catch (Exception e) {
            return null;
        }
        return new Parsed(gram, year, matchCount);
    }

    public record Parsed(String gram, int year, long matchCount) {}
}
