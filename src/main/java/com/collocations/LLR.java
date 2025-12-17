package com.collocations;

public final class LLR {
    private LLR() {}

    // Dunning log-likelihood ratio (G^2) for 2x2 table:
    // k11 = c12
    // k12 = c1 - c12
    // k21 = c2 - c12
    // k22 = N - c1 - c2 + c12
    public static double compute(long c1, long c2, long c12, long N) {
        if (N <= 0) return Double.NaN;

        long k11 = c12;
        long k12 = c1 - c12;
        long k21 = c2 - c12;
        long k22 = N - c1 - c2 + c12;

        if (k11 < 0 || k12 < 0 || k21 < 0 || k22 < 0) return Double.NaN;

        double row1 = k11 + k12;   // c1
        double row2 = k21 + k22;   // N - c1
        double col1 = k11 + k21;   // c2
        double col2 = k12 + k22;   // N - c2
        double total = row1 + row2; // N

        double e11 = row1 * col1 / total;
        double e12 = row1 * col2 / total;
        double e21 = row2 * col1 / total;
        double e22 = row2 * col2 / total;

        double g2 = 2.0 * (
                term(k11, e11) +
                term(k12, e12) +
                term(k21, e21) +
                term(k22, e22)
        );
        return g2;
    }

    private static double term(long k, double e) {
        if (k == 0) return 0.0;
        return k * Math.log(k / e);
    }
}
