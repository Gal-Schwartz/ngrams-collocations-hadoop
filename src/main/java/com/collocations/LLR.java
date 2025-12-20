package com.collocations;
// ==========================================================
    // LLR computation
    // ==========================================================

    /**
     * Log-likelihood ratio for 2x2 contingency table:
     * k11 = c12
     * k12 = c1 - c12
     * k21 = c2 - c12
     * k22 = N - c1 - c2 + c12
     *
     * LLR = 2 * sum_{ij} k_ij * log(k_ij / e_ij), where e_ij are expected counts.
     */
public class LLR {
    public static double computeLLR(long c1, long c2, long c12, long N) {
        long k11 = c12;
        long k12 = c1 - c12;
        long k21 = c2 - c12;
        long k22 = N - c1 - c2 + c12;

        // Guards (required for correctness / prevent NaNs)
        if (k11 < 0 || k12 < 0 || k21 < 0 || k22 < 0) return Double.NaN;
        if (N <= 0) return Double.NaN;
        if (c1 <= 0 || c2 <= 0 || c12 <= 0) return Double.NaN;

        double row1 = k11 + k12;
        double row2 = k21 + k22;
        double col1 = k11 + k21;
        double col2 = k12 + k22;
        double total = row1 + row2; // should equal N

        if (total <= 0) return Double.NaN;

        double e11 = row1 * col1 / total;
        double e12 = row1 * col2 / total;
        double e21 = row2 * col1 / total;
        double e22 = row2 * col2 / total;

        return 2.0 * (term(k11, e11) + term(k12, e12) + term(k21, e21) + term(k22, e22));
    }

    private static double term(double k, double e) {
        if (k == 0.0) return 0.0;
        if (e <= 0.0) return 0.0;
        return k * Math.log(k / e);
    }
}