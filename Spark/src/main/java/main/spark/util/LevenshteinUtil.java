package main.spark.util;

public class LevenshteinUtil {
    public static double levenshteinSimilarity(String a, String b) {
        if (a == null || b == null) return 0.0;
        a = a.trim().toLowerCase();
        b = b.trim().toLowerCase();
        int n = a.length();
        int m = b.length();
        if (n == 0) return m == 0 ? 1.0 : 0.0;

        int[][] dp = new int[n + 1][m + 1];
        for (int i = 0; i <= n; i++) dp[i][0] = i;
        for (int j = 0; j <= m; j++) dp[0][j] = j;

        for (int i = 1; i <= n; i++) {
            for (int j = 1; j <= m; j++) {
                int cost = (a.charAt(i - 1) == b.charAt(j - 1)) ? 0 : 1;
                dp[i][j] = Math.min(
                        Math.min(dp[i - 1][j] + 1, dp[i][j - 1] + 1),
                        dp[i - 1][j - 1] + cost
                );
            }
        }

        int distance = dp[n][m];
        int maxLen = Math.max(n, m);
        return 1.0 - ((double) distance / maxLen);
    }
}
