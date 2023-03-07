CREATE OR REPLACE VIEW `INCOME_STATEMENT_DAILY` AS 
(
	SELECT 
		t.DT,
		REVENUES,
		SPENDING_LIMIT,
		EXPENSES,
		AMORTIZED_COST,
		COALESCE(f.FEE_AMOUNT,0) GENERATED_FEE,
		COALESCE(f.FEE_DUE_DATE,"") FEE_DUE_ON,
		APPLICABLE_FEE,
		COALESCE(fo.FEES_OUTSTANDING,0)-AMORTIZED_COST-APPLICABLE_FEE OUTSTANDING_FEE,
		REVENUES+(EXPENSES+AMORTIZED_COST)+APPLICABLE_FEE EARNINGS,
		SUM(EXPENSES+AMORTIZED_COST+APPLICABLE_FEE) OVER (PARTITION BY EXTRACT(YEAR_MONTH FROM t.DT) ORDER BY t.DT) COST_SPEED
	FROM
	(
		SELECT
			t.DT,
			REVENUES,
			SPENDING_LIMIT,
			EXPENSES,
			ABS(LEAST
			(
				0,
				EXPENSES+SPENDING_LIMIT #rev median is the spending limit
			))
			AMORTIZED_COST,
			COALESCE(SUM(f.FEE_AMOUNT),0) APPLICABLE_FEE
		FROM
		(
			SELECT
				cal.DT,
				COALESCE(rev.REV_1M,0) REVENUES,
				SUM(CASE WHEN t.TR_TYPE = 'expense' THEN t.TR_VALUE ELSE 0 END) EXPENSES,
				COALESCE(m_rev.MEDIAN,0) SPENDING_LIMIT
			FROM CALENDAR cal

			LEFT JOIN REVENUES rev
			ON cal.YEARMONTH=rev.TR_YEARMONTH
			
			LEFT JOIN TRANSACTIONS t
			ON cal.DT=t.TR_DATE

			LEFT JOIN MEDIAN_REV m_rev
			ON TRUE

			GROUP BY cal.DT
		) t
		LEFT JOIN V_SPENDING_FEES f
		ON t.DT BETWEEN f.FEE_START_DATE AND f.FEE_DUE_DATE

		GROUP BY t.DT
	) t
	LEFT JOIN V_SPENDING_FEES f
	ON t.DT=f.TR_DATE

	LEFT JOIN V_OUTSTANDING_FEES fo
	ON t.DT=fo.DT

	ORDER BY DT DESC
);