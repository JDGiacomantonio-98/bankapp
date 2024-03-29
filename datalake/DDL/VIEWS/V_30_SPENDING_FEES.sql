CREATE OR REPLACE VIEW `V_SPENDING_FEES` AS
(
	SELECT
		TR_DATE,
		FEE_START_DATE,
		CASE 
			WHEN 1M_FEE <> 0 THEN DATE_ADD(FEE_START_DATE,INTERVAL 30 DAY)
			WHEN 3M_FEE <> 0 THEN DATE_ADD(FEE_START_DATE,INTERVAL 90 DAY)
			WHEN 6M_FEE <> 0 THEN DATE_ADD(FEE_START_DATE,INTERVAL 180 DAY)
			WHEN 12M_FEE <> 0 THEN DATE_ADD(FEE_START_DATE,INTERVAL 365 DAY)
		END AS FEE_DUE_DATE,
		1M_FEE+3M_FEE+6M_FEE+12M_FEE FEE_AMOUNT
	FROM
	(
		SELECT
			TR_DATE,
			DATE_ADD(TR_DATE,INTERVAL 1 DAY) FEE_START_DATE,
			CASE WHEN ABS(AMORTIZED_COST) <= m_earn.MEDIAN*2*7 THEN AMORTIZED_COST/30 ELSE 0 END AS 1M_FEE,
			CASE WHEN ABS(AMORTIZED_COST) BETWEEN m_earn.MEDIAN*2*7 AND m_earn.MEDIAN*6*7 THEN AMORTIZED_COST/90 ELSE 0 END AS 3M_FEE,
			CASE WHEN ABS(AMORTIZED_COST) BETWEEN m_earn.MEDIAN*6*7 AND m_earn.MEDIAN*15*7 THEN AMORTIZED_COST/180 ELSE 0 END AS 6M_FEE,
			CASE WHEN ABS(AMORTIZED_COST) > m_earn.MEDIAN*15*7 THEN AMORTIZED_COST/365 ELSE 0 END AS 12M_FEE
		FROM
		(
			SELECT
				TR_DATE,
				LEAST
				(
					0,
					SUM(CASE WHEN t.TR_TYPE = 'expense' THEN t.TR_VALUE ELSE 0 END)+COALESCE(m_rev.MEDIAN,0) #rev median is the spending limit
				)
				AMORTIZED_COST
			FROM TRANSACTIONS t
			LEFT JOIN MEDIAN_REV m_rev
			ON TRUE
			GROUP BY TR_DATE
		) t
		LEFT JOIN MEDIAN_EARN m_earn
		ON TRUE
	) t
	-- ORDER BY FEE_DUE_DATE DESC
);