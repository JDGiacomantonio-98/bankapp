CREATE OR REPLACE VIEW `INCOME_STATEMENT_ALL` AS
(
	SELECT
		SUM(REVENUES) REVENUES,
		SUM(EXPENSES) EXPENSES,
		SUM(AMORTIZED_COST) AMORTIZED_COST,
		SUM(APPLICABLE_FEE) FEES,
		SUM(EARNINGS) EARNINGS,
		SUM(EARNINGS)/SUM(REVENUES) MARGIN
	FROM INCOME_STATEMENT_DAILY
);