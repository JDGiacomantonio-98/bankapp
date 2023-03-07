DROP PROCEDURE IF EXISTS `update_transactions`;

CREATE PROCEDURE update_transactions()
BEGIN
	# this procedures scans everytime the transactions_dump table to gather only the last updated transaction values
    # this procudere, while unefficient because it parses the whole dataset each time, accomodates the lack of MERGE statement in MySQL
    
	DELETE FROM TRANSACTIONS WHERE TRUE;
    INSERT INTO TRANSACTIONS
	SELECT 
		FLOW_ID,
		EXEC,
		PROVIDER,
		TR_DATE,
		EXTRACT(YEAR FROM TR_DATE) TR_YEAR,
		TR_WEEK,
		TR_MONTH,
		TR_DAY,
		TR_WEEKDAY,
		TR_HOUR,
		TR_TYPE,
		TR_LOCATION,
		TR_BENEFICIARY,
		TR_METHOD,
		#TR_INFO,
		TR_VALUE,
		TR_UOM
	FROM
	(
		SELECT *, ROW_NUMBER() OVER (PARTITION BY PROVIDER,TR_INFO ORDER BY EXEC DESC) RK from TRANSACTIONS_DUMP
	) L0
	WHERE RK = 1;
END;