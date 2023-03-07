DROP PROCEDURE IF EXISTS `update_revenues`;

CREATE PROCEDURE update_revenues()
BEGIN
	# this procedures updates the app calendar to current date
	# this procudere, while unefficient because it parses the whole dataset each time, accomodates the lack of MERGE statement in MySQL

	DELETE FROM REVENUES WHERE TRUE;

	INSERT INTO REVENUES
	SELECT * FROM V_REVENUES;
END;