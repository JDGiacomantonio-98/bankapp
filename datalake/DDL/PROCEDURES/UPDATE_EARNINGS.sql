DROP PROCEDURE IF EXISTS `update_earnings`;

CREATE PROCEDURE update_earnings()
BEGIN
	# this procedures updates the app calendar to current date
	# this procudere, while unefficient because it parses the whole dataset each time, accomodates the lack of MERGE statement in MySQL

	DELETE FROM EARNINGS WHERE TRUE;

	INSERT INTO EARNINGS
	SELECT * FROM V_EARNINGS;
END;