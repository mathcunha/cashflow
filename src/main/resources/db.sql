DROP TABLE Transactions;

CREATE TABLE Transactions (
    Date date,
    Costumer varchar(15),
    Account varchar(15),
    Value decimal(9,2),
    Count int,
    City varchar(100) 
);

select * from Transactions ;

WITH upd AS (
	UPDATE Transactions SET 
		Value = Value+2, Count = Count+2 
		WHERE Account = 'opa' AND Costumer = 'test' AND date = '2018-05-05'
		RETURNING *
)
INSERT INTO Transactions SELECT * FROM upd;

WITH upd AS (
	UPDATE Transactions SET 
		Value = Value+1, Count = Count+1 
		WHERE Account = 'opa' AND Costumer = 'test' AND date = '2018-05-05' AND city = 'sants'
		RETURNING *
)
INSERT INTO Transactions (date, costumer, account, value, count, city) 
	select '2018-05-05', 'test', 'opa', 1.5, 1, 'sants' WHERE NOT EXISTS (SELECT * FROM upd)
	
	
WITH upd AS (
        UPDATE Transactions SET
                Value = Value+12.0, Count = Count+1
                WHERE Account = 'A' AND Costumer = 'Math' AND date = '2018-02-20' AND city = 'Fortaleza'
                RETURNING *
)
INSERT INTO Transactions (date, costumer, account, value, count, city)
        select '2018-02-20', 'Math', 'A', 12.0, 1, 'Fortaleza' WHERE NOT EXISTS (SELECT * FROM upd)