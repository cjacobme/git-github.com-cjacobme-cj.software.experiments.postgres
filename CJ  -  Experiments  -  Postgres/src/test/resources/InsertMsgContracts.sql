INSERT INTO messages (id, contents) values(1, 'This is contracts msg #1 for 31');
INSERT INTO messages (id, contents) values(2, 'This is contracts msg #2 for 31');
INSERT INTO messages (id, contents) values(3, 'This is contracts msg #1 for 32');
INSERT INTO msgcontracts (id_, contract_id) values(1, 31);
INSERT INTO msgcontracts (id_, contract_id) values(2, 31);
INSERT INTO msgcontracts (id_, contract_id) values(3, 32);
