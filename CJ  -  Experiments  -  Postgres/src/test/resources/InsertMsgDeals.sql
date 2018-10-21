INSERT INTO messages (id, contents) values(11, 'This is deals msg #1 for 12');
INSERT INTO messages (id, contents) values(12, 'This is deals msg #2 for 12');
INSERT INTO messages (id, contents) values(13, 'This is deals msg #1 for 25');
INSERT INTO messages (id, contents) values(14, 'This is deals msg #1 for 24');
INSERT INTO msgdeals (id_, deal_id) values(11, 12);
INSERT INTO msgdeals (id_, deal_id) values(12, 12);
INSERT INTO msgdeals (id_, deal_id) values(13, 25);
INSERT INTO msgdeals (id_, deal_id) values(14, 24);
