CREATE OR REPLACE FUNCTION delete_contract (contract_id bigint) RETURNS bigint AS $$
BEGIN
	RAISE NOTICE 'delete contract %...', contract_id;
	DELETE FROM contracts where id = contract_id;
	RETURN contract_id * -1;
END;
$$ LANGUAGE plpgsql