CREATE OR REPLACE FUNCTION delete_contract (contract_id_ bigint) RETURNS bigint AS $$
DECLARE
	result bigint;
	number_rows bigint;
BEGIN
	result := 0;
	
	DELETE FROM deals WHERE deals.contract_id = contract_id_;
	GET DIAGNOSTICS number_rows = ROW_COUNT;
	result := result + number_rows;
	
	DELETE FROM contracts where id = contract_id_;
	GET DIAGNOSTICS number_rows = ROW_COUNT;
	result := result + number_rows;
	
	return result;
END;
$$ LANGUAGE plpgsql