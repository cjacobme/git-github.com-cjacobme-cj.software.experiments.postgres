CREATE OR REPLACE FUNCTION delete_contract (contract_id_ bigint) RETURNS bigint AS $$
DECLARE
	result bigint;
	number_rows bigint;
BEGIN
	result := 0;
	
	create temp table delmsgcontracts (id bigint, primary key (id));
	
	INSERT INTO delmsgcontracts select (id_) from msgcontracts  WHERE contract_id = contract_id_;
	
	DELETE FROM msgcontracts WHERE contract_id = contract_id_;
	GET DIAGNOSTICS number_rows = ROW_COUNT;
	result := result + number_rows;
	
	DELETE from messages where id in (select id from delmsgcontracts);
	GET DIAGNOSTICS number_rows = ROW_COUNT;
	result := result + number_rows;
	DROP TABLE delmsgcontracts;
	
	DELETE FROM deals WHERE deals.contract_id = contract_id_;
	GET DIAGNOSTICS number_rows = ROW_COUNT;
	result := result + number_rows;
	
	DELETE FROM contracts where id = contract_id_;
	GET DIAGNOSTICS number_rows = ROW_COUNT;
	result := result + number_rows;
	
	return result;
END;
$$ LANGUAGE plpgsql