CREATE TABLE contracts
(
    id bigint NOT NULL,
    seller character varying(255) NOT NULL,
    buyer character varying(255) NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE messages
(
    id bigint NOT NULL,
    contents character varying(1024) NOT NULL,
    CONSTRAINT messages_pkey PRIMARY KEY (id)
);

CREATE TABLE deals
(
    id bigint NOT NULL,
    contract_id bigint NOT NULL,
    start date NOT NULL,
    finish date NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT fk_deal_contract FOREIGN KEY (contract_id)
        REFERENCES contracts (id) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
);

CREATE TABLE msgdeals
(
    id_ bigint NOT NULL,
    deal_id bigint NOT NULL,
    PRIMARY KEY (id_),
    CONSTRAINT fk_msg_id FOREIGN KEY (id_)
        REFERENCES messages (id) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE RESTRICT,
    CONSTRAINT fk_msg_deal FOREIGN KEY (deal_id)
        REFERENCES deals (id) MATCH SIMPLE
        ON UPDATE RESTRICT
        ON DELETE RESTRICT
);
