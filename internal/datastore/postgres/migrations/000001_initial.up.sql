CREATE TABLE relation_tuple_transaction (
    id BIGSERIAL NOT NULL,
    timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL,
    CONSTRAINT pk_rttx PRIMARY KEY (id)
);

CREATE TABLE namespace_config (
    namespace VARCHAR NOT NULL,
    serialized_config BYTEA NOT NULL,
    created_transaction BIGINT NOT NULL,
    deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
    CONSTRAINT pk_namespace_config PRIMARY KEY (namespace, created_transaction)
);

CREATE TABLE relation_tuple (
    id BIGSERIAL NOT NULL,
    namespace VARCHAR NOT NULL,
    object_id VARCHAR NOT NULL,
    relation VARCHAR NOT NULL,
    userset_namespace VARCHAR NOT NULL,
    userset_object_id VARCHAR NOT NULL,
    userset_relation VARCHAR NOT NULL,
    created_transaction BIGINT NOT NULL,
    deleted_transaction BIGINT NOT NULL DEFAULT '9223372036854775807',
    CONSTRAINT pk_relation_tuple PRIMARY KEY (id),
    CONSTRAINT uq_relation_tuple_namespace UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, created_transaction, deleted_transaction),
    CONSTRAINT uq_relation_tuple_living UNIQUE (namespace, object_id, relation, userset_namespace, userset_object_id, userset_relation, deleted_transaction)
);

INSERT INTO relation_tuple_transaction DEFAULT VALUES;
