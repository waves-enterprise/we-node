CREATE TABLE "policy_meta_data"(
    "policy_id" VARCHAR NOT NULL,
    "hash" VARCHAR NOT NULL,
    "sender" VARCHAR NOT NULL,
    "type" VARCHAR NOT NULL,
    "filename" VARCHAR NOT NULL,
    "size" INTEGER NOT NULL,
    "timestamp" BIGINT NOT NULL,
    "author" VARCHAR NOT NULL,
    "comment" VARCHAR NOT NULL,
    PRIMARY KEY("hash")
);
CREATE UNIQUE INDEX "policy_meta_data_policy_id_with_hash_idx" on "policy_meta_data" ("policy_id", "hash");

CREATE TABLE "policy_data"(
    "policy_id" VARCHAR NOT NULL,
    "hash" VARCHAR NOT NULL,
    "data" BYTEA NOT NULL,
    PRIMARY KEY("hash")
);
CREATE UNIQUE INDEX "policy_data_policy_id_with_hash_idx" on "policy_data" ("policy_id", "hash");