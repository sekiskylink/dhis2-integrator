CREATE TABLE dhis2_instance_pair(
    id SERIAL PRIMARY KEY NOT NULL,
    source TEXT NOT NULL,
    destination TEXT NOT NULL,
    source_url TEXT NOT NULL DEFAULT '', -- destination URL configured in dispatcher 2
    source_username TEXT NOT NULL DEFAULT '',
    source_password TEXT NOT NULL DEFAULT '',
    is_active BOOLEAN DEFAULT TRUE,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE sync_datasets (
    id SERIAL PRIMARY KEY NOT NULL,
    instance_pair_id INTEGER NOT NULL REFERENCES dhis2_instance_pair(id),
    dataset_id TEXT NOT NULL,
    dataset_name TEXT NOT NULL,
    reporting_frequency TEXT NOT NULL CHECK (reporting_frequency IN ('daily', 'weekly', 'monthly')),
    include_deleted BOOLEAN DEFAULT FALSE,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
    
);

CREATE TABLE orgunits(
    id SERIAL PRIMARY KEY NOT NULL,
    instance_pair_id INTEGER NOT NULL REFERENCES dhis2_instance_pair(id),
    dhis2_name TEXT NOT NULL,
    dhis2_id VARCHAR(12) NOT NULL DEFAULT '',
    dhis2_path TEXT NOT NULL DEFAULT '',
    dhis2_parent TEXT NOT NULL DEFAULT '',
    dhis2_level TEXT NOT NULL DEFAULT '',
    priority INTEGER,
    is_active BOOLEAN DEFAULT TRUE,
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE indicator_mapping(
    id SERIAL PRIMARY KEY NOT NULL,
    instance_pair_id INTEGER NOT NULL REFERENCES dhis2_instance_pair(id),
    name TEXT NOT NULL DEFAULT '',
    source_indicator_id TEXT NOT NULL DEFAULT '',
    dataset TEXT NOT NULL DEFAULT '',
    dataelement TEXT NOT NULL DEFAULT '',
    category_option_combo TEXT NOT NULL DEFAULT '',
    created TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

