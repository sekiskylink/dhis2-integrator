INSERT INTO dhis2_instance_pair (source, destination, source_url, source_username, source_password) 
    VALUES 
    ('epivac', 'eidsr', 'https://epivac.health.go.ug/api/dataValueSets?', 'foo', 'bar'),
    ('emis', 'eidsr', 'https://emisuganda.org/emis/api/dataValueSets.json?', 'foo', 'bar');

INSERT INTO sync_datasets 
    (instance_pair_id, dataset_id, dataset_name, reporting_frequency, include_deleted)
    VALUES
    (1, 'nTlQefWKMmb', 'Daily Vaccination Summary (V1.2)', 'daily', FALSE),
    (2, 'O3j08JESoRK', 'COVID19: School Based Surveillance Daily Report', 'daily', FALSE);
