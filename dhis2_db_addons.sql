-- Sierra Leone dataset=eZDhcZi6FLP, orgunit=YuQRtpLP10I, sdate='2022-01-01', edate='2022-01-31' 

-- eidsr test
-- aggregate_values_to_upper_level('anVcqowmG23', 'aXmBzv61LbM', '2020-06-03', '2021-06-04');
-- http://localhost:8080/api/29/sqlViews/TLCBynaq2If/data.json?var=dataset:anVcqowmG23&var=orgunit:aXmBzv61LbM&var=sdate:2020-06-03&var=edate:2021-06-04&paging=false

CREATE OR REPLACE FUNCTION aggregate_values_to_upper_level(
    dataset_uid TEXT, orgunit TEXT, sdate TEXT, edate TEXT)
    RETURNS TABLE (value FLOAT,
        dataelement VARCHAR(11),
        categoryoptioncombo VARCHAR(11),
        attributeoptioncombo VARCHAR(11)
    )
AS
$delim$
    DECLARE
        dataset_id BIGINT;
        orgunits_level BIGINT := 5; -- orgunit level 7 for schools, 5 for facilities
        -- orgunits_level BIGINT := 4; -- orgunit level 4 for sierra leone
        orgunits BIGINT[]; -- reporting orgunits
        datasetsources BIGINT[];

    BEGIN
        SELECT datasetid INTO dataset_id FROM dataset WHERE uid = dataset_uid;

        SELECT array_agg(sourceid) INTO datasetsources FROM datasetsource WHERE datasetid = dataset_id;

        SELECT array_agg(organisationunitid) INTO orgunits FROM organisationunit 
            WHERE 
                split_part(path, '/', 4) = orgunit 
                AND hierarchylevel = orgunits_level
                AND organisationunitid = ANY(datasetsources); 

        RETURN QUERY    
            SELECT 
                sum(dv.value::float), dx.uid AS dataelement, 
                coc.uid AS categoryoptioncombo, 
                att.uid AS attributeoptioncombo 
            FROM 
                datavalue dv 
                INNER JOIN period p ON (dv.periodid = p.periodid) 
                INNER JOIN dataelement dx ON (dv.dataelementid = dx.dataelementid) 
                INNER JOIN categoryoptioncombo coc ON (dv.categoryoptioncomboid = coc.categoryoptioncomboid)
                INNER JOIN categoryoptioncombo att ON (dv.attributeoptioncomboid = att.categoryoptioncomboid)
            WHERE 
                dv.sourceid = ANY(orgunits)
                AND dv.value ~ '^[0-9\.]+$'
                AND p.startdate >= sdate::DATE AND p.enddate <= edate::DATE
                AND deleted = FALSE
            GROUP BY dataelement, categoryoptioncombo, attributeoptioncombo;

    END;
$delim$ LANGUAGE plpgsql;

-- SQL VIEW -- SELECT * FROM aggregate_values_to_upper_level('${dataset}', '${orgunit}', '${sdate}', '${edate}');
-- orgunit = district orgunit
