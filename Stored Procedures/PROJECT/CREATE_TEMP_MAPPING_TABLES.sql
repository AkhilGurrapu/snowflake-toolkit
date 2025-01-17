CREATE OR REPLACE PROCEDURE admin.admin.CREATE_TEMP_MAPPING_TABLES(
    SOURCE_DATABASE STRING, 
    SOURCE_SCHEMA STRING
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    if (!SOURCE_DATABASE || !SOURCE_SCHEMA) {
        return "Error: Database and Schema parameters are required";
    }
    SOURCE_DATABASE = SOURCE_DATABASE.toUpperCase();
    SOURCE_SCHEMA = SOURCE_SCHEMA.toUpperCase();

    var cmd = `SELECT table_name 
               FROM ${SOURCE_DATABASE}.information_schema.tables 
               WHERE table_schema = '${SOURCE_SCHEMA}' 
               AND table_catalog = '${SOURCE_DATABASE}'
               AND table_type = 'BASE TABLE'`;
    var tables = snowflake.createStatement({sqlText: cmd}).execute();
    var ddls = [];

    while (tables.next()) {
        var tableName = tables.getColumnValue(1);
        var mapTableName = `${tableName}_ID_MAP`;
        var ddl = `
        CREATE OR REPLACE  TABLE ${mapTableName} (
            old_id NUMBER,
            new_id VARCHAR(50)
        )`;
        ddls.push(ddl);
    }

    return ddls.join(';\n\n') + ';';
$$
;
