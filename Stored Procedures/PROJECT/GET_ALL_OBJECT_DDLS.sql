CREATE OR REPLACE PROCEDURE GET_ALL_OBJECT_DDLS(
    SOURCE_DATABASE STRING, 
    SOURCE_SCHEMA STRING DEFAULT NULL,
    OBJECT_TYPE STRING DEFAULT NULL
)
RETURNS STRING
LANGUAGE JAVASCRIPT
AS
$$
    // Input validation
    if (!SOURCE_DATABASE) {
        return "Error: Database parameter is required";
    }

    // Convert parameters to uppercase
    SOURCE_DATABASE = SOURCE_DATABASE.toUpperCase();
    SOURCE_SCHEMA = SOURCE_SCHEMA ? SOURCE_SCHEMA.toUpperCase() : null;
    OBJECT_TYPE = OBJECT_TYPE ? OBJECT_TYPE.toUpperCase() : null;

    // Valid object types
    const validTypes = [
        'DATABASE', 'SCHEMA', 'TABLE', 'VIEW', 'PROCEDURE', 'DYNAMIC_TABLE', 
        'EXTERNAL_TABLE', 'FILE_FORMAT', 'PIPE', 'SEQUENCE', 'STREAM', 
        'TASK', 'FUNCTION', 'ALERT', 'EVENT_TABLE', 'ICEBERG_TABLE',
        'MATERIALIZED_VIEW', 'STAGE', 'TAG', 'HYBRID_TABLE'
    ];

    // Validate object type if provided
    if (OBJECT_TYPE && !validTypes.includes(OBJECT_TYPE)) {
        return "Error: Invalid object type. Valid types are: " + validTypes.join(", ");
    }

    var ddls = [];

    // Handle DATABASE level DDL
    if (!SOURCE_SCHEMA && !OBJECT_TYPE) {
        var ddlCmd = `SELECT GET_DDL('DATABASE', '${SOURCE_DATABASE}')`;
        var result = snowflake.createStatement({sqlText: ddlCmd}).execute();
        if (result.next()) {
            return result.getColumnValue(1);
        }
    }

    // Handle SCHEMA level DDL
    if (SOURCE_SCHEMA && !OBJECT_TYPE) {
        var ddlCmd = `SELECT GET_DDL('SCHEMA', '${SOURCE_DATABASE}.${SOURCE_SCHEMA}')`;
        var result = snowflake.createStatement({sqlText: ddlCmd}).execute();
        if (result.next()) {
            return result.getColumnValue(1);
        }
    }

    // Object type specific DDL generation
    switch(OBJECT_TYPE) {
        case 'TABLE':
            var cmd = `
                SELECT table_name 
                FROM ${SOURCE_DATABASE}.information_schema.tables 
                WHERE table_schema = '${SOURCE_SCHEMA}' 
                AND table_catalog = '${SOURCE_DATABASE}'
                AND table_type = 'BASE TABLE'`;
            break;
            
        case 'VIEW':
            var cmd = `
                SELECT table_name 
                FROM ${SOURCE_DATABASE}.information_schema.views 
                WHERE table_schema = '${SOURCE_SCHEMA}' 
                AND table_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        case 'EVENT_TABLE':
            var cmd = `
                SELECT table_name 
                FROM ${SOURCE_DATABASE}.information_schema.event_tables 
                WHERE table_schema = '${SOURCE_SCHEMA}' 
                AND table_catalog = '${SOURCE_DATABASE}'`;
            break;

        case 'EXTERNAL_TABLE':
            var cmd = `
                SELECT table_name 
                FROM ${SOURCE_DATABASE}.information_schema.external_tables 
                WHERE table_schema = '${SOURCE_SCHEMA}' 
                AND table_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        case 'PROCEDURE':
            var cmd = `
                SELECT procedure_name || 
                REGEXP_REPLACE(
                    argument_signature,
                    '([A-Za-z_][A-Za-z0-9_]*) (VARCHAR|NUMBER|FLOAT|STRING|BOOLEAN|ARRAY|VARIANT|OBJECT|TIME|TIMESTAMP|DATE|BINARY|INTEGER)',
                    '\\\\2'
                ) AS object_name
                FROM ${SOURCE_DATABASE}.information_schema.procedures 
                WHERE procedure_schema = '${SOURCE_SCHEMA}' 
                AND procedure_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        case 'FUNCTION':
            var cmd = `
                SELECT function_name || 
                REGEXP_REPLACE(
                    argument_signature,
                    '([A-Za-z_][A-Za-z0-9_]*) (VARCHAR|NUMBER|FLOAT|STRING|BOOLEAN|ARRAY|VARIANT|OBJECT|TIME|TIMESTAMP|DATE|BINARY|INTEGER)',
                    '\\\\2'
                ) AS object_name
                FROM ${SOURCE_DATABASE}.information_schema.functions 
                WHERE function_schema = '${SOURCE_SCHEMA}' 
                AND function_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        case 'SEQUENCE':
            var cmd = `
                SELECT sequence_name
                FROM ${SOURCE_DATABASE}.information_schema.sequences
                WHERE sequence_schema = '${SOURCE_SCHEMA}'
                AND sequence_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        case 'PIPE':
            var cmd = `
                SELECT pipe_name
                FROM ${SOURCE_DATABASE}.information_schema.pipes
                WHERE pipe_schema = '${SOURCE_SCHEMA}'
                AND pipe_catalog = '${SOURCE_DATABASE}'`;
            break;
        case 'FILE_FORMAT':
            var cmd = `
                SELECT file_format_name
                FROM ${SOURCE_DATABASE}.information_schema.file_formats
                WHERE file_format_schema = '${SOURCE_SCHEMA}'
                AND file_format_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        case 'STAGE':
            var cmd = `
                SELECT stage_name
                FROM ${SOURCE_DATABASE}.information_schema.stages
                WHERE stage_schema = '${SOURCE_SCHEMA}'
                AND stage_catalog = '${SOURCE_DATABASE}'`;
            break;
            
        default:
            return "Error: Please specify a valid object type";
    }

    // Execute the query to get object names
    var objects = snowflake.createStatement({sqlText: cmd}).execute();
    
    // Get DDL for each object
    while (objects.next()) {
        var objectName = objects.getColumnValue(1);
        var ddlCmd = `SELECT GET_DDL('${OBJECT_TYPE}', '${SOURCE_DATABASE}.${SOURCE_SCHEMA}.${objectName}')`;
        var stmt = snowflake.createStatement({sqlText: ddlCmd});
        var result = stmt.execute();
        if (result.next()) {
            ddls.push(result.getColumnValue(1));
        }
    }

    return ddls.join(';\n\n');
$$;

-- Example calls:
-- Get Database DDL
CALL GET_ALL_OBJECT_DDLS('TASTYBYTES');

-- Get Schema DDL
CALL GET_ALL_OBJECT_DDLS('TASTYBYTES', 'RAW_POS');

-- Get all tables DDL
CALL GET_ALL_OBJECT_DDLS('TASTYBYTES', 'RAW_POS', 'TABLE');

-- Get all tasks DDL
CALL GET_ALL_OBJECT_DDLS('TASTYBYTES', 'RAW_POS', 'TASK');