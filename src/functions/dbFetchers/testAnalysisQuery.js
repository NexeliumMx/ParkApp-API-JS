const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('testAnalysisQuery', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const userId = request.query.get('user_id') || 'fb713fca-4cbc-44b1-8a25-c6685c3efd31';
        const testStep = request.query.get('step') || '1';
        const testDate = request.query.get('date') || '2025-07-02';

        let client;
        try {
            client = await getClient();
            let query = '';
            let values = [userId];

            switch (testStep) {
                case '1':
                    // Test basic count
                    query = `
                        SELECT 
                            COUNT(*) as total_measurements,
                            COUNT(DISTINCT m.sensor_id) as unique_sensors,
                            MIN(m.timestamp) as earliest_time,
                            MAX(m.timestamp) as latest_time
                        FROM measurements m
                        JOIN sensor_info si ON m.sensor_id = si.sensor_id
                        JOIN parking p ON si.parking_id = p.parking_id
                        JOIN permissions perm ON p.parking_id = perm.parking_id
                        WHERE perm.user_id = $1
                        AND DATE(m.timestamp) = $2
                    `;
                    values.push(testDate);
                    break;

                case '2':
                    // Test basic grouping without window function
                    query = `
                        SELECT 
                            si.parking_id,
                            p.parking_alias,
                            EXTRACT(HOUR FROM m.timestamp) as hour_of_day,
                            COUNT(*) as measurement_count,
                            COUNT(*) FILTER (WHERE m.state = true) as occupied_count,
                            COUNT(*) FILTER (WHERE m.state = false) as available_count
                        FROM measurements m
                        JOIN sensor_info si ON m.sensor_id = si.sensor_id
                        JOIN parking p ON si.parking_id = p.parking_id
                        JOIN permissions perm ON p.parking_id = perm.parking_id
                        WHERE perm.user_id = $1
                        AND DATE(m.timestamp) = $2
                        GROUP BY si.parking_id, p.parking_alias, EXTRACT(HOUR FROM m.timestamp)
                        ORDER BY si.parking_id, hour_of_day
                        LIMIT 20
                    `;
                    values.push(testDate);
                    break;

                case '3':
                    // Test window function on small dataset (single sensor)
                    query = `
                        WITH limited_data AS (
                            SELECT 
                                m.sensor_id,
                                m.state,
                                m.timestamp,
                                si.parking_id,
                                p.parking_alias
                            FROM measurements m
                            JOIN sensor_info si ON m.sensor_id = si.sensor_id
                            JOIN parking p ON si.parking_id = p.parking_id
                            JOIN permissions perm ON p.parking_id = perm.parking_id
                            WHERE perm.user_id = $1
                            AND DATE(m.timestamp) = $2
                            ORDER BY m.sensor_id, m.timestamp
                            LIMIT 1000
                        )
                        SELECT 
                            sensor_id,
                            state,
                            timestamp,
                            LEAD(timestamp) OVER (PARTITION BY sensor_id ORDER BY timestamp) as next_timestamp,
                            parking_id,
                            parking_alias
                        FROM limited_data
                        ORDER BY sensor_id, timestamp
                        LIMIT 50
                    `;
                    values.push(testDate);
                    break;

                case '4':
                    // Test complete query but with LIMIT
                    query = `
                        WITH sensor_state_durations AS (
                            SELECT
                                m.sensor_id,
                                m.state,
                                m.timestamp,
                                LEAD(m.timestamp) OVER (PARTITION BY m.sensor_id ORDER BY m.timestamp) as next_timestamp,
                                EXTRACT(HOUR FROM m.timestamp) as time_period,
                                si.parking_id,
                                p.parking_alias
                            FROM measurements m
                            JOIN sensor_info si ON m.sensor_id = si.sensor_id
                            JOIN parking p ON si.parking_id = p.parking_id
                            JOIN permissions perm ON p.parking_id = perm.parking_id
                            WHERE perm.user_id = $1 
                            AND DATE(m.timestamp) = $2
                            ORDER BY m.sensor_id, m.timestamp
                            LIMIT 5000  -- Limit raw data
                        ),
                        time_period_state_times AS (
                            SELECT
                                parking_id,
                                parking_alias,
                                time_period,
                                SUM(
                                    CASE
                                        WHEN state = true AND next_timestamp IS NOT NULL
                                        THEN EXTRACT(EPOCH FROM (next_timestamp - timestamp))
                                        ELSE 0
                                    END
                                ) as occupied_seconds,
                                SUM(
                                    CASE
                                        WHEN state = false AND next_timestamp IS NOT NULL
                                        THEN EXTRACT(EPOCH FROM (next_timestamp - timestamp))
                                        ELSE 0
                                    END
                                ) as available_seconds,
                                COUNT(*) FILTER (WHERE next_timestamp IS NOT NULL) as state_changes
                            FROM sensor_state_durations
                            WHERE next_timestamp IS NOT NULL
                            GROUP BY parking_id, parking_alias, time_period
                        )
                        SELECT
                            time_period,
                            parking_id,
                            parking_alias,
                            occupied_seconds,
                            available_seconds,
                            state_changes,
                            CASE
                                WHEN (occupied_seconds + available_seconds) > 0
                                THEN ROUND((occupied_seconds * 100.0 / (occupied_seconds + available_seconds))::numeric, 2)
                                ELSE 0
                            END as occupancy_percentage
                        FROM time_period_state_times
                        ORDER BY parking_id, time_period
                        LIMIT 25
                    `;
                    values.push(testDate);
                    break;

                case '5':
                    // Test with specific parking ID to reduce dataset
                    query = `
                        WITH sensor_state_durations AS (
                            SELECT
                                m.sensor_id,
                                m.state,
                                m.timestamp,
                                LEAD(m.timestamp) OVER (PARTITION BY m.sensor_id ORDER BY m.timestamp) as next_timestamp,
                                EXTRACT(HOUR FROM m.timestamp) as time_period,
                                si.parking_id,
                                p.parking_alias
                            FROM measurements m
                            JOIN sensor_info si ON m.sensor_id = si.sensor_id
                            JOIN parking p ON si.parking_id = p.parking_id
                            JOIN permissions perm ON p.parking_id = perm.parking_id
                            WHERE perm.user_id = $1 
                            AND DATE(m.timestamp) = $2
                            AND si.parking_id = $3  -- Filter to specific parking
                        )
                        SELECT
                            time_period,
                            parking_id,
                            parking_alias,
                            COUNT(*) as total_records,
                            COUNT(*) FILTER (WHERE next_timestamp IS NOT NULL) as valid_durations,
                            SUM(
                                CASE
                                    WHEN state = true AND next_timestamp IS NOT NULL
                                    THEN EXTRACT(EPOCH FROM (next_timestamp - timestamp))
                                    ELSE 0
                                END
                            ) as occupied_seconds
                        FROM sensor_state_durations
                        GROUP BY time_period, parking_id, parking_alias
                        ORDER BY time_period
                    `;
                    values.push(testDate, '1'); // Test with parking_id = 1
                    break;

                default:
                    return {
                        status: 400,
                        headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                        body: JSON.stringify({ error: 'Invalid step. Use 1-5.' })
                    };
            }

            context.log(`Executing test step ${testStep}`);
            context.log(`Query: ${query.substring(0, 200)}...`);
            
            const startTime = Date.now();
            const result = await client.query(query, values);
            const executionTime = Date.now() - startTime;

            context.log(`Step ${testStep} completed in ${executionTime}ms - ${result.rows.length} rows`);

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({
                    success: true,
                    test_step: testStep,
                    execution_time_ms: executionTime,
                    row_count: result.rows.length,
                    data: result.rows,
                    query_preview: query.substring(0, 300) + '...'
                })
            };

        } catch (error) {
            context.log.error(`Error in test step ${testStep}:`, error);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ 
                    success: false, 
                    test_step: testStep,
                    error: error.message 
                })
            };
        } finally {
            if (client) {
                client.release();
            }
        }
    }
});