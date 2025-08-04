const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

function buildDynamicDateFilter(params) {
    const filters = [];
    const values = [];
    let idx = 1;

    // Year
    if (params.year_range === 'true') {
        filters.push(`EXTRACT(YEAR FROM m.timestamp) BETWEEN $${idx} AND $${idx + 1}`);
        values.push(params.start_year, params.end_year);
        idx += 2;
    } else if (params.year) {
        filters.push(`EXTRACT(YEAR FROM m.timestamp) = $${idx}`);
        values.push(params.year);
        idx += 1;
    }

    // Month
    if (params.month_range === 'true') {
        filters.push(`EXTRACT(MONTH FROM m.timestamp) BETWEEN $${idx} AND $${idx + 1}`);
        values.push(params.start_month, params.end_month);
        idx += 2;
    } else if (params.month) {
        filters.push(`EXTRACT(MONTH FROM m.timestamp) = $${idx}`);
        values.push(params.month);
        idx += 1;
    }

    // Day
    if (params.day_range === 'true') {
        filters.push(`EXTRACT(DAY FROM m.timestamp) BETWEEN $${idx} AND $${idx + 1}`);
        values.push(params.start_day, params.end_day);
        idx += 2;
    } else if (params.day) {
        filters.push(`EXTRACT(DAY FROM m.timestamp) = $${idx}`);
        values.push(params.day);
        idx += 1;
    }

    // Hour
    if (params.hour_range === 'true') {
        filters.push(`EXTRACT(HOUR FROM m.timestamp) BETWEEN $${idx} AND $${idx + 1}`);
        values.push(params.start_hour, params.end_hour);
        idx += 2;
    } else if (params.hour) {
        filters.push(`EXTRACT(HOUR FROM m.timestamp) = $${idx}`);
        values.push(params.hour);
        idx += 1;
    }

    return { filter: filters.join(' AND '), values };
}

app.http('getStatsByDateBucket', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const parkingId = request.query.get('parking_id');
        if (!parkingId) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Missing required parameter: parking_id' })
            };
        }

        // Collect all possible date params
        const params = {
            year: request.query.get('year'),
            year_range: request.query.get('year_range'),
            start_year: request.query.get('start_year'),
            end_year: request.query.get('end_year'),
            month: request.query.get('month'),
            month_range: request.query.get('month_range'),
            start_month: request.query.get('start_month'),
            end_month: request.query.get('end_month'),
            day: request.query.get('day'),
            day_range: request.query.get('day_range'),
            start_day: request.query.get('start_day'),
            end_day: request.query.get('end_day'),
            hour: request.query.get('hour'),
            hour_range: request.query.get('hour_range'),
            start_hour: request.query.get('start_hour'),
            end_hour: request.query.get('end_hour')
        };

        const { filter, values } = buildDynamicDateFilter(params);

        if (!filter) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'No valid date filters provided.' })
            };
        }

        let client;
        try {
            client = await getClient();

            // Build the query with dynamic date filter
            const query = `
                SELECT
                    t.sensor_id,
                    si.sensor_alias,
                    p.parking_alias,
                    si.floor,
                    l.floor_alias,
                    SUM(
                        CASE WHEN prev_state = TRUE THEN EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp)) ELSE 0 END
                    ) AS occupied_seconds,
                    SUM(EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp))) AS total_seconds,
                    CASE 
                        WHEN SUM(EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp))) > 0 
                        THEN ROUND(
                            SUM(
                                CASE WHEN prev_state = TRUE THEN EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp)) ELSE 0 END
                            ) / SUM(EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp))) * 100, 2
                        )
                        ELSE 0
                    END AS occupation_percentage,
                    CASE
                        WHEN SUM(EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp))) > 0
                        THEN ROUND(
                            SUM(
                                CASE 
                                    WHEN prev_state = FALSE AND t.current_state = TRUE THEN 1
                                    ELSE 0
                                END
                            ) / SUM(EXTRACT(EPOCH FROM (t.timestamp - prev_timestamp))), 4
                        )
                        ELSE 0
                    END AS normalized_rotation
                FROM (
                    SELECT
                        m.sensor_id,
                        m.timestamp,
                        m.state AS current_state,
                        LAG(m.timestamp) OVER (PARTITION BY m.sensor_id ORDER BY m.timestamp) AS prev_timestamp,
                        LAG(m.state) OVER (PARTITION BY m.sensor_id ORDER BY m.timestamp) AS prev_state
                    FROM measurements m
                    JOIN sensor_info si ON m.sensor_id = si.sensor_id
                    WHERE
                        si.parking_id = $${values.length + 1}
                        AND ${filter}
                ) t
                JOIN sensor_info si ON t.sensor_id = si.sensor_id
                JOIN parking p ON si.parking_id = p.parking_id
                JOIN levels l ON si.parking_id = l.parking_id AND si.floor = l.floor
                WHERE t.prev_timestamp IS NOT NULL
                GROUP BY t.sensor_id, si.sensor_alias, p.parking_alias, si.floor, l.floor_alias
                ORDER BY t.sensor_id;
            `;

            // Add parkingId to the end of values array
            values.push(parkingId);

            const { rows } = await client.query(query, values);

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify(rows)
            };
        } catch (error) {
            context.log.error('Error executing query', error);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Internal server error' })
            };
        } finally {
            if (client) {
                client.release();
            }
        }
    }
});