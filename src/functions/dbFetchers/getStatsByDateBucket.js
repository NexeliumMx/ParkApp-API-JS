const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

function buildDateRange(params) {
    const {
        start_year, end_year,
        start_month, end_month,
        start_day, end_day
    } = params;

    let startDate = '';
    let endDate = '';

    if (start_year && end_year) {
        const sMonth = start_month ? start_month.padStart(2, '0') : '01';
        const eMonth = end_month ? end_month.padStart(2, '0') : '12';
        const sDay = start_day ? start_day.padStart(2, '0') : '01';
        const eDay = end_day ? end_day.padStart(2, '0') : '31';

        startDate = `${start_year}-${sMonth}-${sDay}`;
        endDate = `${end_year}-${eMonth}-${eDay}`;
    }
    return { startDate, endDate };
}

app.http('getStatsByDateBucket', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const useRange = request.query.get('use_range') === 'true';
        const startDate = request.query.get('start_date');
        const endDate = request.query.get('end_date');
        const exactDate = request.query.get('exact_date');
        const parkingId = request.query.get('parking_id');

        // Dynamic params for building date range
        const start_year = request.query.get('start_year');
        const end_year = request.query.get('end_year');
        const start_month = request.query.get('start_month');
        const end_month = request.query.get('end_month');
        const start_day = request.query.get('start_day');
        const end_day = request.query.get('end_day');

        let finalStartDate = startDate;
        let finalEndDate = endDate;

        if (start_year && end_year) {
            const range = buildDateRange({
                start_year, end_year,
                start_month, end_month,
                start_day, end_day
            });
            finalStartDate = range.startDate;
            finalEndDate = range.endDate;
        }

        // Only require parkingId and date filters, no bucket
        if (!parkingId || (useRange && (!finalStartDate || !finalEndDate)) || (!useRange && !exactDate)) {
            return {
                status: 400,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    error: 'Missing required parameters (parking_id, date filters)'
                })
            };
        }

        let client;
        try {
            client = await getClient();

            // Query to calculate occupation percentage and rotation per sensor_id, including sensor and parking info
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
                        si.parking_id = $5
                        AND (
                            ($1::BOOLEAN = TRUE AND m.timestamp BETWEEN $2::DATE AND $3::DATE)
                            OR
                            ($1::BOOLEAN = FALSE AND DATE(m.timestamp) = $4::DATE)
                        )
                ) t
                JOIN sensor_info si ON t.sensor_id = si.sensor_id
                JOIN parking p ON si.parking_id = p.parking_id
                JOIN levels l ON si.parking_id = l.parking_id AND si.floor = l.floor
                WHERE t.prev_timestamp IS NOT NULL
                GROUP BY t.sensor_id, si.sensor_alias, p.parking_alias, si.floor, l.floor_alias
                ORDER BY t.sensor_id;
            `;

            // Remove bucket from params, only use useRange, dates, parkingId
            const params = useRange
                ? [true, finalStartDate, finalEndDate, null, parkingId]
                : [false, null, null, exactDate, parkingId];

            const { rows } = await client.query(query, params);

            return {
                status: 200,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(rows)
            };
        } catch (error) {
            context.log.error('Error executing query', error);

            return {
                status: 500,
                headers: {
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    error: 'Internal server error'
                })
            };
        } finally {
            if (client) {
                client.release();
            }
        }
    }
});