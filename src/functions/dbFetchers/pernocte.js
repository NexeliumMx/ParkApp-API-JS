const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('pernocte', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const userId = request.query.get('user_id');
        if (!userId) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Missing required parameter: user_id' })
            };
        }

        let client;
        try {
            client = await getClient();

            const query = `
                WITH latest AS (
                  SELECT
                    sensor_id,
                    last(timestamp, timestamp) AS last_status_time,
                    last(state, timestamp) AS state
                  FROM public.measurements
                  GROUP BY sensor_id
                ),
                entries AS (
                  SELECT
                    m.sensor_id,
                    last(m.timestamp, m.timestamp) AS entry_time
                  FROM measurements m
                  JOIN latest l ON m.sensor_id = l.sensor_id
                  WHERE m.state = true AND m.timestamp <= l.last_status_time
                  GROUP BY m.sensor_id
                ),
                enriched AS (
                  SELECT
                    e.sensor_id,
                    s.sensor_alias,
                    p.parking_alias,
                    s.parking_id,
                    e.entry_time,
                    l.last_status_time,
                    p.timezone,
                    p.horario_cierre,
                    CURRENT_TIMESTAMP AT TIME ZONE p.timezone AS local_timestamp,
                    AGE(CURRENT_TIMESTAMP AT TIME ZONE p.timezone, e.entry_time) AS duration_parked
                  FROM entries e
                  JOIN latest l ON e.sensor_id = l.sensor_id
                  JOIN sensor_info s ON e.sensor_id = s.sensor_id
                  JOIN parking p ON s.parking_id = p.parking_id
                  JOIN permissions perm ON p.parking_id = perm.parking_id
                  WHERE perm.user_id = $1
                )
                SELECT
                  sensor_id,
                  parking_id,
                  sensor_alias,
                  parking_alias,
                  entry_time,
                  last_status_time,
                  duration_parked,
                  horario_cierre[EXTRACT(DOW FROM entry_time)::INT] AS closing_time_on_arrival
                FROM enriched
                WHERE local_timestamp > (entry_time::date + horario_cierre[EXTRACT(DOW FROM entry_time)::INT])::timestamp;
            `;

            const values = [userId];
            const { rows } = await client.query(query, values);

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify(rows)
            };
        } catch (error) {
            context.log.error('Error executing overstaying cars query:', error);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Internal server error' })
            };
        } finally {
            if (client) client.release();
        }
    }
});