const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('updateFlags', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        let payload;
        try {
            payload = await request.json();
            context.log('Payload:', JSON.stringify(payload));
        } catch (err) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Invalid JSON payload.' })
            };
        }

        const { sensor_id, low_battery, connection_error, error_flag } = payload;
        if (!sensor_id ||
            typeof low_battery !== 'boolean' ||
            typeof connection_error !== 'boolean' ||
            typeof error_flag !== 'boolean'
        ) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing or invalid fields: sensor_id, low_battery, connection_error, error_flag.' })
            };
        }

        let dbClient;
        try {
            dbClient = await getClient();

            // Update flags for the specified sensor
            const updateQuery = `
                UPDATE public.sensor_info
                SET low_battery = $1,
                    connection_error = $2,
                    error_flag = $3
                WHERE sensor_id = $4
                RETURNING sensor_id
            `;
            const updateValues = [low_battery, connection_error, error_flag, sensor_id];
            const updateRes = await dbClient.query(updateQuery, updateValues);

            if (!updateRes.rowCount) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Sensor not found for the specified sensor_id.' })
                };
            }

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: true, updated_sensor: updateRes.rows[0].sensor_id })
            };
        } catch (err) {
            context.log('Database error:', err.message);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Internal server error.' })
            };
        } finally {
            if (dbClient) dbClient.release();
        }
    }
});