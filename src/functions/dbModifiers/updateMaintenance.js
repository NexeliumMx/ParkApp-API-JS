const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('updateMaintenance', {
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

        const { parking_id, maintenance_date } = payload;
        if (!parking_id || !maintenance_date) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing required fields: parking_id, maintenance_date.' })
            };
        }

        let dbClient;
        try {
            dbClient = await getClient();

            const updateQuery = `
                UPDATE public.parking
                SET maintenance_date = $1
                WHERE parking_id = $2
                RETURNING parking_id, maintenance_date
            `;
            const updateValues = [maintenance_date, parking_id];
            const updateRes = await dbClient.query(updateQuery, updateValues);

            if (!updateRes.rowCount) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Parking not found for the specified parking_id.' })
                };
            }

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(updateRes.rows[0])
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