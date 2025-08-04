const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('grantPermission', {
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

        const { user_id, parking_id } = payload;
        if (!user_id || !parking_id) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing required fields: user_id, parking_id.' })
            };
        }

        let dbClient;
        try {
            dbClient = await getClient();

            // Get client_id for user
            const userRes = await dbClient.query(
                'SELECT client_id FROM public.users WHERE user_id = $1',
                [user_id]
            );
            if (!userRes.rows.length) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'User not found.' })
                };
            }
            const userClientId = userRes.rows[0].client_id;

            // Get client_id for parking
            const parkingRes = await dbClient.query(
                'SELECT client_id FROM public.parking WHERE parking_id = $1',
                [parking_id]
            );
            if (!parkingRes.rows.length) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Parking not found.' })
                };
            }
            const parkingClientId = parkingRes.rows[0].client_id;

            // Check if both client_ids match
            if (userClientId !== parkingClientId) {
                return {
                    status: 403,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'User and parking do not belong to the same client.' })
                };
            }

            // Insert permission
            const insertQuery = `
                INSERT INTO public.permissions (user_id, parking_id)
                VALUES ($1, $2)
                RETURNING user_id, parking_id
            `;
            const insertValues = [user_id, parking_id];
            const permRes = await dbClient.query(insertQuery, insertValues);

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(permRes.rows[0])
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