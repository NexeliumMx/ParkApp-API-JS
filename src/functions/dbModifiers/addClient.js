/*
This function takes a new client alias and autogenerates a client ID 
and posts it to the database. It returns the newly created client object.

curl -i -X POST "http://localhost:7071/api/addClient" \
-H "Content-Type: application/json" \
-d '{"client_alias":"My New Client"}'

*/

const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');
const { randomUUID } = require('crypto');

app.http('addClient', {
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

        if (!payload.client_alias) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing client_alias.' })
            };
        }

        const client_id = randomUUID();
        const insertQuery = `
            INSERT INTO public.clients (
                client_id, client_alias, no_users, no_complexes, no_parkings, no_floors, no_sensors
            ) VALUES ($1, $2, 0, 0, 0, 0, 0)
            RETURNING *;
        `;
        const insertValues = [client_id, payload.client_alias];

        try {
            const dbClient = await getClient();
            const res = await dbClient.query(insertQuery, insertValues);
            dbClient.release();

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(res.rows[0])
            };
        } catch (err) {
            context.log('Database error:', err.message);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Internal server error.' })
            };
        }
    }
});