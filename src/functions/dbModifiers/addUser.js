/*


curl -i -X POST "http://localhost:7071/api/addUser" \
-H "Content-Type: application/json" \
-d '{
  "username": "usuario1",
  "password": "clave123",
  "client_id": "your-client-uuid"
}'
*/
const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');
const { randomUUID } = require('crypto');

app.http('addUser', {
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

        const { username, password, client_id } = payload;
        if (!username || !password || !client_id) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing required fields: username, password, client_id.' })
            };
        }

        const user_id = randomUUID();

        let dbClient;
        try {
            dbClient = await getClient();

            const clientRes = await dbClient.query(
                'SELECT 1 FROM public.clients WHERE client_id = $1',
                [client_id]
            );
            if (!clientRes.rows.length) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Client not found.' })
                };
            }

            const insertQuery = `
                INSERT INTO public.users (user_id, username, password, client_id)
                VALUES ($1, $2, $3, $4)
                RETURNING user_id, username, client_id
            `;
            const insertValues = [user_id, username, password, client_id];
            const userRes = await dbClient.query(insertQuery, insertValues);

            await dbClient.query(
                'UPDATE public.clients SET no_users = no_users + 1 WHERE client_id = $1',
                [client_id]
            );

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(userRes.rows[0])
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