const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getKonvaInfo', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const user_id = request.query.get('user_id');
        const parking_id = request.query.get('parking_id');
        const floor = request.query.get('floor');

        if (!user_id || !parking_id || floor === undefined || floor === null) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Missing required parameters: user_id, parking_id, floor' })
            };
        }

        let client;
        try {
            client = await getClient();

            // Check permission
            const permQuery = `
                SELECT 1 FROM permissions
                WHERE user_id = $1 AND parking_id = $2
                LIMIT 1
            `;
            const permRes = await client.query(permQuery, [user_id, parking_id]);
            if (!permRes.rowCount) {
                return {
                    status: 403,
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                    body: JSON.stringify({ error: 'User does not have permission for this parking.' })
                };
            }

            // Retrieve stage_info and layout_info
            const infoQuery = `
                SELECT stage_info, layout_info
                FROM levels
                WHERE parking_id = $1 AND floor = $2
                LIMIT 1
            `;
            const infoRes = await client.query(infoQuery, [parking_id, floor]);
            if (!infoRes.rowCount) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                    body: JSON.stringify({ error: 'Level not found.' })
                };
            }

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify(infoRes.rows[0])
            };
        } catch (error) {
            context.log.error('Error fetching Konva info:', error);
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