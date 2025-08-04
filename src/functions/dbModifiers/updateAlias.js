const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('updateAlias', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const body = await request.json();
        const { user_id, field, new_value, parking_id, floor } = body;

        // Validate required parameters
        if (!user_id || !field || !new_value || !parking_id ||
            (field === 'floor_alias' && (floor === undefined || floor === null))) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Missing required parameters.' })
            };
        }

        // Only allow specific fields
        if (!['parking_alias', 'complex', 'floor_alias'].includes(field)) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ error: 'Invalid field to update.' })
            };
        }

        let client;
        try {
            client = await getClient();

            // Check permission and admin status
            const permQuery = `
                SELECT u.administrator
                FROM permissions perm
                JOIN users u ON perm.user_id = u.user_id
                WHERE perm.user_id = $1 AND perm.parking_id = $2
            `;
            const permRes = await client.query(permQuery, [user_id, parking_id]);
            if (!permRes.rowCount || !permRes.rows[0].administrator) {
                return {
                    status: 403,
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                    body: JSON.stringify({ error: 'User does not have admin permission for this parking.' })
                };
            }

            let updateQuery, updateValues;
            if (field === 'parking_alias' || field === 'complex') {
                updateQuery = `UPDATE parking SET ${field} = $1 WHERE parking_id = $2 RETURNING parking_id, ${field}`;
                updateValues = [new_value, parking_id];
            } else if (field === 'floor_alias') {
                updateQuery = `UPDATE levels SET floor_alias = $1 WHERE parking_id = $2 AND floor = $3 RETURNING parking_id, floor, floor_alias`;
                updateValues = [new_value, parking_id, floor];
            }

            const updateRes = await client.query(updateQuery, updateValues);

            if (!updateRes.rowCount) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                    body: JSON.stringify({ error: 'Target record not found.' })
                };
            }

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify({ success: true, updated: updateRes.rows[0] })
            };
        } catch (error) {
            context.log.error('Error updating alias:', error);
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