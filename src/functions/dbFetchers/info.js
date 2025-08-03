const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('info', {
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
                WITH parking_sensors AS (
                SELECT
                    parking_id,
                    COUNT(*) AS total_sensors
                FROM sensor_info
                GROUP BY parking_id
                ),
                floor_sensors AS (
                SELECT
                    parking_id,
                    floor,
                    COUNT(*) AS sensors_on_floor
                FROM sensor_info
                GROUP BY parking_id, floor
                ),
                user_permissions AS (
                SELECT
                    perm.parking_id,
                    ARRAY_AGG(ROW(u.user_id, u.username, u.administrator)) AS authorized_users
                FROM permissions perm
                JOIN users u ON perm.user_id = u.user_id
                GROUP BY perm.parking_id
                )

                SELECT
                p.parking_id,
                p.parking_alias,
                p.complex,
                ps.total_sensors AS parking_sensors,
                p.installation_date,
                p.maintenance_date,

                ls.floor_alias,
                fs.sensors_on_floor AS floor_sensors,

                up.authorized_users,

                -- Administrator status of the requesting user
                (SELECT u.administrator FROM users u WHERE u.user_id = $1) AS is_requesting_user_admin

                FROM parking p

                JOIN permissions my_perm ON p.parking_id = my_perm.parking_id
                JOIN parking_sensors ps ON p.parking_id = ps.parking_id
                JOIN levels ls ON p.parking_id = ls.parking_id
                JOIN floor_sensors fs ON ls.parking_id = fs.parking_id AND ls.floor = fs.floor
                JOIN user_permissions up ON p.parking_id = up.parking_id

                WHERE my_perm.user_id = $1;
            `;

            const values = [userId];
            const { rows } = await client.query(query, values);

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' },
                body: JSON.stringify(rows)
            };
        } catch (error) {
            context.log.error('Error executing parking info query:', error);
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