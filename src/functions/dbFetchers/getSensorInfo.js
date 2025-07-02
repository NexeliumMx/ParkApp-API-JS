
const { app } = require('@azure/functions');
const { getClient } = require('./dbClient');

app.http('fetchSensorInfo', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        context.log(`Http function processed request for url "${request.url}"`);

        const userId = request.query.get('user_id');

        context.log(`Received user_id: ${userId}`);

        if (!userId) {
            context.log('user_id is missing in the request');
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: false, message: 'user_id is required' })
            };
        }

        try {
            const client = await getClient();

            // Query to fetch sensor Info by user access
            const query = `
                SELECT 
                    s.sensor_id,
                    s.parking_id,
                    s.floor, 
                    s.current_state, 
                    p.parking_alias, 
                    p.complex
                FROM 
                    sensor_info s
                JOIN 
                    parking p ON s.parking_id = p.parking_id
                JOIN 
                    permissions i ON s.parking_id = i.parking_id
                WHERE 
                    i.user_id = $1;
            `;
            const values = [userId];
            context.log(`Executing query: ${query} with values: ${values}`);
            const res = await client.query(query, values);
            client.release();

            context.log("Database query executed successfully:", res.rows);

            return {
                status: 200,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(res.rows)
            };
        } catch (error) {
            context.log.error("Error during database operation:", error);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: false, message: `Database operation failed: ${error.message}` })
            };
        }
    }
});