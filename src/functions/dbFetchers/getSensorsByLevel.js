const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getSensorsByLevel', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const parkingId = request.query.get('parking_id');
        const floor = request.query.get('floor');
        
        if (!parkingId || !floor) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Missing required parameters: parking_id, floor' 
                })
            };
        }

        let client;
        try {
            client = await getClient();

            // Query to fetch sensors by parking_id and floor
            const query = `
                SELECT 
                    sensor_id,
                    sensor_alias
                FROM 
                    public.sensor_info
                WHERE 
                    parking_id = $1 AND floor = $2
                ORDER BY 
                    sensor_id;
            `;

            const values = [parkingId, parseInt(floor)];
            context.log(`Executing query: ${query} with values: ${values}`);
            
            const res = await client.query(query, values);
            client.release();

            context.log("Database query executed successfully:", res.rows);

            return {
                status: 200,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(res.rows)
            };
        } catch (error) {
            context.log.error("Error during database operation:", error);
            return {
                status: 500,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    success: false, 
                    message: `Database operation failed: ${error.message}` 
                })
            };
        }
    }
});