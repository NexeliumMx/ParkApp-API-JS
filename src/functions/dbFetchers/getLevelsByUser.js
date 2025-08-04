const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getLevelsByUser', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const userId = request.query.get('user_id');
        
        if (!userId) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ error: 'Missing required parameter: user_id' })
            };
        }

        let client;
        try {
            client = await getClient();

            // Query to fetch towers/complexes and their levels for a specific user
            const query = `
                SELECT DISTINCT
                    p.parking_id,
                    p.complex,
                    p.parking_alias,
                    l.floor,
                    l.floor_alias
                FROM 
                    parking p
                JOIN 
                    permissions perm ON p.parking_id = perm.parking_id
                JOIN
                    levels l ON p.parking_id = l.parking_id
                WHERE 
                    perm.user_id = $1
                ORDER BY 
                    p.complex, p.parking_alias, l.floor;
            `;

            const values = [userId];
            context.log(`Executing query: ${query} with values: ${values}`);
            
            const res = await client.query(query, values);
            client.release();

            // Transform the flat result into a nested structure
            const towersMap = new Map();
            
            res.rows.forEach(row => {
                const towerId = row.parking_id;
                
                if (!towersMap.has(towerId)) {
                    towersMap.set(towerId, {
                        parking_id: row.parking_id,
                        complex: row.complex,
                        parking_alias: row.parking_alias,
                        levels: []
                    });
                }
                
                towersMap.get(towerId).levels.push({
                    floor: row.floor,
                    floor_alias: row.floor_alias
                });
            });

            const towers = Array.from(towersMap.values());
            
            context.log("Database query executed successfully:", towers);

            return {
                status: 200,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(towers)
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