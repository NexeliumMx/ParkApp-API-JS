const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getAvailableDates', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const userId = request.query.get('user_id');
        const parkingIds = request.query.get('parking_ids'); // comma-separated
        const floors = request.query.get('floors'); // comma-separated
        const sensorIds = request.query.get('sensor_ids'); // comma-separated
        
        if (!userId) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Missing required parameter: user_id' 
                })
            };
        }

        let client;
        try {
            client = await getClient();

            // Build dynamic query based on provided parameters
            let query = `
                SELECT DISTINCT 
                    DATE(m.timestamp) as available_date,
                    EXTRACT(YEAR FROM m.timestamp) as year,
                    EXTRACT(MONTH FROM m.timestamp) as month,
                    EXTRACT(DAY FROM m.timestamp) as day
                FROM measurements m
                JOIN sensor_info s ON m.sensor_id = s.sensor_id
                JOIN permissions p ON s.parking_id = p.parking_id
                WHERE p.user_id = $1
            `;

            const values = [userId];
            let paramIndex = 2;

            // Add parking filter if provided
            if (parkingIds) {
                const parkingArray = parkingIds.split(',').map(id => id.trim());
                const placeholders = parkingArray.map((_, i) => `$${paramIndex + i}`).join(',');
                query += ` AND s.parking_id IN (${placeholders})`;
                values.push(...parkingArray);
                paramIndex += parkingArray.length;
            }

            // Add floor filter if provided
            if (floors) {
                const floorArray = floors.split(',').map(f => parseInt(f.trim()));
                const placeholders = floorArray.map((_, i) => `$${paramIndex + i}`).join(',');
                query += ` AND s.floor IN (${placeholders})`;
                values.push(...floorArray);
                paramIndex += floorArray.length;
            }

            // Add sensor filter if provided
            if (sensorIds) {
                const sensorArray = sensorIds.split(',').map(id => id.trim());
                const placeholders = sensorArray.map((_, i) => `$${paramIndex + i}`).join(',');
                query += ` AND s.sensor_id IN (${placeholders})`;
                values.push(...sensorArray);
            }

            query += ` ORDER BY available_date DESC`;

            context.log(`Executing query: ${query} with values:`, values);
            
            const res = await client.query(query, values);
            client.release();

            // Group dates by year and month for easier frontend consumption
            const datesByYear = {};
            
            res.rows.forEach(row => {
                const year = row.year;
                const month = row.month;
                
                if (!datesByYear[year]) {
                    datesByYear[year] = {};
                }
                if (!datesByYear[year][month]) {
                    datesByYear[year][month] = [];
                }
                
                datesByYear[year][month].push({
                    date: row.available_date,
                    day: row.day
                });
            });

            const result = {
                available_dates: res.rows.map(row => row.available_date),
                grouped_by_year_month: datesByYear,
                total_dates: res.rows.length
            };

            context.log("Available dates query executed successfully");

            return {
                status: 200,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(result)
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