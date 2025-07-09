const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getAnalysis', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        const userId = request.query.get('user_id');
        const timeInterval = request.query.get('time_interval'); // 'year', 'month', 'day'
        const year = request.query.get('year');
        const month = request.query.get('month');
        const day = request.query.get('day');
        const locationSetting = request.query.get('location_setting'); // 'parking', 'floor', 'sensor'
        const parkingIds = request.query.get('parking_ids'); // comma-separated
        const floors = request.query.get('floors'); // comma-separated
        const sensorIds = request.query.get('sensor_ids'); // comma-separated

        // Validate required parameters
        if (!userId || !timeInterval || !locationSetting) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Missing required parameters: user_id, time_interval, location_setting' 
                })
            };
        }

        // Validate time interval values
        if (!['year', 'month', 'day'].includes(timeInterval)) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Invalid time_interval. Must be: year, month, or day' 
                })
            };
        }

        // Validate location setting values
        if (!['parking', 'floor', 'sensor'].includes(locationSetting)) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Invalid location_setting. Must be: parking, floor, or sensor' 
                })
            };
        }

        let client;
        try {
            client = await getClient();

            // Build the base query with time grouping
            let timeGroupBy = '';
            let timeSelect = '';
            let dateFilter = '';
            
            switch (timeInterval) {
                case 'year':
                    timeGroupBy = 'EXTRACT(MONTH FROM m.timestamp)';
                    timeSelect = `${timeGroupBy} as month`;
                    if (year) {
                        dateFilter = `AND EXTRACT(YEAR FROM m.timestamp) = ${parseInt(year)}`;
                    }
                    break;
                    
                case 'month':
                    timeGroupBy = 'EXTRACT(DAY FROM m.timestamp)';
                    timeSelect = `${timeGroupBy} as day`;
                    if (year && month) {
                        dateFilter = `AND EXTRACT(YEAR FROM m.timestamp) = ${parseInt(year)} AND EXTRACT(MONTH FROM m.timestamp) = ${parseInt(month)}`;
                    }
                    break;
                    
                case 'day':
                    timeGroupBy = 'EXTRACT(HOUR FROM m.timestamp)';
                    timeSelect = `${timeGroupBy} as hour`;
                    if (year && month && day) {
                        dateFilter = `AND DATE(m.timestamp) = '${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}'`;
                    }
                    break;
            }

            // Build location grouping and filters
            let locationGroupBy = '';
            let locationSelect = '';
            let locationFilter = '';
            const values = [userId];
            let paramIndex = 2;

            switch (locationSetting) {
                case 'parking':
                    locationGroupBy = 's.parking_id';
                    locationSelect = `${locationGroupBy}, p.parking_alias`;
                    
                    if (parkingIds) {
                        const parkingArray = parkingIds.split(',').map(id => id.trim());
                        const placeholders = parkingArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter = `AND s.parking_id IN (${placeholders})`;
                        values.push(...parkingArray);
                        paramIndex += parkingArray.length;
                    }
                    break;
                    
                case 'floor':
                    locationGroupBy = 's.parking_id, s.floor';
                    locationSelect = `${locationGroupBy}, p.parking_alias, l.floor_alias`;
                    
                    if (parkingIds) {
                        const parkingArray = parkingIds.split(',').map(id => id.trim());
                        const placeholders = parkingArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter = `AND s.parking_id IN (${placeholders})`;
                        values.push(...parkingArray);
                        paramIndex += parkingArray.length;
                    }
                    
                    if (floors) {
                        const floorArray = floors.split(',').map(f => parseInt(f.trim()));
                        const placeholders = floorArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter += ` AND s.floor IN (${placeholders})`;
                        values.push(...floorArray);
                        paramIndex += floorArray.length;
                    }
                    break;
                    
                case 'sensor':
                    locationGroupBy = 's.sensor_id';
                    locationSelect = `${locationGroupBy}, s.sensor_alias`;
                    
                    if (sensorIds) {
                        const sensorArray = sensorIds.split(',').map(id => id.trim());
                        const placeholders = sensorArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter = `AND s.sensor_id IN (${placeholders})`;
                        values.push(...sensorArray);
                    }
                    break;
            }

            // Build the complete query
            let query = `
                SELECT 
                    ${timeSelect},
                    ${locationSelect},
                    COUNT(*) as total_measurements,
                    COUNT(*) FILTER (WHERE m.state = true) as occupied_count,
                    COUNT(*) FILTER (WHERE m.state = false) as available_count,
                    ROUND(
                        (COUNT(*) FILTER (WHERE m.state = true)::decimal / COUNT(*)) * 100, 2
                    ) as occupancy_percentage,
                    ROUND(
                        (COUNT(*) FILTER (WHERE m.state = false)::decimal / COUNT(*)) * 100, 2
                    ) as availability_percentage,
                    AVG(
                        CASE WHEN m.state = true THEN 
                            EXTRACT(EPOCH FROM m.previous_state_time) / 3600.0 
                        END
                    ) as avg_occupied_duration_hours,
                    AVG(
                        CASE WHEN m.state = false THEN 
                            EXTRACT(EPOCH FROM m.previous_state_time) / 3600.0 
                        END
                    ) as avg_available_duration_hours
                FROM measurements m
                JOIN sensor_info s ON m.sensor_id = s.sensor_id
                JOIN permissions perm ON s.parking_id = perm.parking_id
                JOIN parking p ON s.parking_id = p.parking_id
            `;

            // Add level join if needed for floor analysis
            if (locationSetting === 'floor') {
                query += ` JOIN levels l ON s.parking_id = l.parking_id AND s.floor = l.floor`;
            }

            query += ` WHERE perm.user_id = $1 ${dateFilter} ${locationFilter}`;
            query += ` GROUP BY ${timeGroupBy}, ${locationGroupBy}`;
            query += ` ORDER BY ${timeGroupBy}`;

            if (locationSetting === 'parking') {
                query += `, p.parking_alias`;
            } else if (locationSetting === 'floor') {
                query += `, s.parking_id, s.floor`;
            } else {
                query += `, s.sensor_alias`;
            }

            context.log(`Executing query: ${query}`);
            context.log(`With values:`, values);
            
            const res = await client.query(query, values);
            
            // Calculate overall averages
            const overallStats = {
                total_measurements: res.rows.reduce((sum, row) => sum + parseInt(row.total_measurements), 0),
                overall_occupancy_percentage: 0,
                overall_availability_percentage: 0,
                avg_occupied_duration_hours: 0,
                avg_available_duration_hours: 0
            };

            if (res.rows.length > 0) {
                const totalOccupied = res.rows.reduce((sum, row) => sum + parseInt(row.occupied_count), 0);
                const totalAvailable = res.rows.reduce((sum, row) => sum + parseInt(row.available_count), 0);
                
                overallStats.overall_occupancy_percentage = 
                    Math.round((totalOccupied / overallStats.total_measurements) * 100 * 100) / 100;
                overallStats.overall_availability_percentage = 
                    Math.round((totalAvailable / overallStats.total_measurements) * 100 * 100) / 100;
                
                // Calculate weighted averages for duration
                const validOccupiedRows = res.rows.filter(row => row.avg_occupied_duration_hours !== null);
                const validAvailableRows = res.rows.filter(row => row.avg_available_duration_hours !== null);
                
                if (validOccupiedRows.length > 0) {
                    overallStats.avg_occupied_duration_hours = 
                        validOccupiedRows.reduce((sum, row) => sum + parseFloat(row.avg_occupied_duration_hours), 0) / validOccupiedRows.length;
                }
                
                if (validAvailableRows.length > 0) {
                    overallStats.avg_available_duration_hours = 
                        validAvailableRows.reduce((sum, row) => sum + parseFloat(row.avg_available_duration_hours), 0) / validAvailableRows.length;
                }
            }

            const result = {
                time_interval: timeInterval,
                location_setting: locationSetting,
                filters: {
                    year: year || null,
                    month: month || null,
                    day: day || null,
                    parking_ids: parkingIds || null,
                    floors: floors || null,
                    sensor_ids: sensorIds || null
                },
                overall_statistics: overallStats,
                detailed_data: res.rows,
                total_records: res.rows.length
            };

            context.log("Analysis query executed successfully");

            return {
                status: 200,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(result)
            };

        } catch (error) {
            context.log.error("Error during analysis operation:", error);
            return {
                status: 500,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    success: false, 
                    message: `Analysis operation failed: ${error.message}` 
                })
            };
        } finally {
            if (client) {
                client.release();
            }
        }
    }
});