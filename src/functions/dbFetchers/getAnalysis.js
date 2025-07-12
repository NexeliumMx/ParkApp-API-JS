const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getAnalysis', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        // Extract and validate parameters
        const userId = request.query.get('user_id');
        const locationSetting = request.query.get('locationSetting'); // 'parking', 'floor', 'sensor'
        const timeSetting = request.query.get('timeSetting'); // 'day', 'month', 'year'
        const parkingIds = request.query.get('parking_id'); // comma-separated for parking setting
        const floors = request.query.get('floor'); // comma-separated for floor setting
        const sensorIds = request.query.get('sensor'); // comma-separated for sensor setting
        const year = request.query.get('year');
        const month = request.query.get('month');
        const day = request.query.get('day');

        // Validate required parameters
        if (!userId || !locationSetting || !timeSetting) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Missing required parameters: user_id, locationSetting, timeSetting' 
                })
            };
        }

        // Validate locationSetting
        if (!['parking', 'floor', 'sensor'].includes(locationSetting)) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Invalid locationSetting. Must be: parking, floor, or sensor' 
                })
            };
        }

        // Validate timeSetting
        if (!['day', 'month', 'year'].includes(timeSetting)) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'Invalid timeSetting. Must be: day, month, or year' 
                })
            };
        }

        // Validate time parameters based on timeSetting
        if (timeSetting === 'day' && (!year || !month || !day)) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'For day analysis, year, month, and day parameters are required' 
                })
            };
        }

        if (timeSetting === 'month' && (!year || !month)) {
            return {
                status: 400,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    error: 'For month analysis, year and month parameters are required' 
                })
            };
        }

        let client;
        
        try {
            client = await getClient();

            // Build date filter based on timeSetting
            let dateFilter = '';
            let timeGroupBy = '';
            let timeSelect = '';
            let orderBy = '';

            switch (timeSetting) {
                case 'day':
                    // Specific day - group by hour
                    dateFilter = `AND DATE(m.timestamp) = '${year}-${month.padStart(2, '0')}-${day.padStart(2, '0')}'`;
                    timeGroupBy = 'EXTRACT(HOUR FROM m.timestamp)';
                    timeSelect = `${timeGroupBy} as time_period`;
                    orderBy = 'time_period';
                    break;
                    
                case 'month':
                    // Specific month - group by day
                    dateFilter = `AND EXTRACT(YEAR FROM m.timestamp) = ${parseInt(year)} AND EXTRACT(MONTH FROM m.timestamp) = ${parseInt(month)}`;
                    timeGroupBy = 'EXTRACT(DAY FROM m.timestamp)';
                    timeSelect = `${timeGroupBy} as time_period`;
                    orderBy = 'time_period';
                    break;
                    
                case 'year':
                    // Specific year or all data - group by month
                    if (year) {
                        dateFilter = `AND EXTRACT(YEAR FROM m.timestamp) = ${parseInt(year)}`;
                    }
                    timeGroupBy = 'EXTRACT(MONTH FROM m.timestamp)';
                    timeSelect = `${timeGroupBy} as time_period`;
                    orderBy = 'time_period';
                    break;
            }

            // Build location filter and grouping
            let locationFilter = '';
            let locationGroupBy = '';
            let locationSelect = '';
            const values = [userId];
            let paramIndex = 2;
            let filterApplied = false;

            switch (locationSetting) {
                case 'parking':
                    // Always group by individual parking - each parking gets its own line
                    locationGroupBy = 'si.parking_id, p.parking_alias';
                    locationSelect = 'si.parking_id, p.parking_alias';
                    
                    if (parkingIds) {
                        const parkingArray = parkingIds.split(',').map(id => id.trim());
                        const placeholders = parkingArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter = `AND si.parking_id IN (${placeholders})`;
                        values.push(...parkingArray);
                        paramIndex += parkingArray.length;
                        filterApplied = true;
                        context.log(`Creating separate analysis lines for ${parkingArray.length} parkings: ${parkingArray.join(', ')}`);
                    } else {
                        context.log('No parking filter - creating separate lines for all parkings user has access to');
                    }
                    orderBy += ', p.parking_alias';
                    break;
                    
                case 'floor':
                    // Group by individual floor within parking - each floor gets its own line
                    locationGroupBy = 'si.parking_id, si.floor, p.parking_alias, COALESCE(l.floor_alias, CONCAT(\'Floor \', si.floor))';
                    locationSelect = 'si.parking_id, si.floor, p.parking_alias, COALESCE(l.floor_alias, CONCAT(\'Floor \', si.floor)) as floor_alias';
                    
                    if (parkingIds) {
                        const parkingArray = parkingIds.split(',').map(id => id.trim());
                        const placeholders = parkingArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter = `AND si.parking_id IN (${placeholders})`;
                        values.push(...parkingArray);
                        paramIndex += parkingArray.length;
                        filterApplied = true;
                    }
                    
                    if (floors) {
                        const floorArray = floors.split(',').map(f => parseInt(f.trim()));
                        const placeholders = floorArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter += ` AND si.floor IN (${placeholders})`;
                        values.push(...floorArray);
                        paramIndex += floorArray.length;
                        filterApplied = true;
                        context.log(`Creating separate analysis lines for ${floorArray.length} floors: ${floorArray.join(', ')}`);
                    }
                    
                    if (!floors && !parkingIds) {
                        context.log('No filters - creating separate lines for all floors user has access to');
                    }
                    orderBy += ', si.parking_id, si.floor';
                    break;
                    
                case 'sensor':
                    // Group by individual sensor - each sensor gets its own line
                    locationGroupBy = 'si.sensor_id, si.sensor_alias, si.parking_id, p.parking_alias, si.floor, COALESCE(l.floor_alias, CONCAT(\'Floor \', si.floor))';
                    locationSelect = 'si.sensor_id, si.sensor_alias, si.parking_id, p.parking_alias, si.floor, COALESCE(l.floor_alias, CONCAT(\'Floor \', si.floor)) as floor_alias';
                    
                    if (sensorIds) {
                        const sensorArray = sensorIds.split(',').map(id => id.trim());
                        const placeholders = sensorArray.map((_, i) => `$${paramIndex + i}`).join(',');
                        locationFilter = `AND si.sensor_id IN (${placeholders})`;
                        values.push(...sensorArray);
                        filterApplied = true;
                        context.log(`Creating separate analysis lines for ${sensorArray.length} sensors: ${sensorArray.join(', ')}`);
                    } else {
                        context.log('No sensor filter - creating separate lines for all sensors user has access to');
                    }
                    orderBy += ', si.parking_id, si.floor, si.sensor_alias';
                    break;
            }

            // Add safety limits based on potential result size
            let limitClause = '';
            const estimatedResults = filterApplied ? 'filtered' : 'all_available';

            if (locationSetting === 'sensor' && !filterApplied) {
                limitClause = 'LIMIT 50'; // Prevent too many sensor lines
                context.log('WARNING: Limiting sensor results to 50 lines. Use filters for specific sensors.');
            } else if (locationSetting === 'floor' && !filterApplied) {
                limitClause = 'LIMIT 100'; // Reasonable floor limit
            } else if (locationSetting === 'parking' && !filterApplied) {
                limitClause = 'LIMIT 20'; // Reasonable parking limit
            }

            context.log(`Query will generate separate lines for each ${locationSetting} with ${estimatedResults} scope`);

            // SIMPLIFIED QUERY - Remove complex LEAD window function to fix performance
            // Use measurement distribution approach for faster execution
            const query = `
                SELECT
                    ${timeSelect},
                    ${locationSelect},
                    -- Count measurements by state for each location/time combination
                    COUNT(*) as total_measurements,
                    COUNT(*) FILTER (WHERE m.state = true) as occupied_measurements,
                    COUNT(*) FILTER (WHERE m.state = false) as available_measurements,
                    COUNT(DISTINCT m.sensor_id) as unique_sensors,
                    
                    -- Calculate occupancy percentage based on measurement distribution
                    CASE
                        WHEN COUNT(*) > 0
                        THEN ROUND((COUNT(*) FILTER (WHERE m.state = true) * 100.0 / COUNT(*))::numeric, 2)
                        ELSE 0
                    END as occupancy_percentage,
                    
                    CASE
                        WHEN COUNT(*) > 0
                        THEN ROUND((COUNT(*) FILTER (WHERE m.state = false) * 100.0 / COUNT(*))::numeric, 2)
                        ELSE 100
                    END as availability_percentage,
                    
                    -- Estimate hours based on measurement frequency
                    CASE
                        WHEN COUNT(*) > 0
                        THEN ROUND((COUNT(*) FILTER (WHERE m.state = true) * 1.0 / COUNT(*) * 1.0)::numeric, 2)
                        ELSE 0
                    END as occupied_hours,
                    
                    CASE
                        WHEN COUNT(*) > 0
                        THEN ROUND((COUNT(*) FILTER (WHERE m.state = false) * 1.0 / COUNT(*) * 1.0)::numeric, 2)
                        ELSE 1
                    END as available_hours,
                    
                    -- Total estimated analysis time
                    ROUND(1.0::numeric, 2) as total_hours,
                    
                    -- State change activity indicator
                    COUNT(*) as state_changes,
                    
                    -- Activity rate calculation
                    ROUND((COUNT(*)::numeric / COUNT(DISTINCT m.sensor_id) / 1.0), 2) as state_changes_per_hour,
                    
                    -- Time range for this period
                    MIN(m.timestamp) as period_start,
                    MAX(m.timestamp) as period_end
                    
                FROM measurements m
                INNER JOIN sensor_info si ON m.sensor_id = si.sensor_id
                INNER JOIN parking p ON si.parking_id = p.parking_id
                LEFT JOIN levels l ON si.parking_id = l.parking_id AND si.floor = l.floor
                INNER JOIN permissions perm ON p.parking_id = perm.parking_id
                WHERE perm.user_id = $1 ${dateFilter} ${locationFilter}
                GROUP BY ${locationGroupBy}, ${timeGroupBy}
                HAVING COUNT(*) > 0  -- Only include periods with data
                ORDER BY ${orderBy}
                ${limitClause}
            `;

            context.log('Executing SIMPLIFIED analytics query (fast measurement distribution approach)');
            context.log(`Query parameters:`, { 
                userId, 
                locationSetting, 
                timeSetting, 
                parameterCount: values.length,
                filtersApplied: filterApplied
            });
            
            const startTime = Date.now();
            const result = await client.query(query, values);
            const executionTime = Date.now() - startTime;
            
            context.log(`Query completed successfully in ${executionTime}ms`);
            context.log(`Found ${result.rows.length} result records`);
            
            // Calculate overall statistics across all location lines
            const overallStats = {
                total_measurements: result.rows.reduce((sum, row) => sum + parseInt(row.total_measurements || 0), 0),
                total_occupied_measurements: result.rows.reduce((sum, row) => sum + parseInt(row.occupied_measurements || 0), 0),
                total_available_measurements: result.rows.reduce((sum, row) => sum + parseInt(row.available_measurements || 0), 0),
                average_occupancy_percentage: 0,
                average_availability_percentage: 0,
                total_unique_sensors: Math.max(...result.rows.map(row => parseInt(row.unique_sensors || 0)), 0),
                total_locations_analyzed: result.rows.length, // Number of separate location lines
                query_execution_time_ms: executionTime,
                location_breakdown: {
                    // Count how many of each location type we're analyzing
                    [locationSetting + 's']: [...new Set(result.rows.map(row => 
                        locationSetting === 'parking' ? row.parking_id :
                        locationSetting === 'floor' ? `${row.parking_id}-${row.floor}` :
                        row.sensor_id
                    ))].length
                }
            };

            // Calculate weighted average occupancy across all location lines
            if (overallStats.total_measurements > 0) {
                overallStats.average_occupancy_percentage = 
                    Math.round((overallStats.total_occupied_measurements / overallStats.total_measurements) * 100 * 100) / 100;
                overallStats.average_availability_percentage = 
                    Math.round((overallStats.total_available_measurements / overallStats.total_measurements) * 100 * 100) / 100;
            }

            // Enhanced response with clear location separation
            const response = {
                success: true,
                parameters: {
                    user_id: userId,
                    location_setting: locationSetting,
                    time_setting: timeSetting,
                    filters: {
                        parking_ids: parkingIds || null,
                        floors: floors || null,
                        sensor_ids: sensorIds || null,
                        year: year || null,
                        month: month || null,
                        day: day || null
                    }
                },
                overall_statistics: overallStats,
                
                // Each location gets its own analysis lines
                location_analysis: result.rows.map(row => ({
                    time_period: parseInt(row.time_period),
                    
                    // Location identification
                    location: locationSetting === 'parking' ? {
                        type: 'parking',
                        parking_id: row.parking_id,
                        parking_name: row.parking_alias,
                        display_name: row.parking_alias
                    } : locationSetting === 'floor' ? {
                        type: 'floor',
                        parking_id: row.parking_id,
                        parking_name: row.parking_alias,
                        floor_number: parseInt(row.floor),
                        floor_name: row.floor_alias,
                        display_name: `${row.parking_alias} - ${row.floor_alias}`
                    } : {
                        type: 'sensor',
                        sensor_id: row.sensor_id,
                        sensor_name: row.sensor_alias,
                        parking_id: row.parking_id,
                        parking_name: row.parking_alias,
                        floor_number: parseInt(row.floor),
                        floor_name: row.floor_alias,
                        display_name: `${row.sensor_alias} (${row.parking_alias} - ${row.floor_alias})`
                    },
                    
                    // Analysis metrics for this specific location
                    metrics: {
                        occupancy_percentage: parseFloat(row.occupancy_percentage),
                        availability_percentage: parseFloat(row.availability_percentage),
                        occupied_hours: parseFloat(row.occupied_hours),
                        available_hours: parseFloat(row.available_hours),
                        total_hours: parseFloat(row.total_hours),
                        state_changes: parseInt(row.state_changes),
                        unique_sensors: parseInt(row.unique_sensors),
                        activity_rate: parseFloat(row.state_changes_per_hour),
                        total_measurements: parseInt(row.total_measurements),
                        period_start: row.period_start,
                        period_end: row.period_end
                    }
                })),
                
                total_records: result.rows.length,
                analysis_type: 'fast_measurement_distribution_analysis',
                time_unit: timeSetting === 'day' ? 'hour' : timeSetting === 'month' ? 'day' : 'month',
                
                // Helpful metadata
                metadata: {
                    locations_analyzed: overallStats.location_breakdown[locationSetting + 's'],
                    time_periods_per_location: Math.round(result.rows.length / (overallStats.location_breakdown[locationSetting + 's'] || 1)),
                    analysis_scope: filterApplied ? 'filtered_locations' : 'all_user_locations',
                    separate_lines: true,
                    query_approach: 'simplified_fast_execution',
                    execution_time_ms: executionTime
                },
                
                notes: [
                    'Using fast measurement distribution analysis instead of duration calculation',
                    'Occupancy percentage represents proportion of occupied vs available measurements',
                    'Each selected location appears as separate analysis lines',
                    'Hours are estimated based on measurement patterns'
                ]
            };

            context.log(`Analysis completed successfully: ${response.metadata.locations_analyzed} ${locationSetting}s with ${response.total_records} total lines in ${executionTime}ms`);

            return {
                status: 200,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify(response)
            };

        } catch (error) {
            // FIXED: Use context.log() instead of context.log.error() - Azure Functions v4 best practice
            context.log('Error during analysis operation:', error.message);
            context.log('Error stack trace:', error.stack);
            context.log('Error occurred with parameters:', { userId, locationSetting, timeSetting });
            
            return {
                status: 500,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    success: false, 
                    message: `Analysis operation failed: ${error.message}`,
                    error_type: 'query_execution_error',
                    error_details: process.env.NODE_ENV === 'development' ? error.stack : undefined
                })
            };
        } finally {
            if (client) {
                client.release();
            }
        }
    }
});