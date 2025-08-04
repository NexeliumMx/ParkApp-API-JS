/**
 * Author(s): Andres Gomez
 * Brief: Azure Function to get all sensors accessible by a user through permissions
 * Date: 2025-07-08
 *
 * Copyright (c) 2025 BY: Nexelium Technological Solutions S.A. de C.V.
 * All rights reserved.
 */

const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('getSensorsByUser', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        let client;
        
        try {
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

            context.log(`Fetching sensors for user: ${userId}`);
            
            client = await getClient();
            
            const query = `
                SELECT 
                    s.sensor_id,
                    s.sensor_alias,
                    s.parking_id,
                    s.floor,
                    s.current_state,
                    p.parking_alias,
                    p.complex
                FROM public.sensor_info s
                JOIN public.parking p ON s.parking_id = p.parking_id
                JOIN public.permissions perm ON s.parking_id = perm.parking_id
                WHERE perm.user_id = $1
                ORDER BY p.complex, p.parking_alias, s.floor, s.sensor_alias
            `;
            
            const result = await client.query(query, [userId]);
            
            context.log(`Found ${result.rows.length} sensors for user ${userId}`);
            
            return {
                status: 200,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({
                    success: true,
                    sensors: result.rows,
                    count: result.rows.length
                })
            };
            
        } catch (error) {
            context.log.error('Error fetching sensors by user:', error);
            return {
                status: 500,
                headers: { 
                    'Content-Type': 'application/json',
                    'Access-Control-Allow-Origin': '*'
                },
                body: JSON.stringify({ 
                    success: false,
                    error: `Failed to fetch sensors: ${error.message}` 
                })
            };
        } finally {
            if (client) {
                client.release();
            }
        }
    }
});