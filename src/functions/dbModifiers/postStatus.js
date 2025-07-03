/**
 * Author(s): Andres Gomez
 * Brief: HTTP POST endpoint to register a new status for a parking sensor.
 * Date: 2025-06-27
 *
 * Copyright (c) 2025 BY: Nexelium Technological Solutions S.A. de C.V.
 * All rights reserved.
 */

const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');


app.http('postStatus', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        let payload;

        context.log('--- POST /postStatus: Started ---');

        // Parse and validate JSON
        try {
            payload = await request.json();
            context.log('Payload:', JSON.stringify(payload));
        } catch (err) {
            context.log('Invalid JSON payload');
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Invalid JSON payload.' })
            };
        }

        // Check for required fields
        const requiredFields = ['timestamp', 'sensor_id', 'state', 'previous_state_time'];
        const missing = requiredFields.filter(field => !Object.keys(payload).includes(field));

        if (missing.length > 0) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    error: 'Missing required field(s).',
                    requiredFields
                })
            };
        }

        // Type checks
        if (typeof payload.state !== 'boolean') {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: '`state` must be a boolean.' })
            };
        }
        if (typeof payload.previous_state_time !== 'string') {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: '`previous_state_time` must be a string representing a PostgreSQL interval.' })
            };
        }

        // Find schema and sensor_id for sensor_id (example: search in public schema)
        const sensorId = payload.sensor_id;

        const findSchemaQuery = `
            SELECT 'public' AS schema, sensor_id FROM public.sensor_info WHERE sensor_id = $1;
        `;

        try {
            const client = await getClient();

            context.log('Looking for sensor_id:', sensorId);

            const schemaRes = await client.query(findSchemaQuery, [sensorId]);
            context.log('Result from sensor_info lookup:', JSON.stringify(schemaRes.rows));

            if (!schemaRes.rows.length) {
                client.release();
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Sensor ID not found.' })
                };
            }

            const { schema } = schemaRes.rows[0];

            // Insert status into the corresponding schema table
            const insertQuery = `
                INSERT INTO ${schema}.measurements (sensor_id, timestamp, state, previous_state_time)
                VALUES ($1, $2, $3, $4)
                RETURNING *;
            `;

            const insertValues = [
                sensorId,
                payload.timestamp,
                payload.state,
                payload.previous_state_time // e.g., '00:05:00' for 5 minutes
            ];
            const insertRes = await client.query(insertQuery, insertValues);

            // Update current_state in sensor_info
            await client.query(
                `UPDATE public.sensor_info SET current_state = $1 WHERE sensor_id = $2`,
                [payload.state, sensorId]
            );

            client.release();

            context.log('Status registered:', insertRes.rows[0]);

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(insertRes.rows[0])
            };
        } catch (err) {
            context.log('Database error:', err.message);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Internal server error.' })
            };
        }
    }
});