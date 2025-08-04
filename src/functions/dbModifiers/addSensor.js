/**
 * Author: Andres Gomez
 * Last Modified Date: 26-06-2025
 *
 * This function serves as an HTTP POST endpoint to insert a new sensor into the database.
 * It expects a JSON body with sensor details.
 *
 * Example:
 * curl -i -X POST "http://localhost:7071/api/addSensor" -H "Content-Type: application/json" -d '{"sensor_id":"123","location":"A1","type":"temperature"}'
 *
 * Expected Response:
 * {"success":true,"message":"Sensor inserted successfully"}
 */

const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');
const { randomUUID } = require('crypto');

app.http('addSensor', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        context.log(`Http function processed request for url "${request.url}"`);
        let client;
        let body;

        // Parse JSON safely
        try {
            body = await request.json();
            context.log('Payload received:', JSON.stringify(body));
        } catch (err) {
            context.log('Invalid JSON payload');
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: false, message: 'Invalid JSON payload.' })
            };
        }

        const { parking_id, sensor_alias, floor, column, row, type } = body;

        // Validate all required fields
        if (
            !parking_id ||
            !sensor_alias ||
            floor === undefined ||
            column === undefined ||
            row === undefined ||
            !type
        ) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: false, message: 'Missing required sensor fields.' })
            };
        }

        const sensor_id = randomUUID();

        try {
            client = await getClient();

            context.log('Checking if parking exists...');
            const parkingRes = await client.query(
                'SELECT client_id FROM public.parking WHERE parking_id = $1',
                [parking_id]
            );
            context.log('Parking check complete.');

            if (!parkingRes.rows.length) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ success: false, message: 'Parking not found.' })
                };
            }
            const client_id = parkingRes.rows[0].client_id;

            context.log('Checking if level exists...');
            const levelRes = await client.query(
                'SELECT 1 FROM public.levels WHERE parking_id = $1 AND floor = $2',
                [parking_id, floor]
            );
            context.log('Level check complete.');

            if (!levelRes.rows.length) {
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ success: false, message: 'Level (floor) not found for this parking.' })
                };
            }

            context.log('Inserting sensor...');
            const insertQuery = `
                INSERT INTO public.sensor_info (sensor_id, parking_id, sensor_alias, "floor", "column", "row", "type")
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING sensor_id
            `;
            const values = [sensor_id, parking_id, sensor_alias, floor, column, row, type];

            const res = await client.query(insertQuery, values);
            context.log('Sensor inserted.');

            // Increment sensor count in levels
            context.log('Updating no_sensors in levels...');
            await client.query(
                'UPDATE public.levels SET no_sensors = no_sensors + 1 WHERE parking_id = $1 AND floor = $2',
                [parking_id, floor]
            );
            context.log('Levels updated.');

            // Increment sensor count in parking
            context.log('Updating no_sensors in parking...');
            await client.query(
                'UPDATE public.parking SET no_sensors = no_sensors + 1 WHERE parking_id = $1',
                [parking_id]
            );
            context.log('Parking updated.');

            // Increment sensor count in clients
            context.log('Updating no_sensors in clients...');
            await client.query(
                'UPDATE public.clients SET no_sensors = no_sensors + 1 WHERE client_id = $1',
                [client_id]
            );
            context.log('Clients updated.');

            context.log('Returning response...');
            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: true, message: 'Sensor added successfully', sensorID: res.rows[0].sensor_id })
            };
        } catch (error) {
            context.log.error("Error during sensor insert operation:", error);
            return {
                status: 500,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: false, message: `Sensor insert failed: ${error.message}` })
            };
        } finally {
            if (client) client.release();
        }
    }
});