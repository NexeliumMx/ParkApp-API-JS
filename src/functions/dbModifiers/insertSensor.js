/**
 * Author: Andres Gomez
 * Last Modified Date: 26-06-2025
 *
 * This function serves as an HTTP POST endpoint to insert a new sensor into the database.
 * It expects a JSON body with sensor details.
 *
 * Example:
 * curl -i -X POST "http://localhost:7071/api/insertSensor" -H "Content-Type: application/json" -d '{"sensor_id":"123","location":"A1","type":"temperature"}'
 *
 * Expected Response:
 * {"success":true,"message":"Sensor inserted successfully"}
 */

const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('insertSensor', {
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

        const { sensor_id, parking_id, sensor_alias, floor, column, row, type } = body;

        // Validate all required fields
        if (
            !sensor_id ||
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

        try {
            client = await getClient();

            const insertQuery = `
                INSERT INTO public.sensor_info (sensor_id, parking_id, sensor_alias, "floor", "column", "row", "type")
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING sensor_id
            `;
            const values = [sensor_id, parking_id, sensor_alias, floor, column, row, type];

            context.log('Insert query:', insertQuery, 'Values:', values);

            const res = await client.query(insertQuery, values);

            context.log("Sensor inserted successfully:", res.rows[0].sensorid);

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: true, message: 'Sensor inserted successfully', sensorID: res.rows[0].sensorid })
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