/**
 * Author: Arturo Vargas Cuevas
 * Last Modified Date: 20-11-2024
 *
 * This function serves as an HTTP GET endpoint to test the database connection.
 * It verifies that the API can successfully connect to the database and returns the result.
 *
 * Example:
 * Test database connection:
 * curl -i -X GET "http://localhost:7071/api/testDBconnection"
 *
 * Expected Response:
 * {"success":true,"message":"Connection test successful"}
 */

const { app } = require('@azure/functions');
const { getClient } = require('./dbClient');

app.http('testDBconnection', {
    methods: ['GET'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        context.log(`Http function processed request for url "${request.url}"`);

        try {
            const client = await getClient();  // Reuse the connected client from dbClient.js

            // Example query to test connection (modify as needed)
            const res = await client.query("SELECT 'Connection test successful' AS message");
            client.release(); // Release client back to the pool

            context.log("Database query executed successfully:", res.rows[0].message);

            // Return success message as HTTP response
            return {
                status: 200,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ success: true, message: res.rows[0].message })
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