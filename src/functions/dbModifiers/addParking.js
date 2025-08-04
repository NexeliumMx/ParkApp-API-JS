/*
Esta función HTTP de Azure recibe los datos de un nuevo estacionamiento,
 genera un UUID para el parking, valida que el cliente exista, inserta
  el registro en la tabla parking con los contadores en cero y, si el
   complejo es nuevo para ese cliente, incrementa el contador de 
   complejos y siempre incrementa el de estacionamientos en la tabla 
   clients.
   
curl -i -X POST "http://localhost:7071/api/addParking" \
-H "Content-Type: application/json" \
-d '{
  "parking_alias": "new-parking-alias",
  "client_id": "<existing-client-id>",
  "complex": "Complex Name",
  "installation_date": "yyyy-mm-dd"
}'
*/

const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');
const { randomUUID } = require('crypto');

app.http('addParking', {
    methods: ['POST'],
    authLevel: 'anonymous',
    handler: async (request, context) => {
        let payload;
        try {
            payload = await request.json();
            context.log('Payload:', JSON.stringify(payload));
        } catch (err) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Invalid JSON payload.' })
            };
        }

        const { parking_alias, client_id, complex, installation_date } = payload;
        if (!parking_alias || !client_id || !complex || !installation_date) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing required fields: parking_alias, client_id, complex, installation_date.' })
            };
        }

        // Always set maintenance_date to installation_date
        const maintenance_date = installation_date;

        const parking_id = randomUUID();

        try {
            const dbClient = await getClient();

            // Check if client exists
            const clientRes = await dbClient.query(
                'SELECT * FROM public.clients WHERE client_id = $1',
                [client_id]
            );
            if (!clientRes.rows.length) {
                dbClient.release();
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Client not found.' })
                };
            }

            // Check if complex exists for this client
            const complexRes = await dbClient.query(
                'SELECT 1 FROM public.parking WHERE client_id = $1 AND complex = $2 LIMIT 1',
                [client_id, complex]
            );
            let incrementComplex = false;
            if (!complexRes.rows.length) {
                incrementComplex = true;
            }

            // Insert new parking
            const insertQuery = `
                INSERT INTO public.parking (
                    parking_id, parking_alias, client_id, complex, installation_date, maintenance_date, no_sensors, no_levels
                ) VALUES ($1, $2, $3, $4, $5, $6, 0, 0)
                RETURNING *;
            `;
            const insertValues = [
                parking_id,
                parking_alias,
                client_id,
                complex,
                installation_date,
                maintenance_date
            ];
            const parkingRes = await dbClient.query(insertQuery, insertValues);

            // Update client stats
            let updateClientQuery = 'UPDATE public.clients SET no_parkings = no_parkings + 1';
            if (incrementComplex) {
                updateClientQuery += ', no_complexes = no_complexes + 1';
            }
            updateClientQuery += ' WHERE client_id = $1';
            await dbClient.query(updateClientQuery, [client_id]);

            dbClient.release();

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(parkingRes.rows[0])
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