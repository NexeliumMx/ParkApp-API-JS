const { app } = require('@azure/functions');
const { getClient } = require('../dbClient');

app.http('addLevel', {
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

        const { parking_id, floor, floor_alias } = payload;
        if (!parking_id || floor === undefined || !floor_alias) {
            return {
                status: 400,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ error: 'Missing required fields: parking_id, floor, floor_alias.' })
            };
        }

        try {
            const dbClient = await getClient();

            // Check parking exists and get client_id
            const parkingRes = await dbClient.query(
                'SELECT client_id FROM public.parking WHERE parking_id = $1',
                [parking_id]
            );
            if (!parkingRes.rows.length) {
                dbClient.release();
                return {
                    status: 404,
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ error: 'Parking not found.' })
                };
            }
            const client_id = parkingRes.rows[0].client_id;

            // Insert new level with nulls for blob_image, stage_info, layout_info
            const insertQuery = `
                INSERT INTO public.levels (
                    floor, parking_id, floor_alias, blob_image, stage_info, layout_info
                ) VALUES ($1, $2, $3, NULL, NULL, NULL)
                RETURNING *;
            `;
            const insertValues = [
                floor,
                parking_id,
                floor_alias
            ];
            const levelRes = await dbClient.query(insertQuery, insertValues);

            // Increment no_levels in parkings
            await dbClient.query(
                'UPDATE public.parking SET no_levels = no_levels + 1 WHERE parking_id = $1',
                [parking_id]
            );

            // Increment no_floors in clients
            await dbClient.query(
                'UPDATE public.clients SET no_floors = no_floors + 1 WHERE client_id = $1',
                [client_id]
            );

            dbClient.release();

            return {
                status: 201,
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(levelRes.rows[0])
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