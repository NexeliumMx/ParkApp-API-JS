/**
 * FileName: src/functions/dbClient.js
 * Author(s): Arturo Vargas
 * Brief: Provides a PostgreSQL connection pool for both local and Azure environments.
 * Date: 2024-11-21
 *
 * Description:
 * This module exports a function to obtain a PostgreSQL client from a managed connection pool,
 * supporting both local development and Azure deployments. For local use, it authenticates with
 * username and password. In Azure, it leverages Managed Identity and token-based authentication
 * via DefaultAzureCredential.
 *
 * Copyright (c) 2024 BY: Nexelium Technological Solutions S.A. de C.V.
 * All rights reserved.
 *
 * ---------------------------------------------------------------------------
 * Code Description:
 * 1. Environment Detection: Determines whether the code is running locally or in Azure based on the ENVIRONMENT variable.
 *
 * 2. Pool Initialization: Initializes a singleton connection pool using the `pg` library. For local, uses standard credentials.
 *    For Azure, retrieves an access token using DefaultAzureCredential and uses it as the password.
 *
 * 3. Pool Reuse: Ensures the pool is only created once and reused for subsequent requests, optimizing resource usage.
 *
 * 4. Client Retrieval: Exports an async `getClient` function that returns a pooled client for executing queries.
 *
 * 5. Configuration: Requires environment variables (`PGHOST`, `PGDATABASE`, `PGPORT`, `PGUSER`, `PGPASSWORD`) to be set.
 *    For Azure, Managed Identity must be configured and the application must have access to the PostgreSQL server.
 * ---------------------------------------------------------------------------
 */

const { Pool } = require('pg');
const { DefaultAzureCredential } = require('@azure/identity');

let pool;

/**
 * Initializes and returns a singleton PostgreSQL connection pool.
 * Determines the environment (local or Azure) and configures the pool accordingly.
 * For local, uses user/password authentication; for Azure, uses Managed Identity token.
 * @since 1.0.0
 * @return {Promise<Pool>} The initialized PostgreSQL connection pool.
 * @throws {Error} If a valid Azure Managed Identity token cannot be retrieved.
 */
async function initPool() {
    if (pool) {
        console.log("Reusing existing connection pool.");
        return pool;  // Reuse the existing pool if already created
    }

    if (process.env.ENVIRONMENT === "local") {
        console.log("Running locally. Using traditional user/password authentication.");
        pool = new Pool({
            host: process.env.PGHOST,
            database: process.env.PGDATABASE,
            port: process.env.PGPORT || 5432,
            ssl: { rejectUnauthorized: false },
            user: process.env.PGUSER,
            password: process.env.PGPASSWORD
        });
    } else {
        console.log("Running in Azure. Using token-based authentication with managed identity.");
        const credential = new DefaultAzureCredential();
        const tokenResponse = await credential.getToken("https://ossrdbms-aad.database.windows.net");

        if (!tokenResponse || typeof tokenResponse.token !== 'string') {
            console.error("Failed to retrieve a valid token.");
            throw new Error("Invalid token retrieved from Azure Managed Identity.");
        }

        pool = new Pool({
            host: process.env.PGHOST,
            database: process.env.PGDATABASE,
            port: process.env.PGPORT || 5432,
            ssl: { rejectUnauthorized: false },
            user: process.env.PGUSER,
            password: tokenResponse.token
        });
    }

    console.log("Connection pool initialized.");
    return pool;
}

/**
 * Fetches a PostgreSQL client from the connection pool for executing queries.
 * Ensures the pool is initialized before returning a client.
 * @since 1.0.0
 * @return {Promise<import('pg').PoolClient>} A pooled PostgreSQL client.
 */
async function getClient() {
    const pool = await initPool();
    console.log("Fetching a client from the pool.");
    return pool.connect(); // Returns a pooled client
}

module.exports = { getClient };