/**
 * Author(s): Andres Gomez
 * Brief: Script to populate sensor data using the postStatus Azure Function endpoint
 * Date: 2025-07-08
 *
 * Copyright (c) 2025 BY: Nexelium Technological Solutions S.A. de C.V.
 * All rights reserved.
 */

const { getClient } = require('../functions/dbClient');

class APIDataGenerator {
    constructor(baseUrl = 'http://localhost:7071/api') {
        this.baseUrl = baseUrl;
        this.occupancyPatterns = {
            weekday: {
                peak_hours: [8, 9, 10, 12, 13, 14, 17, 18, 19],
                low_hours: [0, 1, 2, 3, 4, 5, 6, 22, 23],
                normal_hours: [7, 11, 15, 16, 20, 21]
            },
            weekend: {
                peak_hours: [11, 12, 13, 14, 15, 16, 17, 18, 19, 20],
                low_hours: [0, 1, 2, 3, 4, 5, 6, 7, 8, 22, 23],
                normal_hours: [9, 10, 21]
            }
        };
    }

    async postStatus(sensorId, timestamp, state, previousStateTime) {
        const payload = {
            sensor_id: sensorId,
            timestamp: timestamp,
            state: state,
            previous_state_time: previousStateTime
        };

        try {
            const response = await fetch(`${this.baseUrl}/postStatus`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                let errorMessage = `HTTP ${response.status}`;
                try {
                    const errorData = await response.json();
                    errorMessage += `: ${errorData.error || 'Unknown error'}`;
                } catch (jsonError) {
                    errorMessage += `: ${response.statusText}`;
                }
                throw new Error(errorMessage);
            }

            const responseData = await response.json();
            return responseData;
        } catch (error) {
            if (error.name === 'TypeError' && error.message.includes('fetch')) {
                throw new Error(`Network error: Cannot reach Azure Functions at ${this.baseUrl}`);
            }
            throw error;
        }
    }

    isWeekend(date) {
        const day = date.getDay();
        return day === 0 || day === 6;
    }

    getOccupancyProbability(hour, isWeekend) {
        const pattern = isWeekend ? this.occupancyPatterns.weekend : this.occupancyPatterns.weekday;
        
        if (pattern.peak_hours.includes(hour)) {
            return 0.75;
        } else if (pattern.low_hours.includes(hour)) {
            return 0.25;
        } else {
            return 0.50;
        }
    }

    generateRandomInterval(minMinutes = 15, maxMinutes = 240) {
        const minutes = Math.floor(Math.random() * (maxMinutes - minMinutes + 1)) + minMinutes;
        const hours = Math.floor(minutes / 60);
        const remainingMinutes = minutes % 60;
        return `${hours.toString().padStart(2, '0')}:${remainingMinutes.toString().padStart(2, '0')}:00`;
    }

    async getSensors(userId) {
        try {
            console.log(`Fetching sensors for user: ${userId}`);
            
            const response = await fetch(`${this.baseUrl}/getSensorsByUser?user_id=${userId}`, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json'
                }
            });

            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(`HTTP ${response.status}: ${errorData.error || 'Failed to fetch sensors'}`);
            }

            const data = await response.json();
            
            if (!data.success) {
                throw new Error(`API error: ${data.error}`);
            }
            
            console.log(`Successfully fetched ${data.count} sensors`);
            
            // Return sensors in the format expected by the rest of the script
            return data.sensors.map(sensor => ({
                sensor_id: sensor.sensor_id,
                sensor_alias: sensor.sensor_alias,
                parking_id: sensor.parking_id,
                floor: sensor.floor,
                parking_alias: sensor.parking_alias,
                complex: sensor.complex
            }));
            
        } catch (error) {
            console.error('Error fetching sensors:', error.message);
            throw error;
        }
    }

    async populateDataForSensor(sensor, startDate, endDate, batchSize = 50) {
        console.log(`Generating data for sensor: ${sensor.sensor_alias} (${sensor.parking_alias})`);
        
        const currentDate = new Date(startDate);
        let currentState = Math.random() > 0.5;
        let successCount = 0;
        let errorCount = 0;
        const requests = [];

        while (currentDate <= endDate) {
            const hour = currentDate.getHours();
            const isWeekend = this.isWeekend(currentDate);
            const occupancyProb = this.getOccupancyProbability(hour, isWeekend);
            
            // Generate measurements every 15 minutes
            for (let minute = 0; minute < 60; minute += 15) {
                const timestamp = new Date(currentDate);
                timestamp.setMinutes(minute);
                timestamp.setSeconds(0);
                timestamp.setMilliseconds(0);
                
                // Determine if state should change
                const shouldBeOccupied = Math.random() < occupancyProb;
                if (shouldBeOccupied !== currentState && Math.random() < 0.3) {
                    currentState = shouldBeOccupied;
                }
                
                const previousStateTime = this.generateRandomInterval(15, 180);
                
                // Add to batch
                requests.push({
                    sensorId: sensor.sensor_id,
                    timestamp: timestamp.toISOString(),
                    state: currentState,
                    previousStateTime: previousStateTime
                });

                // Process batch when it reaches the batch size
                if (requests.length >= batchSize) {
                    const results = await this.processBatch(requests);
                    successCount += results.success;
                    errorCount += results.errors;
                    requests.length = 0; // Clear the batch
                }
            }
            
            currentDate.setDate(currentDate.getDate() + 1);
            currentDate.setHours(0, 0, 0, 0);
        }

        // Process remaining requests
        if (requests.length > 0) {
            const results = await this.processBatch(requests);
            successCount += results.success;
            errorCount += results.errors;
        }

        console.log(`Sensor ${sensor.sensor_alias}: ${successCount} successful, ${errorCount} errors`);
        return { success: successCount, errors: errorCount };
    }

    async processBatch(requests) {
        const promises = requests.map(async (req) => {
            try {
                await this.postStatus(req.sensorId, req.timestamp, req.state, req.previousStateTime);
                return { success: true };
            } catch (error) {
                console.error(`Error posting status for sensor ${req.sensorId}:`, error.message);
                return { success: false, error: error.message };
            }
        });

        const results = await Promise.all(promises);
        const successCount = results.filter(r => r.success).length;
        const errorCount = results.filter(r => !r.success).length;

        return { success: successCount, errors: errorCount };
    }

    async checkAzureFunctionsHealth() {
        try {
            console.log('Checking Azure Functions health...');
            
            const response = await fetch(`${this.baseUrl}/testDBconnection`, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json'
                }
            });

            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    console.log('Azure Functions and database are both healthy');
                    return true;
                }
            }
            
            console.log(`Health check failed: HTTP ${response.status}`);
            return false;
            
        } catch (error) {
            console.error('Health check failed:', error.message);
            return false;
        }
    }

    async populateTestData(userId, daysBack = 365) {
        try {
            console.log('Starting test data population via API...');
            
            // Validate userId parameter
            if (!userId) {
                throw new Error('userId is required for populateTestData');
            }
            
            // Check if Azure Functions is running
            const isHealthy = await this.checkAzureFunctionsHealth();
            if (!isHealthy) {
                console.error('Cannot connect to Azure Functions. Make sure it is running on localhost:7071');
                return;
            }

            // Get all sensors for the specified user
            const sensors = await this.getSensors(userId);
            console.log(`Found ${sensors.length} sensors to populate`);
            
            if (sensors.length === 0) {
                console.log('No sensors found for this user. Please check user permissions.');
                return;
            }
            
            // Define date range
            const endDate = new Date();
            const startDate = new Date();
            startDate.setDate(endDate.getDate() - daysBack);
            
            console.log(`Generating data from ${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`);
            
            let totalSuccess = 0;
            let totalErrors = 0;
            
            // Process sensors in smaller groups to avoid overwhelming the API
            const concurrentSensors = 3; // Process 3 sensors at a time
            
            for (let i = 0; i < sensors.length; i += concurrentSensors) {
                const sensorBatch = sensors.slice(i, i + concurrentSensors);
                
                console.log(`\nProcessing sensor batch ${Math.floor(i / concurrentSensors) + 1}/${Math.ceil(sensors.length / concurrentSensors)}`);
                
                const promises = sensorBatch.map(sensor => 
                    this.populateDataForSensor(sensor, startDate, endDate)
                );
                
                const results = await Promise.all(promises);
                
                results.forEach(result => {
                    totalSuccess += result.success;
                    totalErrors += result.errors;
                });
                
                // Add a small delay between batches to avoid overwhelming the API
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            console.log('\nTest data population completed!');
            console.log(`Total successful requests: ${totalSuccess}`);
            console.log(`Total errors: ${totalErrors}`);
            
            if (totalSuccess + totalErrors > 0) {
                console.log(`Success rate: ${((totalSuccess / (totalSuccess + totalErrors)) * 100).toFixed(2)}%`);
            }
            
        } catch (error) {
            console.error('Error during test data population:', error);
            throw error;
        }
    }
}

// Script execution
async function main() {
    const generator = new APIDataGenerator();
    
    try {
        // Debug: Show all command line arguments
        console.log('Command line arguments:', process.argv);
        
        // Get user_id from command line arguments
        const userId = process.argv[2];
        const daysBackArg = process.argv[3];
        
        // Validate user_id
        if (!userId) {
            console.error('Usage: node src/scripts/populateStatus.js <user_id> [days_back]');
            console.error('Example: node src/scripts/populateStatus.js fb713fca-4cbc-44b1-8a25-c6685c3efd31 30');
            process.exit(1);
        }
        
        // Parse and validate days_back
        let daysBack = 365; // Default value
        if (daysBackArg) {
            daysBack = parseInt(daysBackArg);
            if (isNaN(daysBack) || daysBack <= 0) {
                console.error(`Invalid number of days: "${daysBackArg}". Please provide a positive integer.`);
                console.error('Example: node src/scripts/populateStatus.js fb713fca-4cbc-44b1-8a25-c6685c3efd31 30');
                process.exit(1);
            }
        }
        
        console.log(`Using parameters: user_id=${userId}, days_back=${daysBack}`);
        
        await generator.populateTestData(userId, daysBack);
        process.exit(0);
    } catch (error) {
        console.error('Script failed:', error);
        process.exit(1);
    }
}

// Run the script
if (require.main === module) {
    main();
}

module.exports = { APIDataGenerator };