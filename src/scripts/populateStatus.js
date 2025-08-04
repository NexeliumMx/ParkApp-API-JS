/**
 * Author(s): Andres Gomez
 * Brief: Fixed script to properly distribute sensor data across all sensors
 * Date: 2025-07-10
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

    /**
     * Enhanced sensor fetching with proper sensor distribution
     * Following Azure Functions best practices for data processing
     */
    async getSensors(userId) {
        try {
            console.log(`Fetching existing sensors for user: ${userId}`);
            
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
            
            console.log(`✅ Successfully fetched ${data.count} existing sensors`);
            
            if (data.count === 0) {
                console.log('❌ No sensors found for this user.');
                console.log('💡 Make sure the user has permissions and sensors exist in the database.');
                return [];
            }

            // Enhanced breakdown showing exact distribution
            const parkingBreakdown = {};
            const sensorsByParking = {};
            
            data.sensors.forEach((sensor, index) => {
                const key = `${sensor.parking_alias || 'Unknown Parking'} (Floor ${sensor.floor})`;
                const parkingKey = sensor.parking_id;
                
                if (!parkingBreakdown[key]) {
                    parkingBreakdown[key] = {
                        count: 0,
                        parking_id: sensor.parking_id,
                        floor: sensor.floor,
                        sensors: []
                    };
                }
                
                if (!sensorsByParking[parkingKey]) {
                    sensorsByParking[parkingKey] = {
                        parking_alias: sensor.parking_alias,
                        sensors: []
                    };
                }
                
                parkingBreakdown[key].count++;
                parkingBreakdown[key].sensors.push(sensor.sensor_alias);
                sensorsByParking[parkingKey].sensors.push(sensor.sensor_alias);
            });

            console.log('📍 Detailed sensor distribution:');
            Object.entries(parkingBreakdown).forEach(([location, info]) => {
                console.log(`   • ${location}: ${info.count} sensors`);
                console.log(`     Sensors: ${info.sensors.slice(0, 3).join(', ')}${info.sensors.length > 3 ? '...' : ''}`);
            });

            console.log('\n📊 Parking summary:');
            Object.entries(sensorsByParking).forEach(([parkingId, info]) => {
                console.log(`   • ${info.parking_alias}: ${info.sensors.length} sensors total`);
            });
            
            // Return sensors with proper indexing for debugging
            const processedSensors = data.sensors.map((sensor, index) => ({
                sensor_id: sensor.sensor_id,
                sensor_alias: sensor.sensor_alias,
                parking_id: sensor.parking_id,
                floor: sensor.floor,
                parking_alias: sensor.parking_alias || 'Unknown Parking',
                complex: sensor.complex || 'Unknown Complex',
                index: index + 1 // For debugging
            }));
            
            console.log(`📋 Will process ${processedSensors.length} sensors sequentially`);
            return processedSensors;
            
        } catch (error) {
            console.error('Error fetching existing sensors:', error.message);
            throw error;
        }
    }

    /**
     * Generate realistic but limited state changes per sensor
     * Following Azure Functions best practices for data volume control
     */
    generateStateDuration(currentState, hour, isWeekend) {
        const pattern = isWeekend ? this.occupancyPatterns.weekend : this.occupancyPatterns.weekday;
        
        if (currentState === true) { // Currently occupied
            if (pattern.peak_hours.includes(hour)) {
                // Peak hours: 45-180 minutes (longer stays)
                return Math.floor(Math.random() * 136) + 45;
            } else if (pattern.low_hours.includes(hour)) {
                // Low hours: 30-90 minutes (moderate stays)
                return Math.floor(Math.random() * 61) + 30;
            } else {
                // Normal hours: 30-120 minutes
                return Math.floor(Math.random() * 91) + 30;
            }
        } else { // Currently available
            if (pattern.peak_hours.includes(hour)) {
                // Peak hours: 10-45 minutes (fills up quickly)
                return Math.floor(Math.random() * 36) + 10;
            } else if (pattern.low_hours.includes(hour)) {
                // Low hours: 60-300 minutes (stays empty longer)
                return Math.floor(Math.random() * 241) + 60;
            } else {
                // Normal hours: 20-120 minutes
                return Math.floor(Math.random() * 101) + 20;
            }
        }
    }

    /**
     * Fixed duration formatting for PostgreSQL
     * Following Azure Functions best practices for database operations
     */
    formatDurationToPostgreSQLInterval(durationHours) {
        const totalMinutes = Math.floor(durationHours * 60);
        const hours = Math.floor(totalMinutes / 60);
        const minutes = totalMinutes % 60;
        
        if (hours > 0 && minutes > 0) {
            return `${hours} hours ${minutes} minutes`;
        } else if (hours > 0) {
            return `${hours} hours`;
        } else if (minutes > 0) {
            return `${minutes} minutes`;
        } else {
            return '1 minute'; // Minimum duration
        }
    }

    /**
     * FIXED: Generate controlled amount of data per sensor
     * Following Azure Functions best practices for data volume management
     */
    async populateDataForSensor(sensor, startDate, endDate, maxStateChanges = 50) {
        console.log(`\n📡 Processing sensor ${sensor.index}/${sensor.total || '?'}: ${sensor.sensor_alias}`);
        console.log(`   🏢 Location: ${sensor.parking_alias} - Floor ${sensor.floor}`);
        console.log(`   🎯 Target: ~${maxStateChanges} state changes maximum`);
        
        // Add sensor-specific randomization to avoid timestamp conflicts
        const sensorOffset = (sensor.sensor_id.split('').reduce((acc, char) => acc + char.charCodeAt(0), 0) % 60) * 60 * 1000; // 0-59 minutes
        let currentTimestamp = new Date(startDate.getTime() + sensorOffset);
        
        // Random initial state per sensor
        const seedValue = sensor.sensor_id.length + sensor.floor;
        let currentState = (seedValue % 2) === 0; // Deterministic but varied initial state
        
        let successCount = 0;
        let errorCount = 0;
        const requests = [];
        let stateChangeCount = 0;

        // Log initial state
        requests.push({
            sensorId: sensor.sensor_id,
            timestamp: currentTimestamp.toISOString(),
            state: currentState,
            previousStateTime: null
        });

        // Generate LIMITED number of state changes per sensor
        while (currentTimestamp < endDate && stateChangeCount < maxStateChanges) {
            const hour = currentTimestamp.getHours();
            const isWeekend = this.isWeekend(currentTimestamp);
            
            // Generate realistic duration for current state
            const durationMinutes = this.generateStateDuration(currentState, hour, isWeekend);
            
            // Calculate next state change time
            const nextTimestamp = new Date(currentTimestamp.getTime() + (durationMinutes * 60 * 1000));
            
            // Don't go beyond end date
            if (nextTimestamp > endDate) {
                break;
            }
            
            // Calculate duration for previous state
            const timeDifferenceMs = nextTimestamp.getTime() - currentTimestamp.getTime();
            const previousStateDurationHours = timeDifferenceMs / (1000 * 60 * 60);
            const previousStateTime = this.formatDurationToPostgreSQLInterval(previousStateDurationHours);
            
            // Change state
            currentState = !currentState;
            currentTimestamp = nextTimestamp;
            stateChangeCount++;
            
            // Add to requests
            requests.push({
                sensorId: sensor.sensor_id,
                timestamp: currentTimestamp.toISOString(),
                state: currentState,
                previousStateTime: previousStateTime
            });

            // Process in smaller batches to avoid overwhelming Azure Functions
            if (requests.length >= 20) { // Smaller batch size
                const results = await this.processBatch(requests, sensor.sensor_alias);
                successCount += results.success;
                errorCount += results.errors;
                requests.length = 0;
                
                // Add delay between batches for this sensor
                await new Promise(resolve => setTimeout(resolve, 100));
            }
        }

        // Process remaining requests
        if (requests.length > 0) {
            const results = await this.processBatch(requests, sensor.sensor_alias);
            successCount += results.success;
            errorCount += results.errors;
        }

        console.log(`   ✅ ${sensor.sensor_alias}: ${successCount} state changes logged, ${errorCount} errors`);
        console.log(`   📊 Generated ${stateChangeCount} state changes over ${Math.ceil((endDate - startDate) / (1000 * 60 * 60 * 24))} days`);
        
        return { 
            success: successCount, 
            errors: errorCount, 
            sensor_alias: sensor.sensor_alias,
            state_changes: stateChangeCount
        };
    }

    /**
     * Enhanced batch processing with sensor context
     * Following Azure Functions best practices for error handling
     */
    async processBatch(requests, sensorAlias) {
        console.log(`     📤 Processing batch of ${requests.length} requests for ${sensorAlias}`);
        
        const promises = requests.map(async (req, index) => {
            try {
                await this.postStatus(req.sensorId, req.timestamp, req.state, req.previousStateTime);
                return { success: true, type: 'inserted' };
            } catch (error) {
                // Handle duplicates gracefully
                if (error.message.includes('duplicate') || 
                    error.message.includes('already exists') ||
                    error.message.includes('UNIQUE constraint') ||
                    error.message.includes('409')) {
                    return { success: true, type: 'skipped', reason: 'duplicate' };
                }
                
                // Log first few errors for debugging
                if (index < 3) {
                    console.error(`     ❌ Error for ${sensorAlias}:`, {
                        timestamp: req.timestamp,
                        error: error.message.substring(0, 100)
                    });
                }
                
                return { success: false, error: error.message };
            }
        });

        const results = await Promise.all(promises);
        const successCount = results.filter(r => r.success).length;
        const errorCount = results.filter(r => !r.success).length;
        const skippedCount = results.filter(r => r.success && r.type === 'skipped').length;
        const insertedCount = results.filter(r => r.success && r.type === 'inserted').length;

        console.log(`     📊 Batch result: ${insertedCount} inserted, ${skippedCount} skipped, ${errorCount} errors`);

        return { success: successCount, errors: errorCount, skipped: skippedCount };
    }

    /**
     * FIXED: Process sensors ONE AT A TIME to avoid conflicts
     * Following Azure Functions best practices for sequential processing
     */
    async populateTestData(userId, daysBack = 7) { // Default to 7 days for testing
        try {
            console.log('🚀 Starting FIXED sensor data population...');
            console.log('📋 This version processes sensors ONE AT A TIME to avoid conflicts');
            console.log('📋 Each sensor gets a LIMITED number of state changes');
            
            if (!userId) {
                throw new Error('userId is required for populateTestData');
            }
            
            // Check Azure Functions health
            const isHealthy = await this.checkAzureFunctionsHealth();
            if (!isHealthy) {
                console.error('❌ Cannot connect to Azure Functions. Make sure it is running on localhost:7071');
                return;
            }

            // Get all existing sensors
            const sensors = await this.getSensors(userId);
            
            if (sensors.length === 0) {
                console.log('❌ No existing sensors found for this user.');
                console.log('💡 Make sure the user has permissions and sensors exist in the database.');
                return;
            }
            
            // Add total count to each sensor for progress tracking
            sensors.forEach((sensor, index) => {
                sensor.total = sensors.length;
                sensor.index = index + 1;
            });
            
            // Define date range (shorter for testing)
            const endDate = new Date();
            const startDate = new Date();
            startDate.setDate(endDate.getDate() - daysBack);
            
            console.log(`\n📅 Generating data from ${startDate.toISOString().split('T')[0]} to ${endDate.toISOString().split('T')[0]}`);
            console.log(`🎯 Processing ${sensors.length} sensors SEQUENTIALLY (one at a time)`);
            console.log(`⏱️  Estimated time: ~${Math.ceil(sensors.length * 30 / 60)} minutes`);
            
            let totalSuccess = 0;
            let totalErrors = 0;
            const sensorResults = [];
            
            // CRITICAL FIX: Process sensors ONE AT A TIME
            for (let i = 0; i < sensors.length; i++) {
                const sensor = sensors[i];
                console.log(`\n🔄 Processing sensor ${i + 1}/${sensors.length}`);
                
                try {
                    // Generate limited data per sensor (30-100 state changes max)
                    const maxStateChanges = Math.floor(Math.random() * 71) + 30; // 30-100 state changes
                    const result = await this.populateDataForSensor(sensor, startDate, endDate, maxStateChanges);
                    
                    totalSuccess += result.success;
                    totalErrors += result.errors;
                    sensorResults.push(result);
                    
                } catch (error) {
                    console.error(`❌ Failed to process sensor ${sensor.sensor_alias}:`, error.message);
                    totalErrors++;
                    sensorResults.push({
                        success: 0,
                        errors: 1,
                        sensor_alias: sensor.sensor_alias,
                        state_changes: 0,
                        error: error.message
                    });
                }
                
                // IMPORTANT: Add delay between sensors to avoid overwhelming Azure Functions
                if (i < sensors.length - 1) {
                    console.log(`   ⏳ Waiting 2 seconds before next sensor...`);
                    await new Promise(resolve => setTimeout(resolve, 2000));
                }
            }
            
            console.log('\n🎉 FIXED sensor data population completed!');
            console.log(`📊 Final Statistics:`);
            console.log(`   • Total sensors processed: ${sensors.length}`);
            console.log(`   • Total successful requests: ${totalSuccess}`);
            console.log(`   • Total errors: ${totalErrors}`);
            console.log(`   • Average requests per sensor: ${(totalSuccess / sensors.length).toFixed(1)}`);
            
            // Show breakdown per sensor
            console.log('\n📋 Per-sensor breakdown:');
            sensorResults.forEach(result => {
                if (result.success > 0) {
                    console.log(`   ✅ ${result.sensor_alias}: ${result.success} state changes`);
                } else {
                    console.log(`   ❌ ${result.sensor_alias}: Failed - ${result.error || 'Unknown error'}`);
                }
            });
            
            // Get distribution for testing
            const uniqueParkings = [...new Set(sensors.map(s => s.parking_id))];
            const uniqueFloors = [...new Set(sensors.map(s => s.floor))].sort((a,b) => a-b);
            
            console.log('\n💡 Now you can test with properly distributed data:');
            console.log(`   🏢 Multiple parkings: parking_id=${uniqueParkings.slice(0,2).join(',')}`);
            console.log(`   🏗️  Multiple floors: floor=${uniqueFloors.slice(0,3).join(',')}`);
            
            console.log('\n📋 Sample API calls:');
            console.log(`curl "http://localhost:7071/api/getAnalysis?user_id=${userId}&locationSetting=parking&timeSetting=day&year=2025&month=7&day=10&parking_id=${uniqueParkings.slice(0,2).join(',')}"`);
            
        } catch (error) {
            console.error('❌ Error during data population:', error);
            throw error;
        }
    }

    async checkAzureFunctionsHealth() {
        try {
            console.log('🔍 Checking Azure Functions health...');
            
            const response = await fetch(`${this.baseUrl}/testDBconnection`, {
                method: 'GET',
                headers: {
                    'Accept': 'application/json'
                }
            });

            if (response.ok) {
                const data = await response.json();
                if (data.success) {
                    console.log('✅ Azure Functions and database are both healthy');
                    return true;
                }
            }
            
            console.log(`❌ Health check failed: HTTP ${response.status}`);
            return false;
            
        } catch (error) {
            console.error('❌ Health check failed:', error.message);
            return false;
        }
    }
}

// Fixed script execution following Azure Functions best practices
async function main() {
    const generator = new APIDataGenerator();
    
    try {
        const userId = process.argv[2];
        const daysBackArg = process.argv[3];
        
        if (!userId) {
            console.error('❌ Usage: node src/scripts/populateStatus.js <user_id> [days_back]');
            console.error('📋 FIXED VERSION: Processes sensors sequentially with limited data per sensor');
            console.error('📋 Default: 7 days, 30-100 state changes per sensor');
            console.error('');
            console.error('Examples:');
            console.error('  node src/scripts/populateStatus.js fb713fca-4cbc-44b1-8a25-c6685c3efd31 7');
            console.error('  node src/scripts/populateStatus.js fb713fca-4cbc-44b1-8a25-c6685c3efd31 3');
            process.exit(1);
        }
        
        let daysBack = 7; // Default to 7 days for controlled testing
        if (daysBackArg) {
            daysBack = parseInt(daysBackArg);
            if (isNaN(daysBack) || daysBack <= 0 || daysBack > 30) {
                console.error(`❌ Invalid days: "${daysBackArg}". Use 1-30 days for testing.`);
                process.exit(1);
            }
        }
        
        console.log(`🔧 FIXED Configuration: user_id=${userId}, days_back=${daysBack}`);
        console.log('📋 Mode: Sequential processing with controlled data volume');
        
        await generator.populateTestData(userId, daysBack);
        process.exit(0);
    } catch (error) {
        console.error('❌ Script failed:', error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = { APIDataGenerator };