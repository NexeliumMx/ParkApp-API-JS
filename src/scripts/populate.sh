#!/bin/bash

API="http://localhost:7071/api"

# 1. Add a client
client_id=$(curl -s -X POST "$API/addClient" \
  -H "Content-Type: application/json" \
  -d '{"client_alias":"Test Client"}' | jq -r '.client_id')

echo "Client: $client_id"

# 2. Add 3 users
user_ids=()
for i in {1..3}; do
  user_id=$(curl -s -X POST "$API/addUser" \
    -H "Content-Type: application/json" \
    -d "{\"username\":\"user$i\",\"password\":\"pass$i\",\"client_id\":\"$client_id\"}" | jq -r '.user_id')
  user_ids+=("$user_id")
  echo "User $i: $user_id"
done

# 3. Add 3 parkings
parking_ids=()
for i in {1..3}; do
  parking_id=$(curl -s -X POST "$API/addParking" \
    -H "Content-Type: application/json" \
    -d "{\"parking_alias\":\"Parking $i\",\"client_id\":\"$client_id\",\"complex\":\"Complex $i\",\"installation_date\":\"2025-07-01\"}" | jq -r '.parking_id')
  parking_ids+=("$parking_id")
  echo "Parking $i: $parking_id"
done

# 4. For each parking, add 5 levels and 50 sensors per level
for parking_id in "${parking_ids[@]}"; do
  for level in {1..5}; do
    curl -s -X POST "$API/addLevel" \
      -H "Content-Type: application/json" \
      -d "{\"parking_id\":\"$parking_id\",\"floor\":$level,\"floor_alias\":\"Level $level\"}" > /dev/null
    for sensor in {1..50}; do
      curl -s -X POST "$API/addSensor" \
        -H "Content-Type: application/json" \
        -d "{\"parking_id\":\"$parking_id\",\"sensor_alias\":\"Sensor L${level}S${sensor}\",\"floor\":$level,\"column\":$sensor,\"row\":1,\"type\":\"normal\"}" > /dev/null
    done
    echo "Parking $parking_id: Level $level with 50 sensors added."
  done
done

# 5. Grant each user access to all 3 parkings
for user_id in "${user_ids[@]}"; do
  for parking_id in "${parking_ids[@]}"; do
    curl -s -X POST "$API/grantPermission" \
      -H "Content-Type: application/json" \
      -d "{\"user_id\":\"$user_id\",\"parking_id\":\"$parking_id\"}" > /dev/null
    echo "Granted user $user_id access to parking $parking_id"
  done
done

echo "Test data population complete."