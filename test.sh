#!/usr/bin/env bash

# Define the commands to run
server="go run server.go"
client1="go run client.go 1"
client2="go run client.go 2"
client3="go run client.go 3"
client4="go run client.go 4"
client5="go run client.go 5"
client6="go run client.go 6"
client7="go run client.go 7"
client8="go run client.go 8"


# Map commands to an array for easy iteration
cmds=("$server" "$client1" "$client2" "$client3" "$client4" "$client5" "$client6" "$client7" "$client8")

# Launch Konsole silently
konsole --fullscreen --layout ./test_layout.json > /dev/null 2>&1 & KPID=$!
sleep 0.5

service="$(qdbus | grep -B1 konsole | grep -v -- -- | sort -t"." -k2 -n | tail -n 1)"

# Loop through the array and run commands
for i in "${!cmds[@]}"; do
    # Sessions are 1-indexed, so we use i+1
    session_id=$((i + 1))
    qdbus $service /Sessions/$session_id org.kde.konsole.Session.runCommand "${cmds[$i]}" > /dev/null
    sleep 0.2
done
