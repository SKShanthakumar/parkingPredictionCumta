#!/bin/sh

LOGFILE="/var/log/training.log"
echo "---- Run started at $(date) ----" >> "$LOGFILE"

stations=(
"Wimco Nagar Depot Metro"
"Wimco Nagar Metro"
"Thiruvotriyur Metro"
"Thiruvotriyur Theradi Metro"
"Kaladipet Metro"
"Tollgate Metro"
"New Washermenpet Metro"
"Tondiarpet Metro"
"Thiagaraya College Metro"
"Washermanpet"
"Mannadi"
"High Court"
"Government Estate"
"LIC"
"Thousand Lights"
"AG-DMS"
"Teynampet"
"Nandanam"
"Saidapet"
"Little Mount"
"Guindy"
"OTA - Nanganallur Road"
"Meenambakkam"
"Chennai International Airport"
"Puratchi Thalaivar Dr. M.G. Ramachandran Central"
"Egmore"
"Nehru Park"
"Kilpauk"
"Pachaiyappas College"
"Shenoy Nagar"
"Anna Nagar East"
"Anna Nagar Tower"
"Thirumangalam"
"Koyambedu"
"Arumbakkam"
"Vadapalani"
"Ashok Nagar"
"Ekkattuthangal"
"Arignar Anna Alandur "
"St. Thomas Mount"
)

for station in "${stations[@]}"; do
    for vehicle in 0 1; do
        echo "Running for station: '$station', vehicle: $vehicle at $(date)"
        /usr/local/bin/python /app/trainAndUpdatePred.py --station "$station" --vehicle "$vehicle" >> "$LOGFILE" 2>&1
    done
done

echo "---- Run completed at $(date) ----" >> "$LOGFILE"
