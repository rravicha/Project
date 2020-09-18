#1) Clear the screen
clear

#2) Run the cur request
curl -X POST -H 'Content-Type: application/json' \
-H 'clientid:sparkey' \
-H 'secret:qpalzmwiskxn' \
-d '{"ply":"bot","dim":"3,2","pp":"1,1","tp":"2,1","dist":"4"}' \
http://127.0.0.1:5000
