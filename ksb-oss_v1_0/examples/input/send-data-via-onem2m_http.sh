URI=$1
DATE_TIME=`date '+%Y-%m-%d %H:%M:%S'`


JSON_DATA="{
  \"dateTime\": \"$DATE_TIME\",
  \"deviceId\": 9002,
  \"temperature\": 80.0,
  \"humidity\": 0.0,
  \"message\": \"hello\"
}"

ONEM2M_PLAIN_DATA="{
  \"m2m:sgn\": {
    \"nev\": {
      \"rep\": {
        \"m2m:cin\": {
          \"rn\": \"$DATE_TIME\",
          \"ty\": 4,
          \"ri\": \"/incse/64u0ANoitlJ6BV0c3h6omt\",
          \"pi\": \"/incse/7y1aGCavXnL9eCtAeI6aVs\",
          \"ct\": \"20170330T132210\",
          \"lt\": \"20170330T132210\",
          \"et\": \"20170331T132210\",
          \"st\": 0,
          \"cr\": \"S_AE1\",
          \"cnf\": \"application/json:0\",
          \"cs\": 177,
          \"con\": \"$JSON_DATA\"
        }
      },
      \"om\": {
        \"op\": 1,
        \"org\": \"S_AE1\"
      },
      \"net\": 3
    },
    \"cr\": \"/Sae1\"
  }
}"

ONEM2M_BASE64_DATA="{
  \"m2m:sgn\": {
    \"nev\": {
      \"rep\": {
        \"m2m:cin\": {
          \"rn\": \"$DATE_TIME\",
          \"ty\": 4,
          \"ri\": \"/incse/64u0ANoitlJ6BV0c3h6omt\",
          \"pi\": \"/incse/7y1aGCavXnL9eCtAeI6aVs\",
          \"ct\": \"20170330T132210\",
          \"lt\": \"20170330T132210\",
          \"et\": \"20170331T132210\",
          \"st\": 0,
          \"cr\": \"S_AE1\",
          \"cnf\": \"application/json:1\",
          \"cs\": 177,
          \"con\": \"ew0KCSJkYXRlVGltZSI6ICIyMDE3LTAxLTAxIDExOjIyOjMzLjQ0NCIsDQoJImRldmljZUlkIjogOTAwMiwNCgkidGVtcGVyYXR1cmUiOiAtODAuMCwNCgkiaHVtaWRpdHkiOiAxMDAuMCwNCgkibWVzc2FnZSI6ICJ3b3JsZCINCn0=\"
        }
      },
      \"om\": {
        \"op\": 1,
        \"org\": \"S_AE1\"
      },
      \"net\": 3
    },
    \"cr\": \"/Sae1\"
  }
}"


echo "send data to: $URI"
curl -XPOST $URI -H 'Content-Type: application/json' --data "$ONEM2M_BASE64_DATA"
echo
