docker compose up sonarqube db -d

curl -u "admin:admin" -X POST \
  "http://localhost:9000/api/user_tokens/generate" \
  -d "name=my-ci-token" \
  -d "type=USER_TOKEN"

docker compose up -d --build
sudo rm -r work_dir/

docker compose down sonarqube db -v