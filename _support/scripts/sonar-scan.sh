#!/usr/bin/env bash

########################
# requirements:        #
# 0. docker            #
# 1. wget              #
# 2. curl              #
# 3. jq                #
# 4. sonar-scanner     #
########################

set -e

projectKey="vmware-go-kcl-v2"
user_tokenName="local_token"
username="admin"
user_password="admin"
new_password="vmware"
url="http://localhost"
port="9000"

if [[ "$( docker container inspect -f '{{.State.Running}}' sonarqube )" == "true" ]];
then
  docker ps
else
  docker run --rm -d --name sonarqube -e SONAR_ES_BOOTSTRAP_CHECKS_DISABLE=true -p 9000:9000 sonarqube
fi

echo "waiting for sonarqube starts..."
wget -q -O - "$@" http://localhost:9000 | awk '/STARTING/{ print $0 }' | xargs

STATUS="$(wget -q -O - "$@" http://localhost:9000 | awk '/UP/{ print $0 }')"
while [ -z "$STATUS" ]
do
	sleep 2
	STATUS="$(wget -q -O - "$@" http://localhost:9000 | awk '/UP/{ print $0 }')"
	printf "."
done

printf '\n %s' "${STATUS}" | xargs
echo ""

# change the default password to avoid create a new one when login for the very first time
curl -u ${username}:${user_password} -X POST "${url}:${port}/api/users/change_password?login=${username}&previousPassword=${user_password}&password=${new_password}"

# search the specific user tokens for SonarQube
hasToken=$(curl --silent -u ${username}:${new_password} -X GET "${url}:${port}/api/user_tokens/search")
if [[ -n "${hasToken}"  ]]; then
  # Revoke the user token for SonarQube
  curl -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "name=${user_tokenName}" -u ${username}:${new_password} "${url}:${port}"/api/user_tokens/revoke
fi

# generate new token
token=$(curl --silent -X POST -H "Content-Type: application/x-www-form-urlencoded" -d "name=${user_tokenName}" -u ${username}:${new_password} "${url}:${port}"/api/user_tokens/generate | jq '.token' | xargs)

# scan and push the results to localhost docker container
sonar-scanner -Dsonar.projectKey="${projectKey}" \
              -Dsonar.projectName="${projectKey}" \
              -Dsonar.sources=. \
              -Dsonar.exclusions="internal/records/**, test/**" \
              -Dsonar.host.url="${url}:${port}" \
              -Dsonar.login="${token}"

