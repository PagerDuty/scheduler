#!/usr/bin/env bash

echo "Setting up Bintray credentials..."
mkdir $HOME/.bintray/
BINTRAY_CRED_FILE=$HOME/.bintray/.credentials
cat <<EOF >$BINTRAY_CRED_FILE
realm = Bintray API Realm
host = api.bintray.com
user = $BINTRAY_USER
password = $BINTRAY_API_KEY
EOF

echo "Configuring Git..."
git config --global user.email "builds@circleci.com"
git config --global user.name "Circle CI"

echo "Setting up Git credentials..."
ssh-add -D && echo "${GITHUB_SSH_KEY}" | base64 --decode | ssh-add -

echo "Parsing release version..."
RELEASE_VER=$(cat version.sbt | grep -o '".*"' | tr -d '"')
GIT_TAG=v$RELEASE_VER

echo "Conditionally publishing release and cutting git tag..."
if ! git ls-remote --exit-code origin refs/tags/$GIT_TAG; then
  SBT_CREDENTIALS=~/.sbt/credentials sbt -Dsbt.override.build.repos=true +publish &&
  git tag -a $GIT_TAG -m "Release version $RELEASE_VER" &&
  git push origin $GIT_TAG
fi
