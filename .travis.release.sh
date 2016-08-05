#!/bin/bash

echo "Setting up Git credentials..."
GIT_CREDS_FILE=~/.git-credentials
echo "https://$GIT_USER:$GIT_API_KEY@github.com" > $GIT_CREDS_FILE

echo "Configuring Git..."
git config --global user.email "builds@travis-ci.com"
git config --global user.name "Travis CI"
git config credential.helper store

echo "Parsing release version..."
RELEASE_VER=$(cat version.sbt | grep -o '".*"' | tr -d '"')
GIT_TAG=v$RELEASE_VER

echo "Conditionally publishing release and cutting git tag..."
test "${TRAVIS_PULL_REQUEST}" = 'false' &&
test "${TRAVIS_JDK_VERSION}" = 'oraclejdk7' &&
sbt -Dsbt.global.base=/home/travis/.sbt ++${TRAVIS_SCALA_VERSION} publish &&
git tag -a $GIT_TAG -m "Release version $RELEASE_VER" &&
git push origin $GIT_TAG
