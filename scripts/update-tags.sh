#bin/sh

git tag -l | xargs git tag -d
git fetch --tags