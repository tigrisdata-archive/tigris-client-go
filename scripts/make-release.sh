#!/bin/bash

set -e

if [ $# -lt 1 ]; then
	echo "Missing target release name, one of: alpha, beta, rc, release"
	exit 1
fi

RELEASE=$1

CUR_BRANCH=$(git branch --show-current)

echo "Making '$RELEASE' release out of '$CUR_BRANCH' branch"

git fetch origin "$RELEASE"
git checkout "$RELEASE"
git merge --ff "$CUR_BRANCH"
git push origin "$RELEASE"

echo "Successfully pushed '$CUR_BRANCH' to '$RELEASE' branch"
