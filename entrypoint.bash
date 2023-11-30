#!/bin/bash

cd /reference || exit

# Create bento_user + home
source /create_service_user.bash

# Fix permissions on /reference
chown -R bento_user:bento_user /reference

# Drop into bento_user from root and execute the CMD specified for the image
exec gosu bento_user "$@"
