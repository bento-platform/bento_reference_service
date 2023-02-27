#!/bin/bash

cd /reference || exit

# Create bento_user + home
source /create_service_user.bash

# Fix permissions on /reference and /env
chown -R bento_user:bento_user /reference
if [[ -d /env ]]; then
  chown -R bento_user:bento_user /env
fi

# Fix permissions on the data directory
if [[ -n "${DATA_PATH}" ]]; then
  chown -R bento_user:bento_user "${DATA_PATH}"
  chmod -R o-rwx "${DATA_PATH}"  # Remove all access from others
fi

# Drop into bento_user from root and execute the CMD specified for the image
exec gosu bento_user "$@"
