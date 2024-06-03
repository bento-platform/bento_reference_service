#!/bin/bash

source /create_service_user.bash

# Set up Git configuration if needed (useful for development images / containers)
gosu bento_user /bin/bash -c '/set_gitconfig.bash'

# Addition to base entrypoint: fix default production temporary folder permissions
if [[ -d '/reference/tmp' ]]; then
  chown -R bento_user:bento_user '/reference/tmp'
fi

# Drop into bento_user from root and execute the CMD specified for the image
exec gosu bento_user "$@"
