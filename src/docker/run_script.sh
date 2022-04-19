#!/bin/bash
# It utilizes following ENV variables:
#   * MALLOC_TYPE – specifies type of memory allocation manager

# Uses passed string as an ENV name, prints ENV name and value, if it exists
# If second (arbitrary) parameter is present, doesn't print the value
print_if_defined() {
  env_name="$1"
  shade_value="$2"
  if [ -n "${!env_name}" ]; then
    if [ -z "$shade_value" ]; then
      echo "  $env_name: ${!env_name}"
    else
      echo "  $env_name: {defined, value shaded}"
    fi
  fi
}

select_malloc() {
  if [[ "${MALLOC_TYPE^^}" == "JEMALLOC" ]]; then
    echo "Using jemalloc"
    [ -n "$LD_PRELOAD" ] && echo "WARNING: MALLOC_TYPE is set to JEMALLOC, but LD_PRELOAD variable is defined. It's value will be overriden. Consider setting MALLOC_TYPE to DEFAULT to keep your custom LD_PRELOAD value."
    export LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so
  elif [[ "${MALLOC_TYPE^^}" == "DEFAULT" ]] || [ -z "$MALLOC_TYPE" ]; then
    echo "Using default memory allocation manager"
  else
    echo "ERROR: Invalid MALLOC_TYPE '$MALLOC_TYPE' value, expected 'JEMALLOC' or 'DEFAULT'"
    exit 1
  fi
  print_if_defined "LD_PRELOAD"
}

# Start of execution
# Check all used ENVs
echo "Checking ENV variables, used by the script:"
print_if_defined "DIRPATH"
print_if_defined "MALLOC_TYPE"

select_malloc && \
    exec python3 "${DIRPATH}"/launcher.py
