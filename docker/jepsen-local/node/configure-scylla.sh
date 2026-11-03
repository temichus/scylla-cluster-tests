#!/bin/bash

# Configure Scylla for this node the way SCT configures its nodes: Jepsen with
# --no-install-scylla (scylla.sc.db) never writes scylla.yaml. Runs at every
# boot; idempotent.
echo "Configuring scylla.yaml" >> /var/log/jepsen-setup.log
YAML=/etc/scylla/scylla.yaml
sed -i \
    -e "s/^listen_address:.*/listen_address: $(hostname)/" \
    -e "s/^rpc_address:.*/rpc_address: $(hostname)/" \
    -e 's/- seeds:.*/- seeds: "n1"/' \
    "$YAML"
grep -q '^cluster_name:' "$YAML" || \
    printf "\ncluster_name: 'jepsen'\n" >> "$YAML"
grep -q '^experimental_features:' "$YAML" || \
    printf '\nexperimental_features:\n    - strongly-consistent-tables\n' >> "$YAML"
# Several nodes share one host: 2 shards, 2 GiB, overprovisioned (what Jepsen's
# own /etc/default/scylla-server template used to set).
printf 'CPUSET="--smp 2"\n' > /etc/scylla.d/cpuset.conf
printf 'MEM_CONF="--memory 2G --overprovisioned"\n' > /etc/scylla.d/memory.conf
