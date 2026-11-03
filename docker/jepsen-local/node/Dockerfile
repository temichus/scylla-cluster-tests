# See https://salsa.debian.org/jgoerzen/docker-debian-base
# See https://hub.docker.com/r/jgoerzen/debian-base-standard
FROM jgoerzen/debian-base-minimal:bookworm as debian-addons
FROM debian:bookworm-slim

COPY --from=debian-addons /usr/local/preinit/ /usr/local/preinit/
COPY --from=debian-addons /usr/local/bin/ /usr/local/bin/
COPY --from=debian-addons /usr/local/debian-base-setup/ /usr/local/debian-base-setup/

RUN run-parts --exit-on-error --verbose /usr/local/debian-base-setup

ENV container=docker
STOPSIGNAL SIGRTMIN+3

# Basic system stuff
RUN apt-get -qy update && \
    apt-get -qy install \
        apt-transport-https

# Install packages
RUN apt-get -qy update && \
    apt-get -qy install \
        dos2unix openssh-server pwgen

# When run, boot-debian-base will call this script, which does final
# per-db-node setup stuff.
ADD setup-jepsen.sh /usr/local/preinit/03-setup-jepsen
RUN chmod +x /usr/local/preinit/03-setup-jepsen

# Configure SSHD
RUN sed -i "s/#PermitRootLogin prohibit-password/PermitRootLogin yes/g" /etc/ssh/sshd_config

# Enable SSH server
ENV DEBBASE_SSH enabled

# Install Jepsen deps
RUN apt-get -qy update && \
    apt-get -qy install \
        build-essential bzip2 ca-certificates curl dirmngr dnsutils faketime iproute2 iptables iputils-ping libzip4 logrotate lsb-release man man-db netcat-openbsd net-tools ntpdate psmisc python3 rsyslog sudo tar tcpdump unzip vim wget

EXPOSE 22
CMD ["/usr/local/bin/boot-debian-base"]

# --- ScyllaDB master, pre-installed so Jepsen runs with --no-install-scylla ---
RUN mkdir -p /etc/apt/keyrings && \
    for k in a43e06657bac99e3 c503c686b007f39e; do curl -fsSL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x$k&options=mr" | gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/keyrings/scylladb.gpg --import; done && chmod 644 /etc/apt/keyrings/scylladb.gpg && \
    curl -fsSL https://downloads.scylladb.com/unstable/scylla/master/deb/unified/latest/scylladb-master/scylla.list -o /etc/apt/sources.list.d/scylla.list && \
    apt-get -qy update && apt-get -qy install scylla && \
    scylla_dev_mode_setup --developer-mode 1 && \
    printf 'MEM_CONF="--memory 2G"\n' > /etc/scylla.d/memory.conf && \
    systemctl disable scylla-server && \
    rm -rf /var/lib/apt/lists/*

# --- Jepsen 0.2.4 on Debian 12: `apt-get install netcat` has no candidate (virtual package),
#     and jepsen.db/tcpdump hard-codes /usr/sbin/tcpdump. Same symlink as SCT jepsen_test.py:58.
RUN apt-get -qy update && apt-get -qy install equivs && cd /tmp && \
    printf 'Section: misc\nPriority: optional\nStandard-Version: 3.9.2\nPackage: netcat\nVersion: 1.10-99\nDepends: netcat-openbsd\nDescription: dummy netcat for Jepsen 0.2.4 on Debian 12\n' > netcat.ctl && \
    equivs-build netcat.ctl && dpkg -i netcat_1.10-99_all.deb && rm -f netcat* && \
    ln -sf /usr/bin/tcpdump /usr/sbin/tcpdump && rm -rf /var/lib/apt/lists/*
