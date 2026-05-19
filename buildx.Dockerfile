# syntax=docker/dockerfile:1
ARG BASE_IMAGE=ubuntu:24.04

FROM ${BASE_IMAGE} AS builder

RUN groupadd --gid 10001 reduct \
    && useradd --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent --shell /usr/sbin/nologin reduct

RUN mkdir -p /data && chown 10001:10001 /data

FROM ${BASE_IMAGE}

ARG ROS_DISTRO=""

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN if [ -n "$ROS_DISTRO" ]; then \
        apt-get update \
        && apt-get install -y --no-install-recommends \
            curl \
            gnupg \
            lsb-release \
        && curl -sSL https://raw.githubusercontent.com/ros/rosdistro/master/ros.key -o /usr/share/keyrings/ros-archive-keyring.gpg \
        && echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/ros-archive-keyring.gpg] http://packages.ros.org/ros2/ubuntu $(lsb_release -cs) main" > /etc/apt/sources.list.d/ros2.list \
        && apt-get update \
        && apt-get install -y --no-install-recommends \
            ros-${ROS_DISTRO}-ros-core \
        && rm -rf /var/lib/apt/lists/*; \
    else \
        true; \
    fi

ENV ROS_DISTRO=${ROS_DISTRO}

COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /etc/group /etc/group
COPY --from=builder /etc/shadow /etc/shadow
COPY --from=builder /etc/gshadow /etc/gshadow
COPY --chown=10001:10001 --from=builder /data /data

COPY .image-build/usr/local/bin/reduct-bridge /usr/local/bin/reduct-bridge
COPY .image-build/usr/local/bin/reduct-cli /usr/local/bin/reduct-cli
COPY docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh

EXPOSE 8383
USER 10001:10001

VOLUME ["/data"]

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["reduct-bridge"]
