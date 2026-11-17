#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
ARG HAS_MACOS_SDK="false"

FROM ubuntu:20.04

ARG RUST_VERSION=1.100.0
ARG PROTOC_VERSION=30.2

ENV DEBIAN_FRONTEND=noninteractive \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl git unzip zip python3 binutils build-essential gcc-10 g++-10 openjdk-17-jdk-headless \
 && rm -rf /var/lib/apt/lists/* \
 && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17

RUN mkdir -p /opt/comet-build \
 && case "$(uname -m)" in \
      x86_64) p=x86_64; sha=327e9397c6fb3ea2a542513a3221334c6f76f7aa524a7d2561142b67b312a01f ;; \
      aarch64) p=aarch_64; sha=a3173ea338ef91b1605b88c4f8120d6c8ccf36f744d9081991d595d0d4352996 ;; \
      *) exit 1 ;; esac \
 && curl -fsSLo /opt/comet-build/protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${p}.zip" \
 && echo "$sha  /opt/comet-build/protoc.zip" | sha256sum -c - \
 && unzip -q /opt/comet-build/protoc.zip -d /usr/local bin/protoc 'include/*' \
 && rm /opt/comet-build/protoc.zip

ENV CC=gcc-10 \
    CXX=g++-10 \
    JAVA_HOME=/usr/lib/jvm/java-17 \
    RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
    RUSTUP_TOOLCHAIN=${RUST_VERSION}

RUN curl --proto '=https' --tlsv1.2 -fsSLo /opt/comet-build/rustup-init.sh https://sh.rustup.rs \
 && echo 'd890e2fef6fa991b19cde2e608199bc1dca4e37f41f884edf8849ff693048de6  /opt/comet-build/rustup-init.sh' | sha256sum -c - \
 && sh /opt/comet-build/rustup-init.sh -y --no-modify-path --profile minimal --default-toolchain "${RUST_VERSION}" \
 && rm /opt/comet-build/rustup-init.sh \
 && rustc --version \
 && ldd --version | head -1
