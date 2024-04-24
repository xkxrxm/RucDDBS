FROM ubuntu:20.04

ENV PATH /home/xkx/.local/bin:$PATH

RUN set -ex; \
  apt-get update; \
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
  jq sudo vim wget curl python3 python3-dev python3-pip gdb libcurl4-openssl-dev build-essential libncurses5-dev gnutls-dev bison zlib1g-dev ccache \
  cmake ninja-build libuv1-dev git g++ make openjdk-11-jdk openssh-client \
  libssl-dev libgflags-dev libleveldb-dev libsnappy-dev openssl \
  lcov libbz2-dev liblz4-dev libzstd-dev libboost-context-dev \
  ca-certificates libc-ares-dev libc-ares2 m4 pkg-config tar gcc redis tcl libreadline-dev ncurses-dev; \
  rm -rf /var/lib/apt/lists/*

RUN useradd -rm -s /bin/bash -g sudo xkx && \
  echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers && \
  ln -s /usr/bin/python3.8 /usr/bin/python && \
  mkdir /var/crash && chown -R xkx /var/crash

USER xkx
WORKDIR /home/xkx

RUN mkdir -p $HOME/Downloads/protobuf && cd $HOME/Downloads/protobuf && \
  curl -fsSL https://github.com/protocolbuffers/protobuf/archive/refs/tags/v21.12.tar.gz | \
  tar -xzf - --strip-components=1 && \
  cmake \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_SHARED_LIBS=yes \
  -Dprotobuf_BUILD_TESTS=OFF \
  -Dprotobuf_ABSL_PROVIDER=package \
  -S . -B cmake-out && \
  cmake --build cmake-out -- -j ${NCPU:-4} && \
  sudo cmake --build cmake-out --target install -- -j ${NCPU:-4} && \
  sudo ldconfig && \
  cd ../ && \
  rm -rf protobuf

RUN git clone https://github.com/monographdb/glog.git glog && \
  cd glog && \
  cmake -S . -B build -G "Unix Makefiles" && \
  cmake --build build -j6 && \
  sudo cmake --build build --target install && \
  cd ../ && \
  rm -rf glog

# install brpc
RUN git clone https://github.com/monographdb/brpc.git brpc && \
  cd brpc &&  \
  mkdir build && cd build &&\
  cmake .. \
  -DWITH_GLOG=ON \
  -DBUILD_SHARED_LIBS=ON && \
  cmake --build . -j6 && \
  sudo cp -r ./output/include/* /usr/include/ && \
  sudo cp ./output/lib/* /usr/lib/ && \
  cd ../../ && \
  rm -rf brpc

# install braft
RUN git clone https://github.com/monographdb/braft.git braft && \
  cd braft && \
  sed -i 's/libbrpc.a//g' CMakeLists.txt && \
  mkdir bld && cd bld && \
  cmake .. -DBRPC_WITH_GLOG=ON &&\
  cmake --build . -j6 && \
  sudo cp -r ./output/include/* /usr/include/ && \
  sudo cp ./output/lib/* /usr/lib/ && \
  cd ../../ && \
  rm -rf braft
