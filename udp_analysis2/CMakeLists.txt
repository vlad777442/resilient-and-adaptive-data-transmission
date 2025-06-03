cmake_minimum_required(VERSION 3.10)
project(UdpPacketLossTracker)

set(CMAKE_CXX_STANDARD 17)

# Protobuf
find_package(Protobuf REQUIRED)

# Boost (only system is needed for Boost.Asio)
find_package(Boost REQUIRED COMPONENTS system)

# Include directories
include_directories(
    ${Protobuf_INCLUDE_DIRS}
    ${Boost_INCLUDE_DIRS}
)

# Protobuf-generated source files
set(PROTO_SRCS
    packet.pb.cc
)

set(PROTO_HDRS
    packet.pb.h
)

# Sender
add_executable(sender udp_sender.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(sender ${Protobuf_LIBRARIES} ${Boost_LIBRARIES})

# Receiver
add_executable(receiver udp_receiver.cpp ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(receiver ${Protobuf_LIBRARIES} ${Boost_LIBRARIES})
