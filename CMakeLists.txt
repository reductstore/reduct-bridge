cmake_minimum_required(VERSION 3.16)
project(reduct_bridge)

find_package(ament_cmake REQUIRED)
find_program(CARGO_EXECUTABLE cargo REQUIRED)

set(CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo")
set(REDUCT_BRIDGE_EXECUTABLE "${CARGO_TARGET_DIR}/release/reduct-bridge")

add_custom_command(
    OUTPUT "${REDUCT_BRIDGE_EXECUTABLE}"
    COMMAND
        "${CMAKE_COMMAND}" -E env
        "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
        "${CARGO_EXECUTABLE}" build
        --release
        --locked
        --no-default-features
        --features ros2
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    DEPENDS
        Cargo.toml
        Cargo.lock
        .cargo/config.toml
    VERBATIM
)

add_custom_target(reduct_bridge_build ALL DEPENDS "${REDUCT_BRIDGE_EXECUTABLE}")

install(
    PROGRAMS "${REDUCT_BRIDGE_EXECUTABLE}"
    DESTINATION "lib/${PROJECT_NAME}"
)

ament_package()
