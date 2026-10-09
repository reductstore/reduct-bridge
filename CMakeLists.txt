cmake_minimum_required(VERSION 3.16)
project(reduct_bridge)

find_package(ament_cmake REQUIRED)

file(STRINGS "${CMAKE_CURRENT_SOURCE_DIR}/rust-toolchain.toml" RUST_TOOLCHAIN_LINE
    REGEX "^[ \\t]*channel[ \\t]*=")
string(REGEX MATCH "\"([^\"]+)\"" RUST_TOOLCHAIN_MATCH "${RUST_TOOLCHAIN_LINE}")
set(RUST_TOOLCHAIN "${CMAKE_MATCH_1}")
if(NOT RUST_TOOLCHAIN)
    message(FATAL_ERROR "Could not resolve the Rust channel from rust-toolchain.toml")
endif()

find_program(RUSTUP_EXECUTABLE rustup)
set(RUSTUP_ENV)

if(NOT RUSTUP_EXECUTABLE)
    set(RUSTUP_VERSION "1.29.0")
    if(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
        set(RUSTUP_TARGET "x86_64-unknown-linux-gnu")
        set(RUSTUP_SHA256 "4acc9acc76d5079515b46346a485974457b5a79893cfb01112423c89aeb5aa10")
    elseif(CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
        set(RUSTUP_TARGET "aarch64-unknown-linux-gnu")
        set(RUSTUP_SHA256 "9732d6c5e2a098d3521fca8145d826ae0aaa067ef2385ead08e6feac88fa5792")
    else()
        message(FATAL_ERROR
            "rustup is required on unsupported host architecture: ${CMAKE_HOST_SYSTEM_PROCESSOR}")
    endif()

    set(RUSTUP_HOME "${CMAKE_CURRENT_BINARY_DIR}/rustup")
    set(CARGO_HOME "${CMAKE_CURRENT_BINARY_DIR}/cargo-home")
    set(RUSTUP_INIT "${CMAKE_CURRENT_BINARY_DIR}/rustup-init")
    set(RUSTUP_ENV "RUSTUP_HOME=${RUSTUP_HOME}" "CARGO_HOME=${CARGO_HOME}")

    file(DOWNLOAD
        "https://static.rust-lang.org/rustup/archive/${RUSTUP_VERSION}/${RUSTUP_TARGET}/rustup-init"
        "${RUSTUP_INIT}"
        EXPECTED_HASH "SHA256=${RUSTUP_SHA256}"
        TLS_VERIFY ON
        STATUS RUSTUP_DOWNLOAD_STATUS
    )
    list(GET RUSTUP_DOWNLOAD_STATUS 0 RUSTUP_DOWNLOAD_RESULT)
    if(NOT RUSTUP_DOWNLOAD_RESULT EQUAL 0)
        list(GET RUSTUP_DOWNLOAD_STATUS 1 RUSTUP_DOWNLOAD_ERROR)
        message(FATAL_ERROR "Failed to download rustup-init: ${RUSTUP_DOWNLOAD_ERROR}")
    endif()

    execute_process(
        COMMAND chmod +x "${RUSTUP_INIT}"
        RESULT_VARIABLE RUSTUP_CHMOD_RESULT
    )
    if(NOT RUSTUP_CHMOD_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to make rustup-init executable")
    endif()
    execute_process(
        COMMAND
            "${CMAKE_COMMAND}" -E env ${RUSTUP_ENV}
            "${RUSTUP_INIT}"
            -y
            --no-modify-path
            --profile minimal
            --default-toolchain "${RUST_TOOLCHAIN}"
        RESULT_VARIABLE RUSTUP_INIT_RESULT
    )
    if(NOT RUSTUP_INIT_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to install Rust ${RUST_TOOLCHAIN}")
    endif()
    set(RUSTUP_EXECUTABLE "${CARGO_HOME}/bin/rustup")
else()
    execute_process(
        COMMAND
            "${RUSTUP_EXECUTABLE}" toolchain install "${RUST_TOOLCHAIN}"
            --profile minimal
        RESULT_VARIABLE RUSTUP_TOOLCHAIN_RESULT
    )
    if(NOT RUSTUP_TOOLCHAIN_RESULT EQUAL 0)
        message(FATAL_ERROR "Failed to install Rust ${RUST_TOOLCHAIN}")
    endif()
endif()

set(CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo")
set(REDUCT_BRIDGE_EXECUTABLE "${CARGO_TARGET_DIR}/release/reduct-bridge")

add_custom_command(
    OUTPUT "${REDUCT_BRIDGE_EXECUTABLE}"
    COMMAND
        "${CMAKE_COMMAND}" -E env
        ${RUSTUP_ENV}
        "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
        "${RUSTUP_EXECUTABLE}" run "${RUST_TOOLCHAIN}" cargo build
        --release
        --locked
        --no-default-features
        --features ros2
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    DEPENDS
        Cargo.toml
        Cargo.lock
        .cargo/config.toml
        rust-toolchain.toml
    VERBATIM
)

add_custom_target(reduct_bridge_build ALL DEPENDS "${REDUCT_BRIDGE_EXECUTABLE}")

install(
    PROGRAMS "${REDUCT_BRIDGE_EXECUTABLE}"
    DESTINATION "lib/${PROJECT_NAME}"
)

ament_package()
