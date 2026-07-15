cmake_minimum_required(VERSION 3.25)
project(rapid_inbox_ingestd LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(SQLite3 3.20 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ICU REQUIRED COMPONENTS uc)
find_library(UNISTRING_LIBRARY NAMES unistring libunistring.so.2 REQUIRED)

set(INGESTD_LIB_SOURCES
    src/batch_writer.cpp
    src/config.cpp
    src/domain_cache.cpp
    src/domain_matcher.cpp
    src/id.cpp
    src/ingest_app.cpp
    src/instance_lock.cpp
    src/json_util.cpp
    src/logger.cpp
    src/mail_queue.cpp
    src/mime_parser.cpp
    src/sha256.cpp
    src/smtp_server.cpp
    src/smtp_session.cpp
    src/sqlite_db.cpp
    src/storage_path.cpp
    src/time_utils.cpp
    src/verification_code.cpp
)

set(INGESTD_SOURCES
    src/main.cpp
)

add_library(ingestd_core STATIC ${INGESTD_LIB_SOURCES})
target_include_directories(ingestd_core PUBLIC src)
target_compile_options(ingestd_core PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(ingestd_core PUBLIC SQLite::SQLite3 OpenSSL::Crypto ${UNISTRING_LIBRARY} ICU::uc)

add_executable(rapid-inbox-ingestd ${INGESTD_SOURCES})
target_compile_options(rapid-inbox-ingestd PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(rapid-inbox-ingestd PRIVATE ingestd_core)

add_executable(ingestd_tests
    tests/test_main.cpp
    tests/test_config.cpp
    tests/test_domain_cache.cpp
    tests/test_domain_matcher.cpp
    tests/test_ingest_app.cpp
    tests/test_instance_lock.cpp
    tests/test_logger.cpp
    tests/test_mail_queue.cpp
    tests/test_mime_parser.cpp
    tests/test_smtp_server.cpp
    tests/test_smtp_session.cpp
    tests/test_batch_writer.cpp
    tests/test_sqlite_db.cpp
    tests/test_storage_utils.cpp
    tests/test_verification_code.cpp
)
target_compile_options(ingestd_tests PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(ingestd_tests PRIVATE ingestd_core)
target_compile_definitions(ingestd_tests PRIVATE RAPID_INBOX_REPO_ROOT="${CMAKE_CURRENT_SOURCE_DIR}/../..")

enable_testing()
add_test(NAME ingestd_tests COMMAND ingestd_tests)
