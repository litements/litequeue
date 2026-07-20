#!/usr/bin/env bash

set -o nounset
set -o errexit

build_sqlite () {
    sqlite_version="${1:?SQLite version is required}"
    sqlite_year="${2:?SQLite release year is required}"
    sqlite_archive_version="${3:?SQLite archive version is required}"
    sqlite_prefix="/opt/sqlite/${sqlite_version}"
    sqlite_archive="/tmp/sqlite-${sqlite_version}.tar.gz"
    sqlite_source="/tmp/sqlite-source-${sqlite_version}"
    sqlite_url="https://www.sqlite.org/${sqlite_year}/sqlite-autoconf-${sqlite_archive_version}.tar.gz"

    curl -LsSf "${sqlite_url}" -o "${sqlite_archive}"
    mkdir -p "${sqlite_source}"
    tar -xzf "${sqlite_archive}" --strip-components=1 -C "${sqlite_source}"

    cd "${sqlite_source}"
    CPPFLAGS=-DSQLITE_ENABLE_DESERIALIZE ./configure \
        --prefix="${sqlite_prefix}" \
        --disable-static
    make -j2
    make install

    actual_version=$( \
        LD_LIBRARY_PATH="${sqlite_prefix}/lib" \
        LD_PRELOAD="${sqlite_prefix}/lib/libsqlite3.so.0" \
        python3 -c 'import sqlite3; print(sqlite3.sqlite_version)' \
    )
    test "${actual_version}" = "${sqlite_version}"

    rm -rf "${sqlite_source}" "${sqlite_archive}"
}

main () {
    build_sqlite "$@"
    exit 0
}

main "$@"
