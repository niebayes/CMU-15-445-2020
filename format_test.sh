#!/bin/bash

cd /home/lzx/bustub_backup/bustub/build

# format tests.
make format -j8
make check-lint -j8
make check-clang-tidy -j8

cd ..