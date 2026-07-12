set -e

# Debug
#cross build --target aarch64-unknown-linux-gnu
#sftp comma@192.168.16.150 <<< $'put target/aarch64-unknown-linux-gnu/debug/ioniq2mqtt'

# Release
cross build --target aarch64-unknown-linux-gnu --release
sftp comma@10.80.107.98 <<< $'put target/aarch64-unknown-linux-gnu/release/ioniq2mqtt'
