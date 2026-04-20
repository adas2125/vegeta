#!/usr/bin/env bash
set -euo pipefail

# Setup helper for a local Vegeta checkout on Linux.
# It can install system packages (if requested), then fetch Go deps and build.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_SYSTEM_PACKAGES=1
RUN_SMOKE_TEST=1

usage() {
  cat <<'EOF'
Usage: scripts/setup_vegeta.sh [options]

Options:
  --no-system-packages  Skip apt/dnf/yum/pacman package installation
  --no-smoke-test       Skip quick run checks after build
  -h, --help            Show this help

What this does:
  1) Ensures Go, git, make are present (installs them unless --no-system-packages)
  2) Runs go mod download/tidy and installs codegen tools
  3) Builds the vegeta binary into ./vegeta
  4) Runs a quick smoke test (version + tiny local attack) unless disabled
EOF
}

log() {
  printf '[setup] %s\n' "$*"
}

has_cmd() {
  command -v "$1" >/dev/null 2>&1
}

detect_arch() {
  case "$(uname -m)" in
    x86_64|amd64)
      echo "amd64"
      ;;
    aarch64|arm64)
      echo "arm64"
      ;;
    *)
      echo ""
      ;;
  esac
}

require_sudo() {
  if has_cmd sudo; then
    echo "sudo"
  elif [[ "${EUID}" -eq 0 ]]; then
    echo ""
  else
    log "Need root privileges to install system packages. Re-run as root or install manually."
    exit 1
  fi
}

detect_pkg_manager() {
  if has_cmd apt-get; then
    echo "apt"
  elif has_cmd dnf; then
    echo "dnf"
  elif has_cmd yum; then
    echo "yum"
  elif has_cmd pacman; then
    echo "pacman"
  else
    echo ""
  fi
}

install_system_packages() {
  local pkgm sudo_cmd
  pkgm="$(detect_pkg_manager)"
  if [[ -z "$pkgm" ]]; then
    log "No supported package manager found (apt/dnf/yum/pacman)."
    log "Install Go >= 1.22, git, and make manually, then rerun with --no-system-packages."
    exit 1
  fi

  sudo_cmd="$(require_sudo)"
  log "Installing system packages using $pkgm"

  case "$pkgm" in
    apt)
      ${sudo_cmd} apt-get update
      ${sudo_cmd} apt-get install -y ca-certificates curl git make golang-go
      ;;
    dnf)
      ${sudo_cmd} dnf install -y ca-certificates curl git make golang
      ;;
    yum)
      ${sudo_cmd} yum install -y ca-certificates curl git make golang
      ;;
    pacman)
      ${sudo_cmd} pacman -Sy --noconfirm ca-certificates curl git make go
      ;;
  esac
}

install_modern_go_from_tarball() {
  local arch go_file go_url sudo_cmd
  local candidates=("1.24.3" "1.23.9" "1.22.12")

  arch="$(detect_arch)"
  if [[ -z "$arch" ]]; then
    log "Unsupported CPU architecture for automated Go tarball install: $(uname -m)"
    log "Install Go 1.22+ manually and re-run this script."
    exit 1
  fi

  for v in "${candidates[@]}"; do
    go_file="go${v}.linux-${arch}.tar.gz"
    go_url="https://go.dev/dl/${go_file}"
    if curl -fsI "$go_url" >/dev/null 2>&1; then
      log "Installing Go ${v} from ${go_url}"
      sudo_cmd="$(require_sudo)"

      rm -f "/tmp/${go_file}"
      curl -fL "$go_url" -o "/tmp/${go_file}"
      ${sudo_cmd} rm -rf /usr/local/go
      ${sudo_cmd} tar -C /usr/local -xzf "/tmp/${go_file}"
      rm -f "/tmp/${go_file}"

      export PATH="/usr/local/go/bin:$PATH"
      hash -r
      log "Upgraded toolchain: $(go version)"
      return
    fi
  done

  log "Could not find a downloadable Go tarball candidate from go.dev"
  log "Install Go 1.22+ manually and rerun this script."
  exit 1
}

ensure_tools_present() {
  local missing=()
  for tool in go git make; do
    if ! has_cmd "$tool"; then
      missing+=("$tool")
    fi
  done

  if [[ "${#missing[@]}" -gt 0 ]]; then
    log "Missing required tools: ${missing[*]}"
    if [[ "$INSTALL_SYSTEM_PACKAGES" -eq 1 ]]; then
      install_system_packages
    else
      log "Install missing tools manually and rerun."
      exit 1
    fi
  fi

  log "Using $(go version)"
}

ensure_go_version() {
  local major minor
  major="$(go env GOVERSION | sed -E 's/^go([0-9]+)\..*$/\1/')"
  minor="$(go env GOVERSION | sed -E 's/^go[0-9]+\.([0-9]+).*$/\1/')"

  if [[ -z "$major" || -z "$minor" ]]; then
    log "Unable to detect Go version"
    exit 1
  fi

  if (( major < 1 || (major == 1 && minor < 22) )); then
    log "Go 1.22+ is required by this repository."
    log "Current version: $(go version)"

    if [[ "$INSTALL_SYSTEM_PACKAGES" -eq 1 ]]; then
      install_modern_go_from_tarball
      return
    fi

    exit 1
  fi
}

install_go_codegen_tools() {
  log "Installing codegen tools used by this repo"
  go install github.com/mailru/easyjson/...@latest
  go install github.com/shurcooL/vfsgen/...@latest
}

setup_repo() {
  cd "$ROOT_DIR"
  log "Downloading modules"
  go mod download

  log "Tidying module graph"
  go mod tidy

  install_go_codegen_tools

  log "Building vegeta binary"
  go build -o vegeta .
}

smoke_test() {
  cd "$ROOT_DIR"
  log "Running version check"
  ./vegeta -version

  log "Running tiny local attack smoke test"
  local server_pid
  go run ./cmd/simple_server -addr :18080 >/tmp/vegeta_setup_server.log 2>&1 &
  server_pid=$!

  # Make sure the local server is always cleaned up.
  trap 'kill "$server_pid" >/dev/null 2>&1 || true' EXIT
  sleep 1

  printf 'GET http://127.0.0.1:18080/\n' | ./vegeta attack -duration=1s -rate=5/s | ./vegeta report

  kill "$server_pid" >/dev/null 2>&1 || true
  trap - EXIT
}

main() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --no-system-packages)
        INSTALL_SYSTEM_PACKAGES=0
        ;;
      --no-smoke-test)
        RUN_SMOKE_TEST=0
        ;;
      -h|--help)
        usage
        exit 0
        ;;
      *)
        log "Unknown option: $1"
        usage
        exit 1
        ;;
    esac
    shift
  done

  ensure_tools_present
  ensure_go_version
  setup_repo

  if [[ "$RUN_SMOKE_TEST" -eq 1 ]]; then
    smoke_test
  fi

  log "Setup complete"
  log "Binary available at: $ROOT_DIR/vegeta"
}

main "$@"
