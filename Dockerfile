# syntax=docker/dockerfile:1.7
FROM alpine:latest

# Preinstall multiple Python versions; override at build time
ARG PYTHON_VERSIONS="\
    3.12.5 \
"
ARG SCALER_REPO="https://github.com/1597463007/scaler.git"
ARG SCALER_REF="ec2_dev"

# ---- System deps for CPython on musl + small QoL tools ----
RUN apk add --no-cache \
    bash curl git ca-certificates \
    coreutils \ 
    build-base \
    bzip2 bzip2-dev \
    zlib zlib-dev \
    xz xz-dev \
    readline-dev \
    sqlite-dev \
    openssl openssl-dev \
    libffi-dev \
    util-linux-dev \
    ncurses-dev \
    tk tk-dev \
    wget tar \
    boost-dev capnproto-dev

# ---- pyenv ----
ENV PYENV_ROOT=/opt/pyenv
ENV PATH=$PYENV_ROOT/bin:$PYENV_ROOT/shims:/usr/local/bin:$PATH
RUN git clone --depth 1 https://github.com/pyenv/pyenv.git "$PYENV_ROOT"
ENV PYTHON_CONFIGURE_OPTS="--enable-optimizations"
ENV MAKEFLAGS="-j$(getconf _NPROCESSORS_ONLN || echo 1)"

RUN set -eux; \
    eval "$(pyenv init -)"; \
    for v in $PYTHON_VERSIONS; do pyenv install -s "$v"; done; \
    pyenv global $PYTHON_VERSIONS; \
    pyenv rehash; \
    python -VV

# ---- uv (fast installer/resolver) ----
RUN curl -LsSf https://astral.sh/uv/install.sh | env UV_INSTALL_DIR=/usr/local/bin sh

# ---- Scaler ----
RUN set -eux; \
    git clone --branch "${SCALER_REF}" --depth 1 "${SCALER_REPO}" scaler; \
    cd scaler; \
    eval "$(pyenv init -)"; \
    python -m pip install --upgrade pip; \
    pip install --no-cache-dir -e .

# ---- Runtime env knobs ----
# e.g., "3.12" or "3.12.5"; must be among PYTHON_VERSIONS
ENV PYTHON_VERSION=""
ENV PYTHON_REQUIREMENTS=""
ENV COMMAND=""
ENV VENV_PATH="/opt/venv"
ENV SSL_CERT_DIR=/etc/ssl/certs SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

# ---- Entrypoint ----
COPY <<'ENTRYPOINT_SH' /entrypoint.sh
#!/usr/bin/env bash
set -euo pipefail

export PYENV_ROOT="${PYENV_ROOT:-/opt/pyenv}"
export PATH="$PYENV_ROOT/bin:$PYENV_ROOT/shims:/usr/local/bin:$PATH"
eval "$(pyenv init -)"

resolve_python_version() {
  local req="${1:-}"
  # List installed CPython versions (bare numbers)
  mapfile -t installed < <(pyenv versions --bare | grep -E '^[0-9]+\.[0-9]+(\.[0-9]+)?$' || true)

  if [[ "${#installed[@]}" -eq 0 ]]; then
    echo "No Python versions are installed under pyenv." >&2
    return 1
  fi

  if [[ -z "$req" ]]; then
    # Default to the current pyenv 'python'
    command -v python
    return 0
  fi

  # Exact match first
  for v in "${installed[@]}"; do
    if [[ "$v" == "$req" ]]; then
      echo "$PYENV_ROOT/versions/$v/bin/python"
      return 0
    fi
  done

  # Prefix match (e.g., "3.12" -> highest 3.12.x)
  mapfile -t matches < <(printf "%s\n" "${installed[@]}" | grep -E "^${req//./\\.}(\.|$)" || true)
  if [[ "${#matches[@]}" -eq 0 ]]; then
    echo "Requested PYTHON_VERSION='$req' not found. Installed: ${installed[*]}" >&2
    return 1
  fi
  # Pick highest patch (needs coreutils sort -V)
  local best
  best="$(printf "%s\n" "${matches[@]}" | sort -V | tail -1)"
  echo "$PYENV_ROOT/versions/$best/bin/python"
}

main() {
  # Resolve target interpreter
  TARGET_PY="$(resolve_python_version "${PYTHON_VERSION:-}")"

  # If user asked for a specific interpreter, also set PYENV_VERSION so bare 'python' uses it
  if [[ -n "${PYTHON_VERSION:-}" ]]; then
    # Derive canonical version string for PYENV_VERSION
    canon_ver="$(basename "$(dirname "$TARGET_PY")")"
    export PYENV_VERSION="$canon_ver"
  fi

  uv venv "${VENV_PATH}" --python "${TARGET_PY}"
  export PATH="${VENV_PATH}/bin:${PATH}"
  echo "Environment Variables:"
  export

  curl -O https://capnproto.org/capnproto-c++-1.1.0.tar.gz
  tar zxf capnproto-c++-1.1.0.tar.gz
  cd capnproto-c++-1.1.0
  ./configure
  make -j6 check
  make install
  cd ..

  uv pip install --python "${VENV_PATH}/bin/python" -v scaler

  # If requirements provided, create venv & install using uv
  if [[ -n "${PYTHON_REQUIREMENTS:-}" ]]; then
    tmpreq="$(mktemp)"
    printf "%s\n" "${PYTHON_REQUIREMENTS}" | tr ' ,;' '\n' | sed -E '/^\s*$/d' > "${tmpreq}"
    uv pip install --python "${VENV_PATH}/bin/python" -v -r "${tmpreq}"
    rm -f "${tmpreq}"
  fi

  if [[ -z "${COMMAND:-}" ]]; then
    echo "[info] No COMMAND provided; opening shell."
    python -V || true
    which python || true
    pyenv versions || true
    exec bash
  else
    echo "[info] Executing COMMAND: ${COMMAND}"
    exec bash -lc "source ${VENV_PATH}/bin/activate && ${COMMAND}"
  fi
}

main "$@"
ENTRYPOINT_SH

RUN chmod +x /entrypoint.sh
SHELL ["/bin/bash", "-lc"]
ENTRYPOINT ["/entrypoint.sh"]
