#!/bin/bash
set -euo pipefail

###############################################################################
# Floe v1 - Walrus Upload Utility
###############################################################################

FILE_TO_UPLOAD="${1:-}"
CUSTOM_CHUNK_SIZE_MB=""
EPOCHS=1
PARALLEL_JOBS=1
RESUME_UPLOAD_ID=""
EXPLICIT_RESUME=0
MAX_RETRIES=3
KEEP_STATE=0
STATE_DIR_OVERRIDE=""
STATE_FILE_OVERRIDE=""

# API
API_BASE="${FLOE_API_BASE:-http://localhost:3001/v1/uploads}"

# Curl hardening (avoid hanging forever on bad networks)
CURL_CONNECT_TIMEOUT_S="${FLOE_CURL_CONNECT_TIMEOUT_S:-5}"
CURL_MAX_TIME_S="${FLOE_CURL_MAX_TIME_S:-240}"
CURL_RETRY="${FLOE_CURL_RETRY:-3}"
CURL_RETRY_DELAY_S="${FLOE_CURL_RETRY_DELAY_S:-1}"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

show_help() {
  echo -e "${CYAN}Floe - Walrus Upload Utility${NC}"
  echo "Usage: $0 <file> [options]"
  echo ""
  echo "Options:"
  echo "  -c, --chunk <mb>      Chunk size in MB (0.25 - 10)"
  echo "  -e, --epochs <num>    Number of epochs (default: 1)"
  echo "  -p, --parallel <n>    Parallel uploads (default: 1)"
  echo "      --resume <id>     Resume an existing uploadId (skips already uploaded chunks)"
  echo "      --api <url>       Override API base (default: ${API_BASE})"
  echo "      --state <path>    Override state file path (disables auto state naming)"
  echo "      --state-dir <dir> Store state files under this directory"
  echo "      --keep-state      Keep the state file after a successful upload"
  echo "  -h, --help            Show help"
  exit 0
}

if [[ -z "$FILE_TO_UPLOAD" || "$FILE_TO_UPLOAD" == "-h" || "$FILE_TO_UPLOAD" == "--help" ]]; then
  show_help
fi

shift
while [[ $# -gt 0 ]]; do
  case $1 in
    -c|--chunk) CUSTOM_CHUNK_SIZE_MB="$2"; shift ;;
    -e|--epochs) EPOCHS="$2"; shift ;;
    -p|--parallel) PARALLEL_JOBS="$2"; shift ;;
    --resume) RESUME_UPLOAD_ID="$2"; EXPLICIT_RESUME=1; shift ;;
    --api) API_BASE="$2"; shift ;;
    --state) STATE_FILE_OVERRIDE="$2"; shift ;;
    --state-dir) STATE_DIR_OVERRIDE="$2"; shift ;;
    --keep-state) KEEP_STATE=1 ;;
    -h|--help) show_help ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

[[ ! -f "$FILE_TO_UPLOAD" ]] && echo "File not found" && exit 1

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

if ! command -v sha256sum >/dev/null 2>&1; then
  echo "sha256sum is required" >&2
  exit 1
fi

if ! [[ "$PARALLEL_JOBS" =~ ^[0-9]+$ ]] || [[ "$PARALLEL_JOBS" -le 0 ]]; then
  echo "Invalid --parallel value" >&2
  exit 1
fi

if ! [[ "$EPOCHS" =~ ^[0-9]+$ ]] || [[ "$EPOCHS" -le 0 ]]; then
  echo "Invalid --epochs value" >&2
  exit 1
fi

FILE_SIZE=$(stat -c%s "$FILE_TO_UPLOAD" 2>/dev/null || stat -f%z "$FILE_TO_UPLOAD")
CONTENT_TYPE=$(file --mime-type -b "$FILE_TO_UPLOAD" 2>/dev/null || echo "application/octet-stream")
TMP_DIR="$(mktemp -d -t floe-upload-XXXXXX)"

resolve_abs_path() {
  local p="$1"
  if command -v realpath >/dev/null 2>&1; then
    realpath "$p"
    return
  fi
  if command -v readlink >/dev/null 2>&1; then
    readlink -f "$p" 2>/dev/null || echo "$p"
    return
  fi
  echo "$p"
}

default_state_dir() {
  if [[ -n "${FLOE_STATE_DIR:-}" ]]; then
    echo "$FLOE_STATE_DIR"
    return
  fi
  if [[ -n "${XDG_STATE_HOME:-}" ]]; then
    echo "${XDG_STATE_HOME%/}/floe"
    return
  fi
  echo "${HOME%/}/.local/state/floe"
}

STATE_DIR="$(default_state_dir)"
if [[ -n "$STATE_DIR_OVERRIDE" ]]; then
  STATE_DIR="$STATE_DIR_OVERRIDE"
fi

mkdir -p "$STATE_DIR" 2>/dev/null || true

ABS_FILE_PATH="$(resolve_abs_path "$FILE_TO_UPLOAD")"
STATE_KEY="$(printf '%s' "${ABS_FILE_PATH}|${FILE_SIZE}|${CONTENT_TYPE}" | sha256sum | awk '{print $1}')"
DEFAULT_STATE_FILE="${STATE_DIR%/}/$(basename "$FILE_TO_UPLOAD").${STATE_KEY}.floe-upload.json"
STATE_FILE="${STATE_FILE_OVERRIDE:-$DEFAULT_STATE_FILE}"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

curl_json() {
  # Usage: curl_json <method> <url> [data]
  local method="$1"; shift
  local url="$1"; shift
  local data="${1:-}"

  if [[ -n "$data" ]]; then
    curl -sS --fail \
      --connect-timeout "$CURL_CONNECT_TIMEOUT_S" \
      --max-time "$CURL_MAX_TIME_S" \
      --retry "$CURL_RETRY" \
      --retry-delay "$CURL_RETRY_DELAY_S" \
      --retry-all-errors \
      -X "$method" "$url" \
      -H "Content-Type: application/json" \
      -d "$data"
  else
    curl -sS --fail \
      --connect-timeout "$CURL_CONNECT_TIMEOUT_S" \
      --max-time "$CURL_MAX_TIME_S" \
      --retry "$CURL_RETRY" \
      --retry-delay "$CURL_RETRY_DELAY_S" \
      --retry-all-errors \
      -X "$method" "$url"
  fi
}

print_api_error() {
  local resp_file="$1"
  local http_code="$2"

  local code=""
  local msg=""
  code=$(jq -r '.error.code // empty' "$resp_file" 2>/dev/null || true)
  msg=$(jq -r '.error.message // empty' "$resp_file" 2>/dev/null || true)

  if [[ -n "$code" || -n "$msg" ]]; then
    echo -e "${RED}HTTP ${http_code}${NC} ${code:-ERROR} ${msg}" >&2
  else
    echo -e "${RED}HTTP ${http_code}${NC} $(tr -d '\n' < "$resp_file" | head -c 240)" >&2
  fi
}

maybe_load_state() {
  if [[ -n "$RESUME_UPLOAD_ID" ]]; then
    return
  fi

  if [[ -f "$STATE_FILE" ]]; then
    local saved
    local file_id
    local completed_at

    saved=$(jq -r '.uploadId // empty' "$STATE_FILE" 2>/dev/null || true)
    file_id=$(jq -r '.fileId // empty' "$STATE_FILE" 2>/dev/null || true)
    completed_at=$(jq -r '.completedAt // empty' "$STATE_FILE" 2>/dev/null || true)

    # If the state file indicates a completed upload, don't auto-resume.
    if [[ -n "$file_id" || -n "$completed_at" ]]; then
      return
    fi

    if [[ -n "$saved" && "$saved" != "null" ]]; then
      RESUME_UPLOAD_ID="$saved"
      echo -e "${YELLOW}Resuming from state file:${NC} $STATE_FILE"
      echo -e "${YELLOW}Upload ID:${NC} $RESUME_UPLOAD_ID"
    fi
  fi
}

archive_state() {
  local reason="$1"
  if [[ ! -f "$STATE_FILE" ]]; then
    return
  fi
  local ts
  ts=$(date +%s)
  local dst="${STATE_FILE}.stale.${ts}"
  mv "$STATE_FILE" "$dst" 2>/dev/null || return
  echo -e "${YELLOW}State archived:${NC} $dst"
  echo -e "${YELLOW}Reason:${NC} $reason"
}

save_state() {
  local upload_id="$1"
  local chunk_size="$2"
  local total_chunks="$3"

  mkdir -p "$(dirname "$STATE_FILE")" 2>/dev/null || true

  cat > "$STATE_FILE" <<JSON
{
  "uploadId": "${upload_id}",
  "apiBase": "${API_BASE}",
  "file": "${FILE_TO_UPLOAD}",
  "fileAbs": "${ABS_FILE_PATH}",
  "fileSize": ${FILE_SIZE},
  "contentType": "${CONTENT_TYPE}",
  "chunkSize": ${chunk_size},
  "totalChunks": ${total_chunks},
  "epochs": ${EPOCHS},
  "savedAt": $(date +%s)
}
JSON
}

# Convert chunk size MB (supports decimals) -> bytes
chunk_mb_to_bytes() {
  local mb="$1"
  awk -v mb="$mb" 'BEGIN { printf "%.0f", (mb * 1024 * 1024) }'
}

maybe_load_state

echo -e "${CYAN}╔════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║              FLOE V1 UPLOADER              ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}API:${NC} $API_BASE"
echo -e "${YELLOW}File:${NC} $(basename "$FILE_TO_UPLOAD")"
echo -e "${YELLOW}Size:${NC} $(numfmt --to=iec-i "$FILE_SIZE")"
echo -e "${YELLOW}Type:${NC} $CONTENT_TYPE"
echo -e "${YELLOW}Epochs:${NC} $EPOCHS"
echo -e "${YELLOW}Parallel:${NC} $PARALLEL_JOBS"

UPLOAD_ID=""
CHUNK_SIZE=""
TOTAL_CHUNKS=""
STATUS_JSON=""

###############################################################################
# CREATE OR RESUME SESSION
###############################################################################

RESUMING=0

if [[ -n "$RESUME_UPLOAD_ID" ]]; then
  echo -e "\n${BLUE}▶ Fetching upload status (resume)...${NC}"

  STATUS_FILE="$TMP_DIR/status.resp.json"
  HTTP=$(curl -sS \
    --connect-timeout "$CURL_CONNECT_TIMEOUT_S" \
    --max-time "$CURL_MAX_TIME_S" \
    --retry "$CURL_RETRY" \
    --retry-delay "$CURL_RETRY_DELAY_S" \
    -o "$STATUS_FILE" \
    -w "%{http_code}" \
    -X GET "$API_BASE/$RESUME_UPLOAD_ID/status" || true)

  if [[ "$HTTP" =~ ^2 ]]; then
    STATUS_JSON=$(cat "$STATUS_FILE")

    UPLOAD_ID="$RESUME_UPLOAD_ID"
    CHUNK_SIZE=$(jq -r '.chunkSize' <<<"$STATUS_JSON")
    TOTAL_CHUNKS=$(jq -r '.totalChunks' <<<"$STATUS_JSON")

    if [[ -z "$CHUNK_SIZE" || "$CHUNK_SIZE" == "null" || -z "$TOTAL_CHUNKS" || "$TOTAL_CHUNKS" == "null" ]]; then
      echo -e "${RED}✗${NC} Invalid status response:" >&2
      echo "$STATUS_JSON" >&2
      exit 1
    fi

    echo -e "${GREEN}✓${NC} Resuming upload: $UPLOAD_ID"
    RESUMING=1
  else
    if [[ "$HTTP" == "404" ]]; then
      archive_state "uploadId not found (canceled/expired)"
    else
      archive_state "failed to fetch status (HTTP ${HTTP})"
      print_api_error "$STATUS_FILE" "$HTTP" || true
    fi

    if [[ "$EXPLICIT_RESUME" -eq 1 ]]; then
      echo -e "${RED}✗${NC} Could not resume uploadId: $RESUME_UPLOAD_ID" >&2
      echo -e "${YELLOW}Tip:${NC} remove --resume to create a new upload session" >&2
      exit 1
    fi

    RESUME_UPLOAD_ID=""
    STATUS_JSON=""
  fi
fi

if [[ "$RESUMING" -ne 1 ]]; then
  echo -e "\n${BLUE}▶ Creating upload session...${NC}"

  CHUNK_BYTES_JSON=""
  if [[ -n "$CUSTOM_CHUNK_SIZE_MB" ]]; then
    CHUNK_BYTES_JSON=",\"chunkSize\": $(chunk_mb_to_bytes "$CUSTOM_CHUNK_SIZE_MB")"
  fi

  JSON=$(cat <<JSON
{
  "filename": "$(basename "$FILE_TO_UPLOAD")",
  "contentType": "$CONTENT_TYPE",
  "sizeBytes": $FILE_SIZE,
  "epochs": $EPOCHS${CHUNK_BYTES_JSON}
}
JSON
)

  RESP=$(curl_json POST "$API_BASE/create" "$JSON" || true)

  if [[ -z "$RESP" ]]; then
    echo -e "${RED}✗${NC} Session creation failed" >&2
    exit 1
  fi

  UPLOAD_ID=$(jq -r '.uploadId' <<<"$RESP")
  CHUNK_SIZE=$(jq -r '.chunkSize' <<<"$RESP")
  TOTAL_CHUNKS=$(jq -r '.totalChunks' <<<"$RESP")

  [[ -z "$UPLOAD_ID" || "$UPLOAD_ID" == "null" ]] && echo "Session creation failed" && exit 1
  echo -e "${GREEN}✓ Upload ID: $UPLOAD_ID${NC}"

  save_state "$UPLOAD_ID" "$CHUNK_SIZE" "$TOTAL_CHUNKS"
  echo -e "${YELLOW}State:${NC} $STATE_FILE"
fi

###############################################################################
# SPLIT FILE
###############################################################################

split -b "$CHUNK_SIZE" -d -a 6 "$FILE_TO_UPLOAD" "$TMP_DIR/chunk-"
CHUNKS=("$TMP_DIR"/chunk-*)

ACTUAL_TOTAL="${#CHUNKS[@]}"
if [[ "$ACTUAL_TOTAL" -ne "$TOTAL_CHUNKS" ]]; then
  echo -e "${RED}✗${NC} Chunk count mismatch: client=$ACTUAL_TOTAL server=$TOTAL_CHUNKS" >&2
  exit 1
fi

###############################################################################
# UPLOAD CHUNKS
###############################################################################

declare -A RECEIVED
if [[ -n "$STATUS_JSON" ]]; then
  while IFS= read -r n; do
    RECEIVED["$n"]=1
  done < <(jq -r '.receivedChunks[]?' <<<"$STATUS_JSON" 2>/dev/null || true)
fi

upload_chunk() {
  local idx="$1"
  local file="$2"

  if [[ -n "${RECEIVED[$idx]:-}" ]]; then
    echo -e "  ${GREEN}✓${NC} Chunk $idx (skipped)"
    return 0
  fi

  local hash
  hash=$(sha256sum "$file" | awk '{print $1}')

  local resp_file="$TMP_DIR/chunk-$idx.resp.json"

  for ((attempt=1; attempt<=MAX_RETRIES; attempt++)); do
    local http
    http=$(curl -sS \
      --connect-timeout "$CURL_CONNECT_TIMEOUT_S" \
      --max-time "$CURL_MAX_TIME_S" \
      --retry "$CURL_RETRY" \
      --retry-delay "$CURL_RETRY_DELAY_S" \
      --retry-all-errors \
      -o "$resp_file" \
      -w "%{http_code}" \
      -X PUT "$API_BASE/$UPLOAD_ID/chunk/$idx" \
      -H "x-chunk-sha256: $hash" \
      -F "chunk=@$file" || true)

    if [[ "$http" =~ ^2 ]]; then
      echo -e "  ${GREEN}✓${NC} Chunk $idx"
      return 0
    fi

    print_api_error "$resp_file" "$http"
    sleep "$attempt"
  done

  echo -e "  ${RED}✗${NC} Chunk $idx failed" >&2
  return 1
}

echo -e "${BLUE}▶ Uploading $TOTAL_CHUNKS chunks...${NC}"

FAIL=0
for i in "${!CHUNKS[@]}"; do
  upload_chunk "$i" "${CHUNKS[$i]}" &
  [[ $(jobs -r | wc -l) -ge $PARALLEL_JOBS ]] && wait -n || true

done
wait || FAIL=1

if [[ $FAIL -ne 0 ]]; then
  echo -e "${RED}Upload failed.${NC}" >&2
  echo -e "${YELLOW}To resume:${NC} $0 \"$FILE_TO_UPLOAD\" --resume $UPLOAD_ID" >&2
  echo -e "${YELLOW}State saved:${NC} $STATE_FILE" >&2
  exit 1
fi

###############################################################################
# FINALIZE
###############################################################################

echo -e "\n${BLUE}▶ Finalizing upload...${NC}"

FINAL_FILE="$TMP_DIR/final.resp.json"
HTTP=$(curl -sS \
  --connect-timeout "$CURL_CONNECT_TIMEOUT_S" \
  --max-time "$CURL_MAX_TIME_S" \
  --retry "$CURL_RETRY" \
  --retry-delay "$CURL_RETRY_DELAY_S" \
  --retry-all-errors \
  -o "$FINAL_FILE" \
  -w "%{http_code}" \
  -X POST "$API_BASE/$UPLOAD_ID/complete" || true)

if ! [[ "$HTTP" =~ ^2 ]]; then
  echo -e "${RED}✗${NC} Finalization failed" >&2
  print_api_error "$FINAL_FILE" "$HTTP"
  echo -e "${YELLOW}To retry finalize:${NC} curl -X POST $API_BASE/$UPLOAD_ID/complete" >&2
  exit 1
fi

FILE_ID=$(jq -r '.fileId' "$FINAL_FILE")

if [[ -z "$FILE_ID" || "$FILE_ID" == "null" ]]; then
  echo -e "${RED}✗${NC} Finalization failed" >&2
  cat "$FINAL_FILE" >&2
  exit 1
fi

###############################################################################
# SUCCESS
###############################################################################

echo -e "${GREEN}✓ Upload successful${NC}"
echo -e "${YELLOW}File ID:${NC} $FILE_ID"
# Metadata endpoint is fileId-based.
FILES_BASE="${API_BASE%/v1/uploads}"
echo -e "${YELLOW}Metadata:${NC} ${FILES_BASE}/v1/files/$FILE_ID/metadata"
echo -e "${YELLOW}Manifest:${NC} ${FILES_BASE}/v1/files/$FILE_ID/manifest"
echo -e "${YELLOW}Stream:${NC} ${FILES_BASE}/v1/files/$FILE_ID/stream"

# Mark state as completed (best-effort), then remove unless keep-state is enabled.
if [[ -f "$STATE_FILE" ]]; then
  tmp_state="$TMP_DIR/state.completed.json"
  jq --arg fileId "$FILE_ID" '. + {"fileId": $fileId, "completedAt": (now|floor)}' "$STATE_FILE" > "$tmp_state" 2>/dev/null || true
  if [[ -s "$tmp_state" ]]; then
    mv "$tmp_state" "$STATE_FILE" || true
  fi

  if [[ "$KEEP_STATE" -eq 0 && "${FLOE_KEEP_STATE:-0}" != "1" ]]; then
    rm -f "$STATE_FILE" 2>/dev/null || true
  fi
fi
