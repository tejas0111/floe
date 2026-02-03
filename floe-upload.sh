#!/bin/bash
set -euo pipefail

###############################################################################
# Floe v1 - Walrus Upload Utility
###############################################################################

FILE_TO_UPLOAD="${1:-}"
CUSTOM_CHUNK_SIZE_MB=""
EPOCHS=1
PARALLEL_JOBS=1
API_BASE="http://localhost:3001/v1/uploads"
MAX_RETRIES=3

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
  echo "  -c, --chunk <mb>    Chunk size in MB (0.25 - 10)"
  echo "  -e, --epochs <num>  Number of epochs (default: 1)"
  echo "  -p, --parallel <n>  Parallel uploads (default: 1)"
  echo "  -h, --help          Show help"
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
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
  shift
done

[[ ! -f "$FILE_TO_UPLOAD" ]] && echo "File not found" && exit 1

FILE_SIZE=$(stat -c%s "$FILE_TO_UPLOAD" 2>/dev/null || stat -f%z "$FILE_TO_UPLOAD")
CONTENT_TYPE=$(file --mime-type -b "$FILE_TO_UPLOAD" 2>/dev/null || echo "application/octet-stream")

echo -e "${CYAN}╔════════════════════════════════════════════╗${NC}"
echo -e "${CYAN}║              FLOE V1 UPLOADER                 ║${NC}"
echo -e "${CYAN}╚════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}File:${NC} $(basename "$FILE_TO_UPLOAD")"
echo -e "${YELLOW}Size:${NC} $(numfmt --to=iec-i "$FILE_SIZE")"
echo -e "${YELLOW}Type:${NC} $CONTENT_TYPE"
echo -e "${YELLOW}Epochs:${NC} $EPOCHS"
echo -e "${YELLOW}Parallel:${NC} $PARALLEL_JOBS"

echo -e "\n${BLUE}▶ Creating upload session...${NC}"

JSON=$(cat <<EOF
{
  "filename": "$(basename "$FILE_TO_UPLOAD")",
  "contentType": "$CONTENT_TYPE",
  "sizeBytes": $FILE_SIZE,
  "epochs": $EPOCHS
  $( [ -n "$CUSTOM_CHUNK_SIZE_MB" ] && echo ",\"chunkSize\": $((CUSTOM_CHUNK_SIZE_MB * 1024 * 1024))" )
}
EOF
)

RESP=$(curl -fsS -X POST "$API_BASE/create" -H "Content-Type: application/json" -d "$JSON")
UPLOAD_ID=$(jq -r '.uploadId' <<<"$RESP")
CHUNK_SIZE=$(jq -r '.chunkSize' <<<"$RESP")
TOTAL_CHUNKS=$(jq -r '.totalChunks' <<<"$RESP")

[[ -z "$UPLOAD_ID" || "$UPLOAD_ID" == "null" ]] && echo "Session creation failed" && exit 1
echo -e "${GREEN}✓ Upload ID: $UPLOAD_ID${NC}"

TMP_DIR="./floe_tmp_$UPLOAD_ID"
mkdir -p "$TMP_DIR"
split -b "$CHUNK_SIZE" -d -a 4 "$FILE_TO_UPLOAD" "$TMP_DIR/chunk-"
CHUNKS=("$TMP_DIR"/chunk-*)

[[ "${#CHUNKS[@]}" -ne "$TOTAL_CHUNKS" ]] && echo "Chunk count mismatch" && exit 1

upload_chunk() {
  local idx="$1"
  local file="$2"
  local hash
  hash=$(sha256sum "$file" | awk '{print $1}')

  for ((attempt=1; attempt<=MAX_RETRIES; attempt++)); do
    if curl -fsS -X PUT \
      "$API_BASE/$UPLOAD_ID/chunk/$idx" \
      -H "x-chunk-sha256: $hash" \
      -F "chunk=@$file" >/dev/null; then
      echo -e "  ${GREEN}✓${NC} Chunk $idx"
      return 0
    fi
    sleep $attempt
  done

  echo -e "  ${RED}✗${NC} Chunk $idx failed after retries"
  return 1
}

echo -e "${BLUE}▶ Uploading $TOTAL_CHUNKS chunks...${NC}"

FAIL=0
for i in "${!CHUNKS[@]}"; do
  upload_chunk "$i" "${CHUNKS[$i]}" &
  [[ $(jobs -r | wc -l) -ge $PARALLEL_JOBS ]] && wait -n || true
done
wait || FAIL=1

[[ $FAIL -ne 0 ]] && echo "Upload failed" && exit 1

echo -e "\n${BLUE}▶ Finalizing upload...${NC}"
FINAL=$(curl -fsS -X POST "$API_BASE/$UPLOAD_ID/complete")
BLOB_ID=$(jq -r '.blobId' <<<"$FINAL")

[[ -z "$BLOB_ID" || "$BLOB_ID" == "null" ]] && echo "Finalization failed" && exit 1

echo -e "${GREEN}✓ Upload successful${NC}"
echo -e "${YELLOW}Blob ID:${NC} $BLOB_ID"
echo -e "${YELLOW}URL:${NC} https://aggregator.walrus.space/v1/blobs/$BLOB_ID"

rm -rf "$TMP_DIR"

