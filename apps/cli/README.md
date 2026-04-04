# Floe CLI

`@floehq/cli` is the official command-line interface for the Floe API.

## Install

```bash
npm install -g @floehq/cli
```

Or run without a global install:

```bash
npx @floehq/cli --help
```

## Commands

```bash
floe upload ./video.mp4
floe upload status <uploadId>
floe upload cancel <uploadId>
floe upload complete <uploadId>
floe upload wait <uploadId>
floe file metadata <fileId>
floe file manifest <fileId>
floe file stream-url <fileId>
floe ops health
floe config show
floe config path
floe config set base-url https://api.floehq.com/v1
floe config unset api-key
```

Shortcuts are also supported for the most common lookups:

```bash
floe status <uploadId>
floe cancel <uploadId>
floe metadata <fileId>
floe manifest <fileId>
floe stream-url <fileId>
```

## Config

The CLI can persist defaults in a local config file. Precedence is:

```txt
flags > environment variables > config file > defaults
```

Supported persistent keys:

```txt
base-url
api-key
bearer
owner-address
auth-user
wallet-address
```

Examples:

```bash
floe config path
floe config set base-url https://api.floehq.com/v1
floe config set api-key sk_live_xxx
floe config unset api-key
```
