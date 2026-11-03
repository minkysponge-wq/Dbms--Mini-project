# Setup Guide

Written for Windows, since that is where this will be marked. macOS and
Linux notes are at the end.

---

## 1. Prerequisites

| Software | Version | Notes |
|---|---|---|
| Python | 3.10+ | `python --version` |
| MySQL **or** MariaDB | MySQL 8.0+ / MariaDB 10.4+ | XAMPP ships MariaDB; both work |
| Node.js | 18+ | For the dashboard |
| MongoDB | 6.0+ | For the event log and vector search |

### MySQL on Windows

**Easiest — XAMPP.** Download from apachefriends.org, install, open the
XAMPP Control Panel and press **Start** next to MySQL. That gives you
MariaDB on port 3306 with user `root` and an empty password, which is
what this project defaults to.

> XAMPP ships **MariaDB**, not MySQL. Everything here was tested on both.
> One MariaDB quirk is already worked around in the schema: it rejects
> `CHECK` constraints on foreign-key columns that cascade, so that one
> rule lives in a trigger instead.

**Or MySQL Community Server.** Download the MSI installer, choose
"Developer Default", and set a root password. Then tell the project about
it — see step 3.

### MongoDB on Windows

Download MongoDB Community Server (MSI) from mongodb.com, run the
installer, and tick **"Install MongoDB as a Service"** so it starts with
Windows. Install **MongoDB Compass** alongside it when offered — it gives
you a GUI for screenshots of the document collection.

Default URI: `mongodb://127.0.0.1:27017`, no authentication.

---

## 2. Install the Python packages

```bash
pip install -r requirements.txt
```

If `pip` is not recognised, use `python -m pip install -r requirements.txt`.

---

## 3. Configure the connection (only if your setup is non-default)

Defaults: MySQL on `127.0.0.1:3306` as `root` with no password, MongoDB
on `127.0.0.1:27017`. If that matches your machine, skip this step.

Otherwise set environment variables before running anything. In
Command Prompt:

```cmd
set RAFTKV_DB_USER=root
set RAFTKV_DB_PASSWORD=yourpassword
```

In PowerShell:

```powershell
$env:RAFTKV_DB_USER = "root"
$env:RAFTKV_DB_PASSWORD = "yourpassword"
```

Full list: `RAFTKV_DB_HOST`, `RAFTKV_DB_PORT`, `RAFTKV_DB_USER`,
`RAFTKV_DB_PASSWORD`, `RAFTKV_DB_NAME`, `RAFTKV_MONGO_URI`,
`RAFTKV_MONGO_DB`.

The dashboard reads the same names from `dashboard/.env.local`. Copy
`dashboard/.env.local.example` to `dashboard/.env.local` and edit it.

---

## 4. Create the database

```bash
python setup.py
```

Expected output:

```
MySQL
  connected to MySQL at 127.0.0.1:3306
  01_schema.sql        tables, keys and constraints (28 statements)
  02_views.sql         views, CTEs and window functions (15 statements)
  03_triggers.sql      triggers (13 statements)
  04_procedures.sql    stored procedures and functions (21 statements)
  05_seed.sql          cluster nodes, roles, permissions and users (29 statements)
  created 17 tables, 7 views, 6 triggers, 10 procedures and functions

MongoDB
  connected to MongoDB at mongodb://127.0.0.1:27017
  collection 'cluster_events' ready with 5 indexes

Everything is ready.
```

`python setup.py --check` reports status without changing anything.

> **This drops and recreates the `raftkv` database.** It is safe to run
> repeatedly; it is not safe to point at a database you care about.

---

## 5. Verify

```bash
python -m unittest discover -s tests
```

30 tests, roughly 75 seconds. All should pass. The suite builds real
five-node clusters against your MySQL, so it exercises the triggers and
constraints as well as the Python.

---

## 6. Run it

**Terminal 1 — the cluster:**

```bash
python server.py
```

```
events: connected to MongoDB at raftkv_events
starting 5-node cluster, waiting for a leader...
leader: node 2
control API listening on http://127.0.0.1:8080
```

**Terminal 2 — the dashboard:**

```bash
cd dashboard
npm install        # first time only
npm run dev
```

Open http://localhost:3000.

| Username | Password | Can do |
|---|---|---|
| `admin` | `admin123` | everything, including crashing nodes |
| `operator` | `operator123` | read and write keys |
| `viewer` | `viewer123` | read only |

> These are development credentials. Change them before putting this
> anywhere real.

---

## 7. The demo worth showing

1. Sign in as **admin**.
2. **Key-value store** tab → write a few keys.
3. **Cluster** tab → find the node badged `leader` → press **Crash**.
4. Watch a new leader get elected within a few seconds.
5. Back to **Key-value store** → the keys are all still there.
6. **Event log** tab → search `which node crashed` in vector mode.

Then sign out, sign in as **viewer**, and try to write a key — the form is
disabled and the API returns 403.

For the database concepts:

```bash
python demos.py
```

---

## Troubleshooting

**`cannot reach MySQL at 127.0.0.1:3306`**
MySQL is not running. In XAMPP, press Start next to MySQL. Check nothing
else holds port 3306.

**`Access denied for user 'root'@'localhost'`**
Your root password is not empty. Set `RAFTKV_DB_PASSWORD` (step 3).

**`WARNING: cannot reach MongoDB`**
Everything except the event log still works. Start the MongoDB service:
press Win+R, type `services.msc`, find MongoDB Server, press Start.

**`ModuleNotFoundError: No module named 'pymysql'`**
`pip install -r requirements.txt` ran against a different Python. Use
`python -m pip install -r requirements.txt`.

**Dashboard shows "cluster process is not responding"**
`server.py` is not running, or is on a different port. It must be up
before the dashboard can crash nodes or write keys; reads from MySQL work
regardless.

**Tests fail with "no leader elected"**
Usually a slow or loaded MySQL. Re-run. If it persists, raise
`ELECTION_TIMEOUT_MIN` and `ELECTION_TIMEOUT_MAX` in `raft/node.py` —
every state change is a SQL round trip, so a slow server needs longer
timeouts.

**Port 3000 or 8080 already in use**
`python server.py --port 9090` and set `RAFTKV_CLUSTER_API` to match; or
`npm run dev -- -p 3001`.

---

## macOS and Linux

```bash
# macOS
brew install mysql mongodb-community node
brew services start mysql
brew services start mongodb-community

# Debian / Ubuntu
sudo apt install mariadb-server nodejs npm
sudo systemctl start mariadb
```

Then follow from step 2. If MySQL uses a socket rather than TCP, set
`RAFTKV_DB_SOCKET=/var/run/mysqld/mysqld.sock`.
