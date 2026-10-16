#!/bin/bash

set -euo pipefail

# MySQL configuration (modify these according to your environment)
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="123456"
MYSQL_DATABASE="app"

# Number of random records to insert per table
USER_COUNT=5
ORDER_COUNT=5
LOG_COUNT_PER_USER=3     # user_log rows per user
TOKEN_COUNT_PER_USER=2    # user_token rows per user
ITEM_COUNT_PER_ORDER=3   # order_item rows per order

# Function: execute a SQL statement, suppress stderr on success
mysql_exec() {
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D"${MYSQL_DATABASE}" "$@" 2>/dev/null
}

# Function: create the database and all tables defined in config.task.yml
ensure_schema() {
    echo "Creating database '${MYSQL_DATABASE}' if not exists..."
    mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -e \
        "CREATE DATABASE IF NOT EXISTS ${MYSQL_DATABASE} DEFAULT CHARSET=utf8mb4;" 2>/dev/null

    echo "Creating tables..."

    # app.user — main table watched by the first task
    mysql_exec -e "
        CREATE TABLE IF NOT EXISTS user (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            name        VARCHAR(100) NOT NULL,
            email       VARCHAR(255) UNIQUE,
            is_active   BOOLEAN DEFAULT TRUE,
            balance     DECIMAL(10, 2),
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    " && echo "  table 'user' ready" || { echo "Error: failed to create table 'user'" >&2; exit 1; }

    # app.user_log — sub table, ref_column: user_id
    mysql_exec -e "
        CREATE TABLE IF NOT EXISTS user_log (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            user_id     INT NOT NULL,
            action      VARCHAR(50) NOT NULL,
            detail      TEXT,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user_id (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    " && echo "  table 'user_log' ready" || { echo "Error: failed to create table 'user_log'" >&2; exit 1; }

    # app.user_token — sub table, ref_column: user_id
    mysql_exec -e "
        CREATE TABLE IF NOT EXISTS user_token (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            user_id     INT NOT NULL,
            token       VARCHAR(128) NOT NULL,
            expired_at  DATETIME,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_user_id (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    " && echo "  table 'user_token' ready" || { echo "Error: failed to create table 'user_token'" >&2; exit 1; }

    # app.order — main table watched by the second task
    mysql_exec -e "
        CREATE TABLE IF NOT EXISTS \`order\` (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            user_id     INT NOT NULL,
            status      VARCHAR(20) DEFAULT 'pending',
            total       DECIMAL(10, 2) DEFAULT 0,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            INDEX idx_user_id (user_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    " && echo "  table 'order' ready" || { echo "Error: failed to create table 'order'" >&2; exit 1; }

    # app.order_item — sub table, ref_column: order_id
    mysql_exec -e "
        CREATE TABLE IF NOT EXISTS order_item (
            id          INT AUTO_INCREMENT PRIMARY KEY,
            order_id    INT NOT NULL,
            product     VARCHAR(100) NOT NULL,
            quantity    INT DEFAULT 1,
            price       DECIMAL(10, 2) NOT NULL,
            created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_order_id (order_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    " && echo "  table 'order_item' ready" || { echo "Error: failed to create table 'order_item'" >&2; exit 1; }
}

# ---------- Data generation helpers ----------

first_names=(
    "John" "Michael" "David" "James" "Robert" "William" "Richard" "Joseph" "Thomas" "Charles"
    "Mary" "Jennifer" "Linda" "Patricia" "Elizabeth" "Barbara" "Susan" "Jessica" "Sarah" "Emily"
)
last_names=(
    "Smith" "Johnson" "Williams" "Jones" "Brown" "Davis" "Miller" "Wilson" "Moore" "Taylor"
    "Anderson" "Thomas" "Jackson" "White" "Harris" "Martin" "Thompson" "Garcia" "Martinez" "Robinson"
)
domains=("gmail.com" "yahoo.com" "hotmail.com" "outlook.com" "icloud.com")
log_actions=("login" "logout" "update_profile" "change_password" "view_page" "purchase")
products=("Laptop" "Phone" "Tablet" "Monitor" "Keyboard" "Mouse" "Headphone" "Webcam" "Charger" "Cable")
order_statuses=("pending" "paid" "shipped" "delivered" "cancelled")

escape_sql() {
    sed "s/'/''/g"
}

# ---------- Insert functions ----------

insert_user() {
    local first=${first_names[$RANDOM % ${#first_names[@]}]}
    local last=${last_names[$RANDOM % ${#last_names[@]}]}
    local name=$(echo "${first} ${last}" | escape_sql)
    local email=$(echo "${first,,}.${last,,}@${domains[$RANDOM % ${#domains[@]}]}" | escape_sql)
    local is_active=$(( $RANDOM % 10 < 7 ? 1 : 0 ))
    local int_part=$(( RANDOM % 9900 + 100 ))
    local dec_part=$(( RANDOM % 100 ))
    local balance=$(printf "%d.%02d" "${int_part}" "${dec_part}")

    mysql_exec -e "
        INSERT INTO user (name, email, is_active, balance)
        VALUES ('${name}', '${email}', ${is_active}, ${balance});
    " 2>/dev/null && echo "  user: ${name}" || echo "  Error inserting user: ${name}" >&2
}

insert_user_log() {
    local user_id=$1
    local action=${log_actions[$RANDOM % ${#log_actions[@]}]}
    local detail=$(echo "ip=$(printf "%d.%d.%d.%d" $((RANDOM%256)) $((RANDOM%256)) $((RANDOM%256)) $((RANDOM%256))) ua=Mozilla/5.0" | escape_sql)

    mysql_exec -e "
        INSERT INTO user_log (user_id, action, detail)
        VALUES (${user_id}, '${action}', '${detail}');
    " 2>/dev/null && echo "    user_log: user_id=${user_id} action=${action}" || echo "    Error inserting user_log" >&2
}

insert_user_token() {
    local user_id=$1
    local token=$(openssl rand -hex 32)
    local expired_at=$(date -u -v+30d "+%Y-%m-%d %H:%M:%S" 2>/dev/null || date -u -d "+30 days" "+%Y-%m-%d %H:%M:%S")

    mysql_exec -e "
        INSERT INTO user_token (user_id, token, expired_at)
        VALUES (${user_id}, '${token}', '${expired_at}');
    " 2>/dev/null && echo "    user_token: user_id=${user_id}" || echo "    Error inserting user_token" >&2
}

insert_order() {
    local user_id=$1
    local status=${order_statuses[$RANDOM % ${#order_statuses[@]}]}
    local int_part=$(( RANDOM % 9900 + 100 ))
    local dec_part=$(( RANDOM % 100 ))
    local total=$(printf "%d.%02d" "${int_part}" "${dec_part}")

    mysql_exec -e "
        INSERT INTO \`order\` (user_id, status, total)
        VALUES (${user_id}, '${status}', ${total});
    " 2>/dev/null && echo "  order: user_id=${user_id} status=${status} total=${total}" || echo "  Error inserting order" >&2
}

insert_order_item() {
    local order_id=$1
    local product=${products[$RANDOM % ${#products[@]}]}
    local qty=$(( RANDOM % 5 + 1 ))
    local int_part=$(( RANDOM % 900 + 10 ))
    local dec_part=$(( RANDOM % 100 ))
    local price=$(printf "%d.%02d" "${int_part}" "${dec_part}")

    mysql_exec -e "
        INSERT INTO order_item (order_id, product, quantity, price)
        VALUES (${order_id}, '${product}', ${qty}, ${price});
    " 2>/dev/null && echo "    order_item: order_id=${order_id} product=${product} qty=${qty} price=${price}" || echo "    Error inserting order_item" >&2
}

# ---------- Main ----------

echo "Starting MySQL data setup for config.task.yml schema..."
echo "Database: ${MYSQL_DATABASE}"
echo ""

ensure_schema
echo ""

# Insert users
echo "Inserting ${USER_COUNT} users..."
for ((i = 0; i < USER_COUNT; i++)); do
    insert_user
done
echo ""

# For each user, insert logs and tokens
echo "Inserting user_log and user_token rows..."
last_user_id=$(mysql_exec -Nse "SELECT MAX(id) FROM user;" 2>/dev/null)
for ((uid = last_user_id - USER_COUNT + 1; uid <= last_user_id; uid++)); do
    echo "  user_id=${uid}:"
    for ((j = 0; j < LOG_COUNT_PER_USER; j++)); do
        insert_user_log "${uid}"
    done
    for ((j = 0; j < TOKEN_COUNT_PER_USER; j++)); do
        insert_user_token "${uid}"
    done
done
echo ""

# Insert orders (randomly assigned to users)
echo "Inserting ${ORDER_COUNT} orders..."
for ((i = 0; i < ORDER_COUNT; i++)); do
    random_user_id=$(( RANDOM % last_user_id + 1 ))
    insert_order "${random_user_id}"
done
echo ""

# For each order, insert items
echo "Inserting order_item rows..."
last_order_id=$(mysql_exec -Nse "SELECT MAX(id) FROM \`order\`;" 2>/dev/null)
for ((oid = last_order_id - ORDER_COUNT + 1; oid <= last_order_id; oid++)); do
    echo "  order_id=${oid}:"
    for ((j = 0; j < ITEM_COUNT_PER_ORDER; j++)); do
        insert_order_item "${oid}"
    done
done
echo ""

echo "Operation completed."
echo ""
echo "Tables created (matching config.task.yml):"
echo "  app.user          (main table, primary_key: id)"
echo "  app.user_log      (sub table,  ref_column: user_id)"
echo "  app.user_token    (sub table,  ref_column: user_id)"
echo "  app.order         (main table, primary_key: id)"
echo "  app.order_item    (sub table,  ref_column: order_id)"
