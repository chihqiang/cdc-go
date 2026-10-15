#!/bin/bash

set -euo pipefail

# MySQL configuration (modify these according to your environment)
MYSQL_HOST="localhost"
MYSQL_PORT="3306"
MYSQL_USER="root"
MYSQL_PASSWORD="123456"
MYSQL_DATABASE="cdctrigger"
TABLE_NAME="test"
INSERT_COUNT=5  # Number of random records to insert

# Function to check and create table if not exists
ensure_table_exists() {
    echo "Checking if table '${TABLE_NAME}' exists in database '${MYSQL_DATABASE}'..."

    # Check table existence
    local table_exists
    table_exists=$(mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D"${MYSQL_DATABASE}" -Nse \
        "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema='${MYSQL_DATABASE}' AND table_name='${TABLE_NAME}')" 2>/dev/null)

    if [ "${table_exists}" -eq 0 ]; then
        echo "Table '${TABLE_NAME}' does not exist. Creating..."
        # Create table with specified structure
        if mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D"${MYSQL_DATABASE}" -e "
            CREATE TABLE ${TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                age TINYINT UNSIGNED,
                email VARCHAR(255) UNIQUE,
                is_active BOOLEAN DEFAULT TRUE,
                balance DECIMAL(10, 2),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                profile TEXT,
                data JSON,
                file_hash CHAR(32),
                quantity BIGINT
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        " 2>/dev/null; then
            echo "Table '${TABLE_NAME}' created successfully"
        else
            echo "Error: Failed to create table '${TABLE_NAME}'" >&2
            exit 1
        fi
    else
        echo "Table '${TABLE_NAME}' already exists"
    fi
}

# Function to generate and insert one random record
insert_random_record() {
    # Name lists
    local first_names=(
        "John" "Michael" "David" "James" "Robert" "William" "Richard" "Joseph" "Thomas" "Charles"
        "Christopher" "Daniel" "Matthew" "Anthony" "Donald" "Mark" "Paul" "Steven" "Andrew" "Kenneth"
        "Mary" "Jennifer" "Linda" "Patricia" "Elizabeth" "Barbara" "Susan" "Jessica" "Sarah" "Emily"
    )
    local last_names=(
        "Smith" "Johnson" "Williams" "Jones" "Brown" "Davis" "Miller" "Wilson" "Moore" "Taylor"
        "Anderson" "Thomas" "Jackson" "White" "Harris" "Martin" "Thompson" "Garcia" "Martinez" "Robinson"
    )

    # Generate random data with special character handling
    local first=${first_names[$RANDOM % ${#first_names[@]}]}
    local last=${last_names[$RANDOM % ${#last_names[@]}]}
    local name=$(echo "${first} ${last}" | sed "s/'/''/g")  # Escape single quotes for MySQL

    local age=$(( RANDOM % 48 + 18 ))  # 18-65 years
    local domains=("gmail.com" "yahoo.com" "hotmail.com" "outlook.com" "icloud.com")
    local email=$(echo "${first,,}.${last,,}@${domains[$RANDOM % ${#domains[@]}]}" | sed "s/'/''/g")

    local is_active=$(( $RANDOM % 10 < 7 ? 1 : 0 ))  # 70% active probability
    # 替换 bc：用 RANDOM 生成整数后格式化为两位小数（100.00 - 9999.99）
    local int_part=$(( RANDOM % 9900 + 100 ))
    local dec_part=$(( RANDOM % 100 ))
    local balance=$(printf "%d.%02d" "${int_part}" "${dec_part}")

    local jobs=(
        "Software Engineer" "Data Analyst" "Project Manager" "UX Designer" "Marketing Specialist"
        "HR Manager" "Financial Advisor" "Mechanical Engineer" "Teacher" "Doctor"
    )
    local profile=${jobs[$RANDOM % ${#jobs[@]}]}

    local hobbies=(
        "reading" "hiking" "gaming" "cooking" "traveling" "photography" "painting" "yoga"
        "cycling" "running" "coding" "dancing" "singing" "gardening"
    )
    local h1=${hobbies[$RANDOM % ${#hobbies[@]}]}
    local h2=${hobbies[$RANDOM % ${#hobbies[@]}]}
    local favorite_color=$(printf "#%06x" $((RANDOM % 16777215)))
    local data=$(echo "{\"hobbies\": [\"${h1}\", \"${h2}\"], \"favorite_color\": \"${favorite_color}\"}" | sed "s/'/''/g")

    local file_hash=$(openssl rand -hex 16)
    local quantity=$(( RANDOM % 5000 + 1 ))

    # Execute insertion
    if mysql -h"${MYSQL_HOST}" -P"${MYSQL_PORT}" -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" -D"${MYSQL_DATABASE}" -e "
        INSERT INTO ${TABLE_NAME} (
            name, age, email, is_active, balance, profile, data, file_hash, quantity
        ) VALUES (
            '${name}', ${age}, '${email}', ${is_active}, ${balance}, '${profile}', '${data}', '${file_hash}', ${quantity}
        );
    " 2>/dev/null; then
        echo "Inserted: ${name}"
    else
        echo "Error inserting record: ${name}" >&2
    fi
}

# Main execution flow
echo "Starting MySQL data setup..."
ensure_table_exists

echo "Inserting ${INSERT_COUNT} random records..."
for ((i=0; i<INSERT_COUNT; i++)); do
    insert_random_record
done

echo "Operation completed"