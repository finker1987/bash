#!/bin/bash

# replica control (not start script on replica)
is_repl=$(psql -U postgres -p 5432 -t -c 'select pg_is_in_recovery()' | tr -d " ")
if [[ "$is_repl" == "f" ]]; then
    echo "this is not a replica."
else
    echo "this is a replica, exit."
    exit
fi

# dublicate run control
PID="/tmp/$0.pid" # Temp file
if [ ! -f "$PID" ]; then
    echo $$ >"$PID" # Print actual PID into a file
else
    ps -p "$(cat "$PID")" >/dev/null && exit || echo $$ >"$PID"
fi

# variables
time_start=$(date +"%Y-%m-%d_%H%M%S")
time_start_metric=$(date -u -d $(date +'%H:%M:%S') +"%s")
backup_type="logical_format_directory_s3"
metrics_file=/opt/node_exporter/textfile/postgresql_backup_status_metrics.prom
echo "" > "$metrics_file"
postgres_port="5432"

# directory
dir_main="/store/backup/postgresql/pg_dump/$backup_type"
dir_listing="$dir_main/$time_start/listing"
dir_dump="$dir_main/$time_start/dump"
dir_schema="$dir_main/$time_start/schema"
dir_config="$dir_main/$time_start/config"
dir_ddl="$dir_main/$time_start/ddl"
error_file="$dir_main/$time_start/error.log"
log_file="$dir_main/$time_start/backup.log"
mkdir -p "$dir_main" "$dir_dump" "$dir_schema" "$dir_listing" "$dir_config" "$dir_ddl"
touch "$error_file"

# Logging backup postgresql started
echo -e "\n$(date +"%Y-%m-%d %H:%M:%S %:z") starting logical postgresql cluster backup" >> "$log_file"

# remove old backups
cd $dir_main
dirs=($(ls -dtr */))
num_dirs=${#dirs[@]}
num_to_delete=$(($num_dirs - 3))
# Loop through the list of directories, starting from the oldest
echo "$(date +"%Y-%m-%d %H:%M:%S %:z") removing old backups" >> "$log_file"
for ((i = 0; i < $num_dirs; i++)); do
    if (($i < $num_to_delete)); then
        # Delete the directory
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") deleting ${dirs[$i]}" >> "$log_file"
        rm -rf "${dirs[$i]}"
    else
        # Keep the directory
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") keeping ${dirs[$i]}" >> "$log_file"
    fi
done

# Creating schema dump
pg_dumpall -U postgres -p 5432 --schema-only >"$dir_schema"/schema.sql 2>> "$log_file"

    # check backup schema status: 0-error, 1-ok
    if [ $? -eq 0 ]; then
        pg_backup_schema_status="1"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") schema dump done" >> "$log_file"
        echo "pg_backup_schema_status{start=\"$time_start\"}" "$pg_backup_schema_status" >> "$metrics_file"
    else
        pg_backup_schema_status="0"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") schema dump error" >> "$log_file"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") schema dump error" >> "$error_file"
        echo "pg_backup_schema_status{start=\"$time_start\"}" "$pg_backup_schema_status" >> "$metrics_file"
    fi

# backup configs
datadir=$(psql -U postgres -p 5432 -t -c "select setting from pg_settings where name='data_directory';")
cd $datadir
cp -v pg_hba.conf postgresql.conf postgresql.auto.conf "$dir_config"

# check backup config status: 0-error, 1-ok
if [ $? -eq 0 ]; then
        pg_backup_config_status="1"
        echo "$(date +"%Y-%m-%d_%H:%M:%S %:z") backup config files done" >>"$log_file"
        # node_exporter custom metrics
        echo "pg_backup_config_status{start=\"$time_start\"}" "$pg_backup_config_status" >> "$metrics_file"
    else
        pg_backup_config_status="0"
        echo "$(date +"%Y-%m-%d_%H:%M:%S %:z") backup config files error" >>"$log_file"
        echo "$(date +"%Y-%m-%d_%H:%M:%S %:z") backup config files error" >> "$error_file"
        # node_exporter custom metrics
        echo "pg_backup_config_status{start=\"$time_start\"}" "$pg_backup_config_status" >> "$metrics_file"
    fi

# list database name
db_array=($(psql -U postgres -p 5432 -t -c "select datname from pg_database where datname not in ('postgres','template0','template1','test');"))

# creating database dump
for dbname in "${db_array[@]}"; do
    backup_path="$dir_dump/$dbname"
    mkdir -p "$backup_path"
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup database $dbname for $(hostname) started" >> "$log_file"
    time_start_db=$(date +"%Y-%m-%d_%H%M%S")
    pg_dump -d "$dbname" -U postgres -p 5432 -Fd -f "$backup_path"/"$dbname".dmp -j 10 -Z 6 2>> "$log_file"

    # check pg_dump status: 0-error, 1-ok
    if [ $? -eq 0 ]; then
        pg_dump_status="1"
        # node_exporter custom metrics
        echo "pg_backup_database_status{start=\"$time_start_db\",db_name=\"$dbname\",type=\"pg_dump\"}" "$pg_dump_status" >> "$metrics_file"
    else
        pg_dump_status="0"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup database $dbname for $(hostname) error" >> "$log_file"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup database $dbname for $(hostname) error" >> "$error_file"
        # node_exporter custom metrics
        echo "pg_backup_database_status{start=\"$time_start_db\",db_name=\"$dbname\",type=\"pg_dump\"}" "$pg_dump_status" >> "$metrics_file"
    fi

   # Creating file with description database tables
    psql -U postgres -p 5432 -t -d "$dbname" -c "select
        schema_name,
        relname,
        pg_size_pretty(table_size) as size
    from
        (
        select
                pg_catalog.pg_namespace.nspname as schema_name,
                relname,
                pg_relation_size(pg_catalog.pg_class.oid) as table_size
        from
                pg_catalog.pg_class
        join pg_catalog.pg_namespace on
                relnamespace = pg_catalog.pg_namespace.oid
    ) t
    where
        (schema_name not like 'pg_%') and (schema_name <> 'information_schema')
    order by
        table_size desc;" >"$dir_listing/$dbname.txt"


    echo 2>>"$log_file"
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") logical backup database $dbname for $(hostname) finished" >> "$log_file"
done

# creating backup table ddl
for dbname in "${db_array[@]}"; do
        mkdir -p "$dir_ddl"/"$dbname"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup ddl database $dbname for $(hostname) started" >> "$log_file"
        pg_dump -cs -p 5432 -U postgres -d "$dbname" -f "$dir_ddl"/"$dbname"/"$dbname".schema.sql

        if [ $? -eq 0 ]; then
        pg_dump_ddl_schema_status="1"
        else
        pg_dump_ddl_schema_status="0"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup ddl $dbname $schema for $(hostname) error" >> "$log_file"
        # echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup ddl $dbname $schema for $(hostname) error" >> "$error_file"
        fi

    for schema in $(psql -p 5432 -AtU postgres -d "$dbname" -c "\dnS"| awk -F\| '{ print $1}'| grep -v temp | grep -v pg_catalog |grep -v information_schema | grep -v pg_toast); do
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") Schema $schema is backup $dbname for $(hostname) started" >> "$log_file"
        for table in $(psql -p 5432 -AtU postgres -d "$dbname" -c "\dt $schema.*" | awk -F\| '{ print $2}'); do
            echo "$(date +"%Y-%m-%d %H:%M:%S %:z") Table $schema.$table backup done" >> "$log_file"
        pg_dump -p 5432 -U postgres -d "$dbname" -sc -t "$schema"."$table" -f "$dir_ddl"/"$dbname"/"$schema"."$table".sql

    # check pg_dump_ddl status: 0-error, 1-ok
    if [ $? -eq 0 ]; then
        pg_dump_ddl_status="1"
    else
        pg_dump_ddl_status="0"
        echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup ddl $dbname $schema.$table for $(hostname) error" >> "$log_file"
        # echo "$(date +"%Y-%m-%d %H:%M:%S %:z") backup ddl $dbname $schema.$table for $(hostname) error" >> "$error_file"
    fi
        done
    done
done

# transfer backup to S3
time_start_s3=$(date +"%Y-%m-%d_%H%M%S")
echo "$(date +"%Y-%m-%d %H:%M:%S %:z") send backup to s3 - start" >>"$log_file"
s3Backet=$(hostname)
s3ConfPath=/etc/s3cmd/s3cmd.conf
S3_PATH="s3://$s3Backet/$backup_type"
Full_backup_dir="$dir_main/$time_start"
retention=3
retention_s3=$(expr $retention - 1 )
retention_date="$(date --date "$retention_s3 days ago" "+%Y-%m-%d")"
s3cmd -c $s3ConfPath put "$Full_backup_dir" "$S3_PATH"/ --recursive >>"$log_file"

# check backup transfer status to s3
if [ $? -eq 0 ]; then
    s3_transfer_status="1"
    s3_transfer_status_summary="ENABLED"
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") send backup to s3 - done" >> "$log_file"
    # node_exporter custom metrics
    echo "pg_backup_s3_status{start=\"$time_start_s3\"}" "$s3_transfer_status" >> "$metrics_file"
else
    s3_transfer_status="0"
    s3_transfer_status_summary="ERROR"
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") send backup to s3 - error" >> "$log_file"
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") send backup to s3 - error" >> "$error_file"
    # node_exporter custom metrics
    echo "pg_backup_s3_status{start=\"$time_start_s3\"}" "$s3_transfer_status" >> "$metrics_file"
fi

# delete old backup from S3
FILES=$(s3cmd -c $s3ConfPath ls "${S3_PATH}/" | awk '{print $2}' | tr -d " " | awk -F'/' '{print $5}')
for file in $FILES; do
    echo $file
    FILE_DATE=$(echo "$file" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}')
    if [[ "$FILE_DATE" < "$retention_date" ]]; then
        s3cmd -c "$s3ConfPath" del "$S3_PATH"/"$file" --recursive >>"$log_file"
    fi
done

# check backup job status: 0-error, 1-ok
if [ -s "$error_file" ]; then
    status_job=0
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") error logical backup postgresql cluster" >> "$log_file"
else
    status_job=1
    echo "$(date +"%Y-%m-%d %H:%M:%S %:z") logical backup postgresql cluster successfully" >> "$log_file"
fi

# prepare variables for node_exporter custom metrics
dump_size=$(du -hd0 -b "$dir_dump" | awk '{print $1}')
time_done=$(date +"%Y-%m-%d_%H%M%S")
time_done_metric=$(date -u -d $(date +'%H:%M:%S') +"%s")
time_diff_metrics=$(date -u -d "0 $time_done_metric sec - $time_start_metric sec" +"%H:%M:%S")
num_local_metrics=$(ls "$dir_main" -l | grep '^d' | wc -l)
num_s3_metrics=$(s3cmd -c $s3ConfPath ls "${S3_PATH}/" | wc -l)

# node_exporter custom backups metrics: 0-error, 1-ok
echo "pg_backup_summary_info{start=\"$time_start\",done=\"$time_done\",s3=\"$s3_transfer_status_summary\",time_diff=\"$time_diff_metrics\",size=\"$dump_size\",num_local=\"$num_local_metrics\",num_s3=\"$num_s3_metrics\",type=\"$backup_type\",tool=\"pg_dump\"}" "$status_job" >> "$metrics_file"
