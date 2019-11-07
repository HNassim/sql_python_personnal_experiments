#!/bin/bash

source /app/cfvr/presta03/.bashrc

args=("$@")
echo "Nombre d'argument : " $#
echo "Sas user name : "  ${args[0]}
echo "Sas password : " ${args[1]}
echo "PostgreSQL password : "  ${args[2]}
echo "SAS table directory path : " ${args[3]}
echo "SAS table name : "  ${args[4]}
echo "Log directory path : " ${args[5]}

if [ $# = 6 ]; then
	echo "No columns argument. All table's columns are used by default."
	execSAS sas_to_sql_all.sas sas_user_name:${args[0]} sas_pwd:${args[1]} psql_to_schema:${args[2]} sas_table_dir_path:${args[3]} sas_table:${args[4]} log_dir:${args[5]}
fi

if [ $# = 7 ]; then
	echo "Columns name to transfer : " ${args[6]}
	execSAS sas_to_sql.sas sas_user_name:${args[0]} sas_pwd:${args[1]} psql_to_schema:${args[2]} sas_table_dir_path:${args[3]} sas_table:${args[4]} log_dir:${args[5]} columns:${args[6]}
fi
