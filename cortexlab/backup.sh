#!/bin/bash
set -euo pipefail

ENV_FILE="/home/ubuntu/Documents/PYTHON/iblsre/alyx/containers/deploy-web/.env"

if [[ -f "${ENV_FILE}" ]]; then
	set -a
	source "${ENV_FILE}"
	set +a
fi

BACKUPS_S3_URI="${BACKUPS_S3_URI:-s3://alyx-cortexlab/backups}"
DB_HOST="${DB_HOST:-${POSTGRES_HOST:-}}"
DB_PORT="${DB_PORT:-${POSTGRES_PORT:-5432}}"
DB_NAME="${DB_NAME:-${POSTGRES_DB:-alyx}}"
DB_USER="${DB_USER:-${POSTGRES_USER:-ibl_dev}}"
DB_PASSWORD="${DB_PASSWORD:-${POSTGRES_PASSWORD:-}}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-alyx_postgres}"  # pg_dump installed in this container
AWS_CONTAINER="${AWS_CONTAINER:-alyx_apache}"  # aws-cli installed in this container

TODAY="$(date -u +%F)"
DAY_OF_WEEK="$(date -u +%u)"
DAY_OF_MONTH="$(date -u +%d)"

WEEKLY_PREFIX="${BACKUPS_S3_URI%/}/weekly/"
MONTHLY_PREFIX="${BACKUPS_S3_URI%/}/monthly/"

weekly_cutoff="$(date -u -d '365 days ago' +%F)"
monthly_cutoff="$(date -u -d '730 days ago' +%F)"

echo "backup.sh start: date=${TODAY} dow=${DAY_OF_WEEK} dom=${DAY_OF_MONTH}"

docker_exec_args=(-i)
if [[ -n "${DB_PASSWORD:-}" ]]; then
	docker_exec_args+=(-e "PGPASSWORD=${DB_PASSWORD}")
fi

create_dump() {
	local cadence="$1"
	local prefix="$2"
	local final_key="${prefix}alyx_${cadence}_${TODAY}.sql.gz"
	local part_key="${final_key}.part"

	echo "Creating ${cadence} dump for ${TODAY}"
	docker exec "${docker_exec_args[@]}" "${POSTGRES_CONTAINER}" \
		pg_dump -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" \
		| gzip -1 \
		| docker exec -i "${AWS_CONTAINER}" aws s3 cp - "${part_key}" --only-show-errors

	docker exec "${AWS_CONTAINER}" aws s3 mv "${part_key}" "${final_key}" --only-show-errors
	echo "Uploaded ${final_key}"
}

copy_weekly_to_monthly() {
	local weekly_key="${WEEKLY_PREFIX}alyx_weekly_${TODAY}.sql.gz"
	local monthly_key="${MONTHLY_PREFIX}alyx_monthly_${TODAY}.sql.gz"

	echo "Copying weekly dump to monthly for ${TODAY}"
	docker exec "${AWS_CONTAINER}" aws s3 cp "${weekly_key}" "${monthly_key}" --only-show-errors
	echo "Copied ${weekly_key} to ${monthly_key}"
}

prune_prefix_by_date() {
	local prefix="$1"
	local cutoff="$2"

	docker exec "${AWS_CONTAINER}" aws s3 ls "${prefix}" | while read -r _ _ _ key; do
		[[ -z "${key:-}" ]] && continue

		if [[ "${key}" =~ ([0-9]{4}-[0-9]{2}-[0-9]{2}) ]]; then
			file_date="${BASH_REMATCH[1]}"
			if [[ "${file_date}" < "${cutoff}" ]]; then
				echo "Pruning ${prefix}${key} (file_date=${file_date}, cutoff=${cutoff})"
				docker exec "${AWS_CONTAINER}" aws s3 rm "${prefix}${key}" --only-show-errors
			fi
		fi
	done
}

if [[ "${DAY_OF_WEEK}" == "7" ]]; then
	echo "Backup cadence: weekly"
	create_dump "weekly" "${WEEKLY_PREFIX}"
	if [[ "${DAY_OF_MONTH}" == "01" ]]; then
		echo "Backup cadence overlap: first-of-month on weekly day; copying weekly dump to monthly"
		copy_weekly_to_monthly
	fi
elif [[ "${DAY_OF_MONTH}" == "01" ]]; then
	echo "Backup cadence: monthly"
	create_dump "monthly" "${MONTHLY_PREFIX}"
else
	echo "Backup cadence: none (retention-only run)"
fi

prune_prefix_by_date "${WEEKLY_PREFIX}" "${weekly_cutoff}"
prune_prefix_by_date "${MONTHLY_PREFIX}" "${monthly_cutoff}"
docker exec alyx_apache python /var/www/alyx/alyx/manage.py deleterevisions --days=15