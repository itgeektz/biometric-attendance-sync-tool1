import local_config as config
import requests
import datetime
import json
import os
import logging
from logging.handlers import RotatingFileHandler
from pickledb import PickleDB

# --- ERPNext/BioTime Constants ---
EMPLOYEE_NOT_FOUND_ERROR_MESSAGE = "No Employee found for the given employee field value"
EMPLOYEE_INACTIVE_ERROR_MESSAGE = "Transactions cannot be created for an Inactive Employee"
DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE = "This employee already has a log with the same timestamp"

allowlisted_errors = [
    EMPLOYEE_NOT_FOUND_ERROR_MESSAGE,
    EMPLOYEE_INACTIVE_ERROR_MESSAGE,
    DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE
]

if hasattr(config, 'allowed_exceptions'):
    allowlisted_errors_temp = []
    for error_number in config.allowed_exceptions:
        allowlisted_errors_temp.append(allowlisted_errors[error_number - 1])
    allowlisted_errors = allowlisted_errors_temp

device_punch_values_IN = getattr(config, 'device_punch_values_IN', [0, 4])
device_punch_values_OUT = getattr(config, 'device_punch_values_OUT', [1, 5])
ERPNEXT_VERSION = getattr(config, 'ERPNEXT_VERSION', 15)

# --- Setup Logging & Status ---
if not os.path.exists(config.LOGS_DIRECTORY):
    os.makedirs(config.LOGS_DIRECTORY)

def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')
    handler = RotatingFileHandler(log_file, maxBytes=10_000_000, backupCount=50)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)
    return logger

error_logger = setup_logger('error_logger', os.path.join(config.LOGS_DIRECTORY, 'error.log'), logging.ERROR)
info_logger = setup_logger('info_logger', os.path.join(config.LOGS_DIRECTORY, 'logs.log'))
status = PickleDB(os.path.join(config.LOGS_DIRECTORY, 'status.json'))

# --- Utilities ---
def _safe_convert_date(datestring, pattern):
    try:
        return datetime.datetime.strptime(datestring, pattern)
    except:
        return None

def _safe_get_error_str(res):
    try:
        error_json = res.json()
        if 'exc' in error_json:
            return json.loads(error_json['exc'])[0]
        return json.dumps(error_json)
    except:
        return str(res.content)

def get_dump_file_name_and_directory(device_id, device_ip):
    return config.LOGS_DIRECTORY + '/' + device_id + "_" + device_ip.replace('.', '_') + '_last_fetch_dump.json'

# --- BioTime Fetch ---
def get_biotime_token(server_ip, server_port, username, password):
    resp = requests.post(
        f"http://{server_ip}:{server_port}/api-token-auth/",
        json={"username": username, "password": password}
    )
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise Exception("Failed to obtain BioTime auth token")
    return token

def get_all_attendance_from_biotime(server_ip, server_port, username, password,
                                    start_time, end_time,
                                    emp_code=None, device_sn=None):
    token = get_biotime_token(server_ip, server_port, username, password)
    headers = {"Content-Type": "application/json", "Authorization": f"Token {token}"}
    params = {"start_time": start_time, "end_time": end_time}
    if emp_code: params["emp_code"] = emp_code
    if device_sn: params["terminal_sn"] = device_sn

    attendances = []
    page = 1
    while True:
        params["page"] = page
        resp = requests.get(f"http://{server_ip}:{server_port}/iclock/api/transactions/",
                            headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()
        for rec in data.get("data", []):
            ts = datetime.datetime.strptime(rec["punch_time"], "%Y-%m-%d %H:%M:%S")
            attendances.append({
                "uid": rec.get("id"),
                "user_id": rec.get("emp_code"),
                "timestamp": ts,
                "punch": int(rec.get("punch_state", 255)),
                "status": 1
            })
        if not data.get("next"):
            break
        page += 1
    return attendances

# --- ERPNext Push ---
def send_to_erpnext(employee_field_value, timestamp, device_id=None, log_type=None, latitude=None, longitude=None):
    endpoint_app = "hrms" if ERPNEXT_VERSION > 13 else "erpnext"
    url = f"{config.ERPNEXT_URL}/api/method/{endpoint_app}.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"
    headers = {
        'Authorization': f"token {config.ERPNEXT_API_KEY}:{config.ERPNEXT_API_SECRET}",
        'Accept': 'application/json'
    }
    data = {
        'employee_field_value': employee_field_value,
        'timestamp': str(timestamp),
        'device_id': device_id,
        'log_type': log_type,
        'latitude': latitude,
        'longitude': longitude
    }
    resp = requests.post(url, headers=headers, json=data)
    if resp.status_code == 200:
        return 200, resp.json()['message']['name']
    else:
        return resp.status_code, _safe_get_error_str(resp)

# --- Time Range ---
def get_time_range(device_id):
    last_ts_str = status.get(f'{device_id}_last_success_push')
    last_ts = _safe_convert_date(last_ts_str, "%Y-%m-%d %H:%M:%S")
    if not last_ts:
        last_ts = _safe_convert_date(config.IMPORT_START_DATE, "%Y%m%d")
    start_time = last_ts or (datetime.datetime.now() - datetime.timedelta(days=10))
    end_time = datetime.datetime.now()
    return start_time.strftime("%Y-%m-%d %H:%M:%S"), end_time.strftime("%Y-%m-%d %H:%M:%S")

# --- Core ---
def pull_and_push(device):
    attendance_success_log_file = '_'.join(["attendance_success_log", device['device_id']])
    attendance_failed_log_file = '_'.join(["attendance_failed_log", device['device_id']])
    success_logger = setup_logger(attendance_success_log_file,
                                  os.path.join(config.LOGS_DIRECTORY, attendance_success_log_file) + '.log')
    fail_logger = setup_logger(attendance_failed_log_file,
                               os.path.join(config.LOGS_DIRECTORY, attendance_failed_log_file) + '.log')

    start_time, end_time = get_time_range(device['device_id'])
    info_logger.info(f"Fetching logs for {device['device_id']} from {start_time} to {end_time}")

    logs = get_all_attendance_from_biotime(
        device['server_ip'], device['server_port'],
        device['username'], device['password'],
        start_time, end_time
    )

    for log in logs:
        punch_direction = device.get('punch_direction', None)
        if punch_direction == 'AUTO':
            if log['punch'] in device_punch_values_OUT:
                punch_direction = 'OUT'
            elif log['punch'] in device_punch_values_IN:
                punch_direction = 'IN'
        status_code, msg = send_to_erpnext(
            log['user_id'], log['timestamp'], device['device_id'],
            punch_direction, device['latitude'], device['longitude']
        )
        if status_code == 200:
            success_logger.info("\t".join([
                msg, str(log['uid']), str(log['user_id']),
                str(log['timestamp'].timestamp()), str(log['punch']), str(log['status']),
                json.dumps(log, default=str)
            ]))
            status.set(f'{device["device_id"]}_last_success_push', str(log['timestamp']))
            status.save()
        else:
            fail_logger.error("\t".join([
                str(status_code), str(log['uid']), str(log['user_id']),
                str(log['timestamp'].timestamp()), str(log['punch']), str(log['status']),
                json.dumps(log, default=str)
            ]))
            if not any(err in msg for err in allowlisted_errors):
                raise Exception(f"Critical ERPNext Push Failure for device {device['device_id']}")

# --- Main ---
def main():
    for device in config.devices:
        try:
            pull_and_push(device)
        except Exception as e:
            error_logger.exception(f"Error with device {device['device_id']}: {e}")

if __name__ == "__main__":
    main()
