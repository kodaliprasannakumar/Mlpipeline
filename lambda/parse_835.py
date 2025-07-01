import json
import boto3
import os

s3 = boto3.client('s3')

def parse_835(lines):
    result = {
        "source_file": "",
        "claims": [],
        "payment_info": {},
        "payer": {},
        "payee": {}
    }

    current_claim = {}
    service_lines = []
    adjustments = []

    for line in lines:
        line = line.strip('~\n')
        segments = line.split('*')
        if not segments or len(segments) < 2:
            continue

        tag = segments[0]

        if tag == "BPR":
            result["payment_info"] = {
                "amount": float(segments[2]),
                "method": segments[3],
                "payment_date": segments[16]
            }

        elif tag == "N1":
            if segments[1] == "PR":
                result["payer"]["name"] = segments[2]
            elif segments[1] == "PE":
                result["payee"]["name"] = segments[2]

        elif tag == "N3":
            addr = {"street": segments[1]}
            if "address" not in result["payer"]:
                result["payer"]["address"] = addr
            else:
                result["payee"]["address"] = addr

        elif tag == "N4":
            city_state_zip = {
                "city": segments[1],
                "state": segments[2],
                "zip": segments[3]
            }
            if "city" not in result["payer"].get("address", {}):
                result["payer"]["address"].update(city_state_zip)
            else:
                result["payee"]["address"].update(city_state_zip)

        elif tag == "PER" and segments[1] == "BL":
            result["payer"]["contact"] = {
                "name": segments[2],
                "phone": segments[4]
            }

        elif tag == "CLP":
            if current_claim:
                current_claim["adjustments"] = adjustments
                current_claim["service_lines"] = service_lines
                result["claims"].append(current_claim)
                service_lines = []
                adjustments = []

            current_claim = {
                "claim_id": segments[1],
                "charge_amount": float(segments[3]),
                "payment_amount": float(segments[4]),
                "patient_responsibility": float(segments[5])
            }

        elif tag == "CAS":
            adjustments.append({
                "group_code": segments[1],
                "reason_code": segments[2],
                "amount": float(segments[3])
            })

        elif tag == "NM1" and segments[1] == "IL":
            current_claim["subscriber"] = {
                "last_name": segments[3],
                "first_name": segments[4],
                "subscriber_id": segments[9] if len(segments) > 9 else ""
            }

        elif tag == "SVC":
            if len(segments) >= 4:
                proc_code = segments[1].split(":")[1] if ":" in segments[1] else segments[1]
                service_lines.append({
                    "procedure_code": proc_code,
                    "charge": float(segments[2]),
                    "payment": float(segments[3])
                })

    if current_claim:
        current_claim["adjustments"] = adjustments
        current_claim["service_lines"] = service_lines
        result["claims"].append(current_claim)

    return result


def handler(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']
    result_key = key.replace('.dat', '.json')

    response = s3.get_object(Bucket=bucket, Key=key)
    raw_data = response['Body'].read().decode('utf-8')
    lines = raw_data.splitlines()

    structured = parse_835(lines)
    structured['source_file'] = key

    s3.put_object(
        Bucket=os.environ['OUTPUT_BUCKET'],
        Key=result_key,
        Body=json.dumps(structured, indent=2).encode('utf-8')
    )

    return {
        'statusCode': 200,
        'body': f'Parsed {key} and saved to {result_key}'
    }
