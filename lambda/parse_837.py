import json
import boto3
import os

s3 = boto3.client('s3')

def parse_837(lines):
    result = {
        "source_file": "",
        "claims": []
    }

    current_claim = {}
    service_lines = []
    diagnosis_codes = []

    for line in lines:
        line = line.strip('~\n')
        segments = line.split('*')
        if not segments or len(segments) < 2:
            continue

        tag = segments[0]

        if tag == "CLM":
            if current_claim:
                current_claim["diagnosis_codes"] = diagnosis_codes
                current_claim["service_lines"] = service_lines
                result["claims"].append(current_claim)
                service_lines = []
                diagnosis_codes = []

            try:
                current_claim = {
                    "claim_id": segments[1],
                    "total_charge": float(segments[2])
                }
            except (IndexError, ValueError):
                continue

        elif tag == "NM1":
            if segments[1] == "IL" and len(segments) >= 5:
                current_claim["subscriber"] = {
                    "last_name": segments[3],
                    "first_name": segments[4]
                }
            elif segments[1] == "82" and len(segments) >= 5:
                current_claim["provider"] = {
                    "last_name": segments[3],
                    "first_name": segments[4]
                }

        elif tag == "HI":
            for i in range(1, len(segments)):
                if ":" in segments[i]:
                    diagnosis_codes.append(segments[i].split(":")[1])

        elif tag == "SV3" or tag == "SV1":
            try:
                proc_code = segments[1].split(":")[1] if ":" in segments[1] else segments[1]
                service_lines.append({
                    "procedure_code": proc_code,
                    "charge": float(segments[2]),
                    "units": float(segments[5]) if len(segments) > 5 else 1
                })
            except (IndexError, ValueError):
                continue

    if current_claim:
        current_claim["diagnosis_codes"] = diagnosis_codes
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

    print("🔍 Parsing lines from:", key)
    structured = parse_837(lines)
    structured['source_file'] = key

    print("✅ Parsed claims:", json.dumps(structured, indent=2))

    s3.put_object(
        Bucket=os.environ['OUTPUT_BUCKET'],
        Key=result_key,
        Body=json.dumps(structured, indent=2).encode('utf-8')
    )

    return {
        'statusCode': 200,
        'body': f'Parsed {key} and saved to {result_key}'
    }
