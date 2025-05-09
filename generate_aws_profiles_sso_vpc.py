import boto3
import configparser
from pathlib import Path

# Customize your SSO settings and profiles here
profile_names = ["prod", "dev"]
sso_info = {
    "prod": {
        "sso_start_url": "https://your-sso-portal.awsapps.com/start",
        "sso_region": "us-east-1",
        "sso_account_id": "123456789012",
        "sso_role_name": "ProdAdmin",
        "region": "us-east-1"
    },
    "dev": {
        "sso_start_url": "https://your-sso-portal.awsapps.com/start",
        "sso_region": "us-west-2",
        "sso_account_id": "210987654321",
        "sso_role_name": "DevPowerUser",
        "region": "us-west-2"
    }
}

services = ["s3", "ec2", "sts"]

# Path to AWS config file
config_path = Path.home() / ".aws" / "config"
config = configparser.RawConfigParser()
config.read(config_path)

for profile in profile_names:
    session = boto3.Session(profile_name=profile)
    region = sso_info[profile]["region"]
    ec2 = session.client("ec2", region_name=region)

    # Get interface VPC endpoints
    response = ec2.describe_vpc_endpoints()
    service_endpoints = {}
    for ep in response["VpcEndpoints"]:
        if ep["VpcEndpointType"] == "Interface":
            for svc in services:
                if f".{svc}." in ep["ServiceName"]:
                    service_endpoints[svc] = ep["DnsEntries"][0]["DnsName"]

    section = f"profile {profile}"
    if not config.has_section(section):
        config.add_section(section)

    config.set(section, "sso_start_url", sso_info[profile]["sso_start_url"])
    config.set(section, "sso_region", sso_info[profile]["sso_region"])
    config.set(section, "sso_account_id", sso_info[profile]["sso_account_id"])
    config.set(section, "sso_role_name", sso_info[profile]["sso_role_name"])
    config.set(section, "region", region)
    config.set(section, "output", "json")

    # Add service endpoint overrides
    for svc in service_endpoints:
        key = f"{svc}"
        config.set(section, key, f"\n    endpoint_url = https://{service_endpoints[svc]}")

# Save updated config
with config_path.open("w") as f:
    config.write(f)

print(f"✅ AWS config updated at {config_path}")
