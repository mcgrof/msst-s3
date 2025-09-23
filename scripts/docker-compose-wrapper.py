#!/usr/bin/env python3
"""
Simple docker-compose wrapper using Docker SDK
Works around docker-compose compatibility issues
"""

import sys
import subprocess
import yaml
import os
import time
import argparse


def load_compose_file():
    """Load docker-compose.yml file"""
    compose_file = "docker-compose.yml"
    if not os.path.exists(compose_file):
        print(f"Error: {compose_file} not found")
        sys.exit(1)

    with open(compose_file, "r") as f:
        return yaml.safe_load(f)


def docker_run(service_name, config, detached=False):
    """Run a docker container based on service config"""
    cmd = ["docker", "run"]

    if detached:
        cmd.append("-d")

    # Add name
    cmd.extend(["--name", service_name])

    # Add environment variables
    if "environment" in config:
        env_vars = config["environment"]
        if isinstance(env_vars, dict):
            for key, value in env_vars.items():
                cmd.extend(["-e", f"{key}={value}"])
        elif isinstance(env_vars, list):
            for env_var in env_vars:
                if "=" in env_var:
                    cmd.extend(["-e", env_var])
                else:
                    # Skip entries like "SERVICES=s3" format
                    cmd.extend(["-e", env_var])

    # Add ports
    if "ports" in config:
        for port in config["ports"]:
            cmd.extend(["-p", port])

    # Add volumes
    if "volumes" in config:
        for volume in config["volumes"]:
            cmd.extend(["-v", volume])

    # Add networks
    if "networks" in config:
        for network in config["networks"]:
            cmd.extend(["--network", network])

    # Add image
    cmd.append(config["image"])

    # Add command if specified
    if "command" in config:
        if isinstance(config["command"], list):
            cmd.extend(config["command"])
        else:
            cmd.append(config["command"])

    return subprocess.run(cmd, capture_output=False)


def docker_up(services=None, detached=True):
    """Start services"""
    compose_data = load_compose_file()

    # Create networks if they don't exist
    if "networks" in compose_data:
        for network_name in compose_data["networks"].keys():
            # Check if network exists
            result = subprocess.run(
                ["docker", "network", "ls", "--filter", f"name={network_name}", "-q"],
                capture_output=True,
                text=True,
            )
            if not result.stdout.strip():
                print(f"Creating network: {network_name}")
                subprocess.run(["docker", "network", "create", network_name])

    # Start requested services or all services
    services_to_start = (
        services if services else compose_data.get("services", {}).keys()
    )

    for service_name in services_to_start:
        if service_name not in compose_data.get("services", {}):
            print(f"Service {service_name} not found in docker-compose.yml")
            continue

        # Check if container already exists
        result = subprocess.run(
            ["docker", "ps", "-a", "--filter", f"name={service_name}", "-q"],
            capture_output=True,
            text=True,
        )

        if result.stdout.strip():
            print(f"Container {service_name} already exists, removing...")
            subprocess.run(["docker", "rm", "-f", service_name])

        print(f"Starting {service_name}...")
        config = compose_data["services"][service_name]
        docker_run(service_name, config, detached)


def docker_down():
    """Stop and remove all containers"""
    compose_data = load_compose_file()

    for service_name in compose_data.get("services", {}).keys():
        print(f"Stopping {service_name}...")
        subprocess.run(["docker", "stop", service_name], stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", service_name], stderr=subprocess.DEVNULL)

    # Remove networks
    if "networks" in compose_data:
        for network_name in compose_data["networks"].keys():
            print(f"Removing network: {network_name}")
            subprocess.run(
                ["docker", "network", "rm", network_name], stderr=subprocess.DEVNULL
            )


def docker_ps():
    """List running containers"""
    compose_data = load_compose_file()
    service_names = list(compose_data.get("services", {}).keys())

    if service_names:
        filter_args = []
        for name in service_names:
            filter_args.extend(["--filter", f"name={name}"])
        subprocess.run(["docker", "ps"] + filter_args)
    else:
        subprocess.run(["docker", "ps"])


def docker_logs(services=None):
    """Show logs for services"""
    if services:
        for service in services:
            subprocess.run(["docker", "logs", service])
    else:
        compose_data = load_compose_file()
        for service_name in compose_data.get("services", {}).keys():
            print(f"\n=== Logs for {service_name} ===")
            subprocess.run(["docker", "logs", service_name])


def main():
    parser = argparse.ArgumentParser(description="Simple docker-compose wrapper")
    parser.add_argument("command", choices=["up", "down", "ps", "logs"])
    parser.add_argument(
        "services", nargs="*", help="Service names (for up/logs commands)"
    )
    parser.add_argument(
        "-d", "--detach", action="store_true", help="Run in detached mode"
    )

    args = parser.parse_args()

    if args.command == "up":
        docker_up(args.services, detached=args.detach)
    elif args.command == "down":
        docker_down()
    elif args.command == "ps":
        docker_ps()
    elif args.command == "logs":
        docker_logs(args.services)


if __name__ == "__main__":
    main()
