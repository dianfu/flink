#!/usr/bin/env python
#################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
import argparse
import hashlib
import os
from subprocess import call

import grpc
import logging
import sys

from apache_beam.portability.api.beam_provision_api_pb2_grpc import ProvisionServiceStub
from apache_beam.portability.api.beam_provision_api_pb2 import GetProvisionInfoRequest
from apache_beam.portability.api.beam_artifact_api_pb2_grpc import ArtifactRetrievalServiceStub
from apache_beam.portability.api.beam_artifact_api_pb2 import GetManifestRequest, GetArtifactRequest
from apache_beam.portability.api.endpoints_pb2 import ApiServiceDescriptor

from google.protobuf import json_format, text_format

parser = argparse.ArgumentParser()

parser.add_argument("--id", default="1", help="Local identifier (required).")
parser.add_argument("--logging_endpoint", default="localhost:9989",
                    help="Logging endpoint (required).")
parser.add_argument("--artifact_endpoint", default="localhost:9990",
                    help="Artifact endpoint (required).")
parser.add_argument("--provision_endpoint", default="localhost:9991",
                    help="Provision endpoint (required).")
parser.add_argument("--control_endpoint", default="localhost:9992",
                    help="Control endpoint (required).")
parser.add_argument("--semi_persist_dir", default="/tmp",
                    help="Local semi-persistent directory (optional).")

args = parser.parse_args()

worker_id = args.id
logging_endpoint = args.logging_endpoint
artifact_endpoint = args.artifact_endpoint
provision_endpoint = args.provision_endpoint
control_endpoint = args.control_endpoint
semi_persist_dir = args.semi_persist_dir

if worker_id == "":
    logging.fatal("No id provided.")

if logging_endpoint == "":
    logging.fatal("No logging endpoint provided.")

if artifact_endpoint == "":
    logging.fatal("No artifact endpoint provided.")

if provision_endpoint == "":
    logging.fatal("No provision endpoint provided.")

if control_endpoint == "":
    logging.fatal("No control endpoint provided.")

logging.info("Initializing python harness: %s" % " ".join(sys.argv))

metadata = [("worker_id", worker_id)]

with grpc.insecure_channel(provision_endpoint) as channel:
    client = ProvisionServiceStub(channel=channel)
    info = client.GetProvisionInfo(GetProvisionInfoRequest(), metadata=metadata).info
    options = json_format.MessageToJson(info.pipeline_options)

print(info)
print(options)

staged_dir = os.path.join(semi_persist_dir, "staged")
files = []

with grpc.insecure_channel(artifact_endpoint) as channel:
    client = ArtifactRetrievalServiceStub(channel=channel)
    response = client.GetManifest(GetManifestRequest(retrieval_token=info.retrieval_token),
                                  metadata=metadata)
    artifacts = response.manifest.artifact
    for artifact in artifacts:
        name = artifact.name
        files.append(name)
        permissions = artifact.permissions
        sha256 = artifact.sha256
        file_path = os.path.join(staged_dir, name)
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                sha256obj = hashlib.sha256()
                sha256obj.update(f.read())
                hash_value = sha256obj.hexdigest()
            if hash_value == sha256:
                logging.info("file exist and has same sha256 hash value, skip...")
                continue
            else:
                os.remove(file_path)
        if not os.path.exists(os.path.dirname(file_path)):
            os.makedirs(os.path.dirname(file_path), 0o755)
        stream = client.GetArtifact(
            GetArtifactRequest(name=name, retrieval_token=info.retrieval_token), metadata=metadata)
        with open(file_path, "wb") as f:
            sha256obj = hashlib.sha256()
            for artifact_chunk in stream:
                sha256obj.update(artifact_chunk.data)
                f.write(artifact_chunk.data)
            hash_value = sha256obj.hexdigest()
        if hash_value != sha256:
            raise Exception("she256 not consist!")
        os.chmod(file_path, permissions)

print(files)

acceptable_whl_specs = ["cp27-cp27mu-manylinux1_x86_64.whl"]


def find_beam_sdk_whl():
    for filename in files:
        if filename.startswith("apache_beam"):
            for acceptable_whl_spec in acceptable_whl_specs:
                if filename.endswith(acceptable_whl_spec):
                    logging.info("Found Apache Beam SDK wheel: %s" % filename)
                    return filename
    return ""


def pip_location():
    if "pip" in os.environ:
        return os.environ["pip"]
    else:
        return "/usr/local/bin/pip"


def python_location():
    if "python" in os.environ:
        return os.environ["python"]
    else:
        return "python"


pip = pip_location()
python_path = python_location()


def pip_install_package(files, dir, name, force, optional, extras):
    for filename in files:
        if filename == name:
            package = name
            if extras is not None and len(extras) > 0:
                package += "[" + ",".join(extras) + "]"
            if force:
                args = [pip, "install", "--upgrade", "--force-reinstall", "--no-deps",
                        os.path.join(dir, package)]
                exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
                if exit_code > 0:
                    raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
                args = [pip, "install", os.path.join(dir, package)]
                exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
                if exit_code > 0:
                    raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
                return exit_code
            args = [pip, "install", os.path.join(dir, package)]
            exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
            if exit_code > 0:
                raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
            return exit_code
    if optional:
        return 0
    raise Exception("package '" + name + "' not found")


sdk_whl_file = find_beam_sdk_whl()
if sdk_whl_file != "":
    exit_code = pip_install_package(files, staged_dir, sdk_whl_file, False, False, ["gcp"])

sdk_src_file = "dataflow_python_sdk.tar"
if os.path.exists(os.path.join(staged_dir, sdk_src_file)):
    pip_install_package(files, staged_dir, sdk_src_file, False, False, ["gcp"])


def pip_install_requirments(files, dir, name):
    for filename in files:
        if filename == name:
            args = ["install", "-r", os.path.join(dir, name), "--no-index", "--no-deps",
                    "--find-links", dir]
            exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
            if exit_code > 0:
                raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
            args = ["install", "-r", os.path.join(dir, name), "--find-links", dir]
            exit_code = call(args, stdout=sys.stdout, stderr=sys.stderr)
            if exit_code > 0:
                raise Exception("execute %s error! exit code: %d" % (" ".join(args), exit_code))
            return 0
    return 0


pip_install_requirments(files, staged_dir, "requirements.txt")


def install_extra_packages(files, extra_packages_file, dir):
    for filename in files:
        if filename == extra_packages_file:
            with open(os.path.join(dir, extra_packages_file), "r") as f:
                while True:
                    text_line = f.readline()
                    if text_line:
                        pip_install_package(files, dir, text_line, True, False, None)
                    else:
                        break
            return 0
    return 0


install_extra_packages(files, staged_dir, "extra_packages.txt")

pip_install_package(files, staged_dir, "workflow.tar.gz", False, True, None)

os.environ["WORKER_ID"] = worker_id
os.environ["PIPELINE_OPTIONS"] = options
os.environ["SEMI_PERSISTENT_DIRECTORY"] = semi_persist_dir
os.environ["LOGGING_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
    ApiServiceDescriptor(url=logging_endpoint))
os.environ["CONTROL_API_SERVICE_DESCRIPTOR"] = text_format.MessageToString(
    ApiServiceDescriptor(url=control_endpoint))

print(text_format.MessageToString(ApiServiceDescriptor(url=logging_endpoint)))
print(text_format.MessageToString(ApiServiceDescriptor(url=control_endpoint)))

env = dict(os.environ)

sdk_worker_main_path = os.path.dirname(os.path.abspath(__file__)) + "/sdk_worker_main.py"
with open("/tmp/pyflink.txt", "wb") as f:
    call([python_path, sdk_worker_main_path], stdout=f, stderr=f, env=env)
